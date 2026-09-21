package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Mic92/niks3/server/pg"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"golang.org/x/sync/errgroup"
)

const (
	maxSignedURLDuration = time.Duration(5) * time.Hour

	// deletionPollInterval is how often a closure waiting on objects that GC
	// is deleting re-checks them.
	deletionPollInterval = time.Second

	// deletionWaitTimeout bounds how long closure creation waits for GC to
	// finish deleting an object it needs. The server's write timeout is 60s,
	// so waiting longer would only produce a broken response. A batch of S3
	// deletes normally finishes well within this.
	deletionWaitTimeout = 30 * time.Second

	// staleClaimAge is how old a deletion claim must be before it is treated
	// as left behind by a GC run that died. A live run re-stamps claims it
	// picks up, so a claim this old has no deletion in flight and the object
	// is simply re-uploaded.
	staleClaimAge = time.Hour
)

// errObjectsBeingDeleted is returned when closure creation waited for GC to
// finish deleting an object and it did not finish in time.
var errObjectsBeingDeleted = errors.New("objects are being deleted by garbage collection, retry later")

// optionalSize maps a reported size to a nullable column; nil stays NULL and is
// excluded from byte totals.
func optionalSize(size *uint64) pgtype.Int8 {
	// A size beyond int64 cannot be stored in the BIGINT column; treat it as
	// unknown rather than wrapping around.
	if size == nil || *size > math.MaxInt64 {
		return pgtype.Int8{}
	}

	return pgtype.Int8{Int64: int64(*size), Valid: true}
}

type PendingObject struct {
	Type          string               `json:"type"`                     // Object type (narinfo, listing, build_log, realisation, nar)
	PresignedURL  string               `json:"presigned_url,omitempty"`  // For small files (listing, build_log, realisation)
	MultipartInfo *MultipartUploadInfo `json:"multipart_info,omitempty"` // For large files (nar)
}

type PendingClosureResponse struct {
	ID             string                   `json:"id"`
	StartedAt      time.Time                `json:"started_at"`
	PendingObjects map[string]PendingObject `json:"pending_objects"`
}

// PendingClosure is the outcome of recording a closure. Every object of the
// closure gets a pending_objects row so garbage collection leaves it alone
// until commit; only the ones in recorded.uploads are handed to the client.
type PendingClosure struct {
	id        int64
	startedAt time.Time
	recorded  recordedObjects
}

func rollbackOnError(ctx context.Context, tx *pgx.Tx, err *error, committed *bool) {
	if p := recover(); p != nil && !*committed {
		if rbErr := (*tx).Rollback(ctx); rbErr != nil {
			slog.Error("failed to rollback transaction", "error", rbErr)
		}

		panic(p) // re-throw after Rollback
	} else if *err != nil && !*committed {
		if rbErr := (*tx).Rollback(ctx); rbErr != nil {
			slog.Error("failed to rollback transaction", "error", rbErr)
		}
	}
}

// checkS3ObjectsExist checks which of the given object keys exist in S3 using a worker pool.
// Returns a map of keys that are missing from S3 and any S3 error encountered.
// If an S3 error occurs, returns immediately with partial results.
func (s *Service) checkS3ObjectsExist(ctx context.Context, objectKeys []string) (map[string]bool, error) {
	if len(objectKeys) == 0 {
		return make(map[string]bool), nil
	}

	missingObjects := make(map[string]bool)
	var mu sync.Mutex

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(s.S3Concurrency)

	for _, key := range objectKeys {
		g.Go(func() error {
			if err := s.S3RateLimiter.Wait(ctx); err != nil {
				return fmt.Errorf("rate limiter: %w", err)
			}

			_, err := s.MinioClient.StatObject(ctx, s.Bucket, key, minio.StatObjectOptions{})
			if err != nil {
				if isRateLimitError(err) {
					s.S3RateLimiter.RecordThrottle()
				}

				errResp := minio.ToErrorResponse(err)
				if errResp.Code == minio.NoSuchKey {
					mu.Lock()
					missingObjects[key] = true
					mu.Unlock()
					s.S3RateLimiter.RecordSuccess()

					return nil
				}
				// Return error to cancel the group
				return fmt.Errorf("failed to check S3 object %q: %w", key, err)
			}

			s.S3RateLimiter.RecordSuccess()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return missingObjects, fmt.Errorf("check S3 objects: %w", err)
	}

	for key := range missingObjects {
		slog.Info("Object in database but missing from S3", "key", key)
	}

	return missingObjects, nil
}

// pendingParams builds the pending_objects row for key.
func pendingParams(pendingClosureID int64, key string, obj objectWithRefs, needsUpload bool) pg.InsertPendingObjectsParams {
	refs := obj.Refs
	if refs == nil {
		refs = []string{}
	}

	return pg.InsertPendingObjectsParams{
		PendingClosureID: pendingClosureID,
		Key:              key,
		Refs:             refs,
		Size:             optionalSize(obj.NarSize),
		NeedsUpload:      needsUpload,
	}
}

// objectState classifies an object row for closure creation.
type objectState int

const (
	// objectMissing: no row, or a tombstone GC has not claimed yet. Both are
	// uploaded: a tombstone may come from an abandoned closure that never
	// uploaded the object, and re-uploading an existing key is idempotent.
	// The pending_objects row keeps GC from claiming the tombstone.
	objectMissing objectState = iota
	// objectPresent: live row, the object is in S3.
	objectPresent
	// objectDeleting: GC has claimed the row and may be deleting the S3
	// object right now. Uploading would race the delete.
	objectDeleting
	// objectAbandonedClaim: a claim old enough that its GC run must have
	// died; there is no deletion in flight, so the object is re-uploaded.
	objectAbandonedClaim
)

func classifyObject(row *pg.LockExistingObjectsRow, dbNow time.Time) objectState {
	switch {
	case row == nil:
		return objectMissing
	case row.DeletingAt.Valid:
		if dbNow.Sub(row.DeletingAt.Time) > staleClaimAge {
			return objectAbandonedClaim
		}

		return objectDeleting
	case row.Tombstoned:
		return objectMissing
	default:
		return objectPresent
	}
}

// recordedObjects is the outcome of recording a set of keys for a closure.
type recordedObjects struct {
	// uploads are the pending objects the client must upload.
	uploads []pg.InsertPendingObjectsParams
	// present are objects recorded as already in S3 (needs_upload = false).
	present []string
	// deleting are objects GC has claimed for deletion. They got no
	// pending_objects row; the caller retries them once GC is done.
	deleting []string
}

func (r *recordedObjects) merge(o recordedObjects) {
	r.uploads = append(r.uploads, o.uploads...)
	r.present = append(r.present, o.present...)
	r.deleting = append(r.deleting, o.deleting...)
}

// recordPendingObjects inserts pending_objects rows for keys in one
// transaction, share-locking the object rows first. Keys whose object GC is
// deleting get no row and are returned as still deleting. The lock makes the
// deletion claim (FOR UPDATE SKIP LOCKED) either skip these objects for its
// run or, if it got there first, become visible here as deleting_at.
func recordPendingObjects(
	ctx context.Context,
	pool *pgxpool.Pool,
	pendingClosureID int64,
	objectsMap map[string]objectWithRefs,
	keys []string,
) (recordedObjects, error) {
	var out recordedObjects

	tx, err := pool.Begin(ctx)
	if err != nil {
		return out, fmt.Errorf("failed to start transaction: %w", err)
	}

	committed := false

	defer rollbackOnError(ctx, &tx, &err, &committed)

	queries := pg.New(tx)

	var dbNow pgtype.Timestamp

	if dbNow, err = queries.Now(ctx); err != nil {
		return out, fmt.Errorf("failed to read database clock: %w", err)
	}

	var rows []pg.LockExistingObjectsRow

	if rows, err = queries.LockExistingObjects(ctx, keys); err != nil {
		return out, fmt.Errorf("failed to lock existing objects: %w", err)
	}

	rowByKey := make(map[string]*pg.LockExistingObjectsRow, len(rows))
	for i := range rows {
		rowByKey[rows[i].Key] = &rows[i]
	}

	params := make([]pg.InsertPendingObjectsParams, 0, len(keys))

	for _, key := range keys {
		obj := objectsMap[key]

		switch classifyObject(rowByKey[key], dbNow.Time) {
		case objectMissing, objectAbandonedClaim:
			p := pendingParams(pendingClosureID, key, obj, true)
			params = append(params, p)
			out.uploads = append(out.uploads, p)
		case objectPresent:
			params = append(params, pendingParams(pendingClosureID, key, obj, false))
			out.present = append(out.present, key)
		case objectDeleting:
			out.deleting = append(out.deleting, key)
		}
	}

	if _, err = queries.InsertPendingObjects(ctx, params); err != nil {
		return out, fmt.Errorf("failed to insert pending objects: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return out, fmt.Errorf("failed to commit transaction: %w", err)
	}

	committed = true

	return out, nil
}

func createPendingClosureInner(
	ctx context.Context,
	pool *pgxpool.Pool,
	closureKey string,
	objectsMap map[string]objectWithRefs,
) (*PendingClosure, error) {
	if !strings.HasSuffix(closureKey, ".narinfo") {
		return nil, fmt.Errorf("closure key must end with .narinfo: %s", closureKey)
	}

	// The pending closure row is committed on its own so a failure while
	// recording objects leaves something for cleanupPendingClosures to reap.
	pendingClosure, err := pg.New(pool).InsertPendingClosure(ctx, closureKey)
	if err != nil {
		return nil, fmt.Errorf("failed to insert pending closure: %w", err)
	}

	keys := make([]string, 0, len(objectsMap))
	for k := range objectsMap {
		keys = append(keys, k)
	}

	// Sorted so rows are locked in a consistent order across transactions.
	slices.Sort(keys)

	recorded, err := recordPendingObjects(ctx, pool, pendingClosure.ID, objectsMap, keys)
	if err != nil {
		return nil, err
	}

	return &PendingClosure{
		id:        pendingClosure.ID,
		startedAt: pendingClosure.StartedAt.Time,
		recorded:  recorded,
	}, nil
}

// resolveDeletingObjects waits for garbage collection to finish with objects it
// has claimed, then records them for the closure. Once the row is gone (or the
// claim was abandoned) the object is uploaded again; if the claim was undone
// because the S3 delete failed, the object is present.
func resolveDeletingObjects(
	ctx context.Context,
	pool *pgxpool.Pool,
	pendingClosureID int64,
	objectsMap map[string]objectWithRefs,
	deleting []string,
) (recordedObjects, error) {
	slog.Info("Waiting for garbage collection to finish deleting objects", "count", len(deleting))

	var out recordedObjects

	deadline := time.Now().Add(deletionWaitTimeout)

	for {
		round, err := recordPendingObjects(ctx, pool, pendingClosureID, objectsMap, deleting)
		if err != nil {
			return recordedObjects{}, err
		}

		deleting = round.deleting
		round.deleting = nil
		out.merge(round)

		if len(deleting) == 0 {
			return out, nil
		}

		if time.Now().After(deadline) {
			return recordedObjects{}, fmt.Errorf("%w: %d objects still claimed after %s",
				errObjectsBeingDeleted, len(deleting), deletionWaitTimeout)
		}

		select {
		case <-ctx.Done():
			return recordedObjects{}, fmt.Errorf("waiting for object deletion: %w", ctx.Err())
		case <-time.After(deletionPollInterval):
		}
	}
}

// createPendingObjects generates presigned URLs or multipart upload info for pending objects.
// Presigned URLs are generated synchronously (no network call, just local signing).
// Multipart uploads are parallelized since they require S3 network calls.
func (s *Service) createPendingObjects(
	ctx context.Context,
	pendingClosureID int64,
	pendingObjectsParams []pg.InsertPendingObjectsParams,
	objectsMap map[string]objectWithRefs,
	result map[string]PendingObject,
) error {
	if len(pendingObjectsParams) == 0 {
		return nil
	}

	// Collect NAR objects that need multipart uploads (require S3 calls)
	type narTask struct {
		key     string
		narSize uint64
	}

	var narTasks []narTask

	// Process non-NAR objects synchronously (presigned URLs are just local signing, no network)
	for _, pendingObject := range pendingObjectsParams {
		obj := objectsMap[pendingObject.Key]

		if obj.Type == objectTypeNar {
			var narSize uint64
			if obj.NarSize != nil {
				narSize = *obj.NarSize
			}

			// Small NARs fall through to a presigned PUT like the other small objects.
			if !useSimpleUpload(narSize) {
				narTasks = append(narTasks, narTask{key: pendingObject.Key, narSize: narSize})

				continue
			}
		}

		po, err := s.makePresignedURL(ctx, pendingObject.Key, obj.Type)
		if err != nil {
			return fmt.Errorf("failed to create presigned URL %q: %w", pendingObject.Key, err)
		}

		result[pendingObject.Key] = po
	}

	// Process NAR objects in parallel (multipart uploads require S3 network calls)
	if len(narTasks) == 0 {
		return nil
	}

	var mu sync.Mutex

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(s.S3Concurrency)

	for _, task := range narTasks {
		g.Go(func() error {
			po, err := s.createMultipartUpload(ctx, pendingClosureID, task.key, task.narSize)
			if err != nil {
				return fmt.Errorf("failed to create multipart upload %q: %w", task.key, err)
			}

			po.Type = objectTypeNar

			mu.Lock()
			result[task.key] = po
			mu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("create multipart uploads: %w", err)
	}

	return nil
}

func (s *Service) makePresignedURL(ctx context.Context, objectKey string, objectType string) (PendingObject, error) {
	if err := s.S3RateLimiter.Wait(ctx); err != nil {
		return PendingObject{}, fmt.Errorf("rate limiter: %w", err)
	}

	presignedURL, err := s.PresignClient.PresignedPutObject(ctx,
		s.Bucket,
		objectKey,
		maxSignedURLDuration)
	if err != nil {
		if isRateLimitError(err) {
			s.S3RateLimiter.RecordThrottle()
		}

		return PendingObject{}, fmt.Errorf("failed to create presigned URL: %w", err)
	}

	s.S3RateLimiter.RecordSuccess()

	return PendingObject{
		Type:         objectType,
		PresignedURL: presignedURL.String(),
	}, nil
}

// verifyPresentObjects stats the objects the database says are in S3 and
// flips the ones that are missing to needs_upload. Runs outside any
// transaction so S3 round trips hold no row locks.
func (s *Service) verifyPresentObjects(
	ctx context.Context,
	pendingClosureID int64,
	objectsMap map[string]objectWithRefs,
	present []string,
) ([]pg.InsertPendingObjectsParams, error) {
	missingFromS3, err := s.checkS3ObjectsExist(ctx, present)
	if err != nil {
		return nil, fmt.Errorf("failed to verify objects in S3: %w", err)
	}

	if len(missingFromS3) == 0 {
		return nil, nil
	}

	slog.Warn("Found objects in DB but missing from S3, will re-upload", "count", len(missingFromS3))

	keys := make([]string, 0, len(missingFromS3))
	for key := range missingFromS3 {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	if err := pg.New(s.Pool).MarkPendingObjectsForUpload(ctx, pg.MarkPendingObjectsForUploadParams{
		PendingClosureID: pendingClosureID,
		Keys:             keys,
	}); err != nil {
		return nil, fmt.Errorf("failed to mark pending objects for upload: %w", err)
	}

	uploads := make([]pg.InsertPendingObjectsParams, 0, len(keys))
	for _, key := range keys {
		uploads = append(uploads, pendingParams(pendingClosureID, key, objectsMap[key], true))
	}

	return uploads, nil
}

func (s *Service) createPendingClosure(
	ctx context.Context,
	pool *pgxpool.Pool,
	closureKey string,
	objectsMap map[string]objectWithRefs,
	verifyS3 bool,
) (*PendingClosureResponse, error) {
	pendingClosure, err := createPendingClosureInner(ctx, pool, closureKey, objectsMap)
	if err != nil {
		return nil, err
	}

	recorded := &pendingClosure.recorded

	if len(recorded.deleting) > 0 {
		resolved, err := resolveDeletingObjects(ctx, pool, pendingClosure.id, objectsMap, recorded.deleting)
		if err != nil {
			return nil, err
		}

		recorded.deleting = nil
		recorded.merge(resolved)
	}

	if verifyS3 && len(recorded.present) > 0 {
		uploads, err := s.verifyPresentObjects(ctx, pendingClosure.id, objectsMap, recorded.present)
		if err != nil {
			return nil, err
		}

		recorded.uploads = append(recorded.uploads, uploads...)
	}

	pendingObjects := make(map[string]PendingObject, len(recorded.uploads))

	if err := s.createPendingObjects(ctx, pendingClosure.id, recorded.uploads, objectsMap, pendingObjects); err != nil {
		return nil, err
	}

	return &PendingClosureResponse{
		ID:             strconv.FormatInt(pendingClosure.id, 10),
		StartedAt:      pendingClosure.startedAt,
		PendingObjects: pendingObjects,
	}, nil
}

var errPendingClosureNotFound = errors.New("not found")

func commitPendingClosure(ctx context.Context, pool *pgxpool.Pool, pendingClosureID int64) error {
	if err := pg.New(pool).CommitPendingClosure(ctx, pendingClosureID); err != nil {
		msg := "Closure does not exist:"

		var pgError *pgconn.PgError

		ok := errors.As(err, &pgError)
		if ok && strings.Contains(pgError.Message, msg) {
			return fmt.Errorf("failed to commit pending closure: %w", errPendingClosureNotFound)
		}

		return fmt.Errorf("failed to commit pending closure: %w", err)
	}

	return nil
}
