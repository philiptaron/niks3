package server_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server"
	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/minio/minio-go/v7"
)

// commitClosureDirect records a committed closure of one narinfo and one NAR
// straight through the queries, with both objects present in S3.
func commitClosureDirect(t *testing.T, service *server.Service, queries *pg.Queries, narinfoKey, narKey string) {
	t.Helper()
	ctx := t.Context()

	pendingClosure, err := queries.InsertPendingClosure(ctx, narinfoKey)
	ok(t, err)

	_, err = queries.InsertPendingObjects(ctx, []pg.InsertPendingObjectsParams{
		{PendingClosureID: pendingClosure.ID, Key: narinfoKey, Refs: []string{narKey}, NeedsUpload: true},
		{PendingClosureID: pendingClosure.ID, Key: narKey, Refs: []string{}, NeedsUpload: true},
	})
	ok(t, err)

	for _, key := range []string{narinfoKey, narKey} {
		_, err := service.MinioClient.PutObject(ctx, service.Bucket, key, nil, 0, minio.PutObjectOptions{})
		ok(t, err)
	}

	ok(t, queries.CommitPendingClosure(ctx, pendingClosure.ID))
}

func objectDeletedAt(t *testing.T, service *server.Service, key string) (pgtype.Timestamp, bool) {
	t.Helper()

	var deletedAt pgtype.Timestamp

	err := service.Pool.QueryRow(t.Context(), "SELECT deleted_at FROM objects WHERE key = $1", key).Scan(&deletedAt)
	if err != nil {
		return pgtype.Timestamp{}, false
	}

	return deletedAt, true
}

// TestClaimObjectsForDeletionTerminates checks that the deletion claim hands
// out every eligible object exactly once per GC run, that a later run picks
// up claims left behind, and that an in-flight closure shields its objects.
func TestClaimObjectsForDeletionTerminates(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	queries := pg.New(service.Pool)

	const total = 2500

	_, err := service.Pool.Exec(ctx, `
		INSERT INTO objects (key, refs, deleted_at, first_deleted_at)
		SELECT 'claim/' || i, '{}', timezone('UTC', now()), timezone('UTC', now())
		FROM generate_series(1, $1) AS i`, total)
	ok(t, err)

	claimAll := func(runStartedAt pgtype.Timestamp) map[string]int {
		t.Helper()

		seen := make(map[string]int)

		for rounds := 0; ; rounds++ {
			if rounds > total/server.DeletionBatchSize+1 {
				t.Fatalf("claim loop did not terminate after %d rounds", rounds)
			}

			keys, err := queries.ClaimObjectsForDeletion(ctx, pg.ClaimObjectsForDeletionParams{
				RunStartedAt:       runStartedAt,
				GracePeriodSeconds: 0,
				LimitCount:         server.DeletionBatchSize,
			})
			ok(t, err)

			if len(keys) == 0 {
				return seen
			}

			for _, k := range keys {
				seen[k]++
			}
		}
	}

	firstRun := dbNow(t, queries)

	seen := claimAll(firstRun)
	if len(seen) != total {
		t.Fatalf("first run claimed %d distinct objects, want %d", len(seen), total)
	}

	for k, n := range seen {
		if n != 1 {
			t.Fatalf("object %s was claimed %d times in one run", k, n)
		}
	}

	// The same run must not see its own claims again.
	if again := claimAll(firstRun); len(again) != 0 {
		t.Fatalf("same run re-claimed %d objects", len(again))
	}

	// A closure in flight shields its object from the next run.
	pendingClosure, err := queries.InsertPendingClosure(ctx, "shield.narinfo")
	ok(t, err)

	_, err = queries.InsertPendingObjects(ctx, []pg.InsertPendingObjectsParams{
		{PendingClosureID: pendingClosure.ID, Key: "claim/1", Refs: []string{}, NeedsUpload: false},
	})
	ok(t, err)

	// A later run treats the leftover claims as abandoned and picks them up.
	secondRun := dbNow(t, queries)

	seen = claimAll(secondRun)
	if len(seen) != total-1 {
		t.Fatalf("second run claimed %d objects, want %d", len(seen), total-1)
	}

	if _, claimed := seen["claim/1"]; claimed {
		t.Fatal("object referenced by a pending closure was claimed for deletion")
	}
}

// TestSharedObjectProtectedWhileClosureUploads is the regression test for a
// committed closure ending up with a reference to a deleted object. Closure A
// shares a NAR with old closure B. While A is uploading, GC deletes B and
// marks stale objects; the NAR must survive because A holds a pending row for
// it, and A's commit must resurrect it even if it was tombstoned meanwhile.
func TestSharedObjectProtectedWhileClosureUploads(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	queries := pg.New(service.Pool)

	hashB := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb01"
	hashA := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb02"
	narinfoB := hashB + ".narinfo"
	narinfoA := hashA + ".narinfo"
	narKey := narKeyFor(hashB)

	commitClosureDirect(t, service, queries, narinfoB, narKey)

	// Closure A starts uploading. The NAR is already cached, so the client
	// is only asked for the narinfo.
	resp := createPendingClosure(t, service, map[string]any{
		"closure": narinfoA,
		"objects": []map[string]any{
			{"key": narinfoA, "type": "narinfo", "refs": []string{narKey}},
			{"key": narKey, "type": "nar", "refs": []string{}, "nar_size": 1024},
		},
	})

	if _, offered := resp.PendingObjects[narKey]; offered {
		t.Fatal("cached NAR was offered for upload")
	}

	narinfo, offered := resp.PendingObjects[narinfoA]
	if !offered || narinfo.PresignedURL == "" {
		t.Fatal("narinfo of the new closure should be offered for upload")
	}

	var needsUpload bool

	err := service.Pool.QueryRow(ctx,
		"SELECT needs_upload FROM pending_objects WHERE key = $1", narKey).Scan(&needsUpload)
	ok(t, err)

	if needsUpload {
		t.Fatal("cached NAR should be recorded as present, not as needing upload")
	}

	// GC runs in the middle of A's upload: B is old enough to go.
	cutoff := dbNow(t, queries)
	cutoff.Time = cutoff.Time.Add(time.Second)

	deleted, err := queries.DeleteClosures(ctx, cutoff)
	ok(t, err)

	if deleted != 1 {
		t.Fatalf("expected closure B to be deleted, got %d deletions", deleted)
	}

	_, err = queries.MarkStaleObjects(ctx)
	ok(t, err)

	if deletedAt, _ := objectDeletedAt(t, service, narKey); deletedAt.Valid {
		t.Fatal("NAR referenced by an in-flight closure was tombstoned")
	}

	if deletedAt, _ := objectDeletedAt(t, service, narinfoB); !deletedAt.Valid {
		t.Fatal("narinfo of the deleted closure should be tombstoned")
	}

	claimed, err := queries.ClaimObjectsForDeletion(ctx, pg.ClaimObjectsForDeletionParams{
		RunStartedAt:       dbNow(t, queries),
		GracePeriodSeconds: 0,
		LimitCount:         100,
	})
	ok(t, err)

	for _, key := range claimed {
		if key == narKey {
			t.Fatal("NAR referenced by an in-flight closure was claimed for deletion")
		}
	}

	// Even a tombstone placed on the NAR (e.g. by a run that could not yet
	// see A's pending row) is undone when A commits.
	_, err = service.Pool.Exec(ctx,
		"UPDATE objects SET deleted_at = timezone('UTC', now()), first_deleted_at = timezone('UTC', now()) WHERE key = $1", narKey)
	ok(t, err)

	handlePresignedUpload(ctx, t, narinfo.PresignedURL)
	commitPendingClosure(t, service, resp.ID, nil)

	deletedAt, exists := objectDeletedAt(t, service, narKey)
	if !exists {
		t.Fatal("NAR row vanished")
	}

	if deletedAt.Valid {
		t.Fatal("committing closure A did not resurrect the NAR it references")
	}

	if _, err := queries.GetClosure(ctx, narinfoA); err != nil {
		t.Fatalf("closure A not committed: %v", err)
	}
}

// startCreatePendingClosure runs the create handler in the background and
// delivers the recorder once it returns.
func startCreatePendingClosure(t *testing.T, service *server.Service, request map[string]any) <-chan *httptest.ResponseRecorder {
	t.Helper()

	body, err := json.Marshal(request)
	ok(t, err)

	done := make(chan *httptest.ResponseRecorder, 1)

	go func() {
		rr := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/pending_closures", bytes.NewReader(body))
		service.CreatePendingClosureHandler(rr, req)
		done <- rr
	}()

	return done
}

// TestCreatePendingClosureWaitsForClaimedObject verifies that a closure whose
// object GC has claimed for deletion waits until the deletion has finished and
// then re-uploads it, instead of racing a PUT against the S3 delete.
func TestCreatePendingClosureWaitsForClaimedObject(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()

	hash := "dddddddddddddddddddddddddddddd01"
	narinfoKey := hash + ".narinfo"
	narKey := narKeyFor(hash)

	_, err := service.Pool.Exec(ctx, `
		INSERT INTO objects (key, refs, deleted_at, first_deleted_at, deleting_at)
		VALUES ($1, '{}', timezone('UTC', now()), timezone('UTC', now()), timezone('UTC', now()))`, narKey)
	ok(t, err)

	done := startCreatePendingClosure(t, service, map[string]any{
		"closure": narinfoKey,
		"objects": []map[string]any{
			{"key": narinfoKey, "type": "narinfo", "refs": []string{narKey}},
			{"key": narKey, "type": "nar", "refs": []string{}, "nar_size": 1024},
		},
	})

	select {
	case rr := <-done:
		t.Fatalf("closure creation did not wait for the claimed object (status %d): %s", rr.Code, rr.Body.String())
	case <-time.After(1500 * time.Millisecond):
	}

	// GC finishes: the S3 object and the row are gone.
	_, err = service.Pool.Exec(ctx, "DELETE FROM objects WHERE key = $1", narKey)
	ok(t, err)

	var rr *httptest.ResponseRecorder

	select {
	case rr = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("closure creation did not resume after the object was deleted")
	}

	if rr.Code != http.StatusOK {
		t.Fatalf("closure creation failed with status %d: %s", rr.Code, rr.Body.String())
	}

	var resp server.PendingClosureResponse
	ok(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	if nar, offered := resp.PendingObjects[narKey]; !offered || nar.PresignedURL == "" {
		t.Fatal("deleted NAR should be offered for upload once GC is done with it")
	}
}

// TestCreatePendingClosureAfterClaimUndone covers the other way a wait can
// end: the S3 delete failed and GC marked the object active again, so it is
// present and needs no upload.
func TestCreatePendingClosureAfterClaimUndone(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	queries := pg.New(service.Pool)

	hash := "gggggggggggggggggggggggggggggg01"
	narinfoKey := hash + ".narinfo"
	narKey := narKeyFor(hash)

	_, err := service.Pool.Exec(ctx, `
		INSERT INTO objects (key, refs, deleted_at, first_deleted_at, deleting_at)
		VALUES ($1, '{}', timezone('UTC', now()), timezone('UTC', now()), timezone('UTC', now()))`, narKey)
	ok(t, err)

	done := startCreatePendingClosure(t, service, map[string]any{
		"closure": narinfoKey,
		"objects": []map[string]any{
			{"key": narinfoKey, "type": "narinfo", "refs": []string{narKey}},
			{"key": narKey, "type": "nar", "refs": []string{}, "nar_size": 1024},
		},
	})

	time.Sleep(500 * time.Millisecond)

	ok(t, queries.MarkObjectsAsActive(ctx, []string{narKey}))

	var rr *httptest.ResponseRecorder

	select {
	case rr = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("closure creation did not resume after the claim was undone")
	}

	if rr.Code != http.StatusOK {
		t.Fatalf("closure creation failed with status %d: %s", rr.Code, rr.Body.String())
	}

	var resp server.PendingClosureResponse
	ok(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	if _, offered := resp.PendingObjects[narKey]; offered {
		t.Fatal("NAR whose deletion was undone should not be offered for upload")
	}

	var needsUpload bool

	err = service.Pool.QueryRow(ctx,
		"SELECT needs_upload FROM pending_objects WHERE key = $1", narKey).Scan(&needsUpload)
	ok(t, err)

	if needsUpload {
		t.Fatal("present NAR should be recorded with needs_upload = false")
	}
}

// TestPresentIgnoresDeletedClosure verifies that the present check is tied to
// the closure row, so a closure GC has already deleted is not reported as
// cached while its objects are still live ahead of the marking phase.
func TestPresentIgnoresDeletedClosure(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	ctx := t.Context()
	queries := pg.New(service.Pool)

	hash := "ffffffffffffffffffffffffffffff01"
	narinfoKey := hash + ".narinfo"
	narKey := narKeyFor(hash)

	commitClosureDirect(t, service, queries, narinfoKey, narKey)

	present := func() []string {
		t.Helper()

		body, err := json.Marshal(api.PresentRequest{Keys: []string{narinfoKey}})
		ok(t, err)

		rr := testRequest(t, &TestRequest{
			method:  "POST",
			path:    "/api/objects/present",
			body:    body,
			handler: service.PresentHandler,
		})

		var resp api.PresentResponse
		ok(t, json.Unmarshal(rr.Body.Bytes(), &resp))

		return resp.Present
	}

	before, err := queries.GetClosure(ctx, narinfoKey)
	ok(t, err)

	time.Sleep(20 * time.Millisecond)

	if got := present(); len(got) != 1 || got[0] != narinfoKey {
		t.Fatalf("committed closure should be present, got %v", got)
	}

	after, err := queries.GetClosure(ctx, narinfoKey)
	ok(t, err)

	if !after.Time.After(before.Time) {
		t.Fatal("present check should refresh the closure's updated_at")
	}

	// GC deleted the closure but has not marked its objects yet.
	_, err = service.Pool.Exec(ctx, "DELETE FROM closures WHERE key = $1", narinfoKey)
	ok(t, err)

	if got := present(); len(got) != 0 {
		t.Fatalf("deleted closure must not be reported present, got %v", got)
	}
}
