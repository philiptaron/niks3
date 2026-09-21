-- name: InsertPendingClosure :one
INSERT INTO pending_closures (started_at, key)
VALUES (timezone('UTC', now()), $1)
RETURNING *;

-- name: InsertPendingObjects :copyfrom
INSERT INTO pending_objects (pending_closure_id, key, refs, size, needs_upload) VALUES ($1, $2, $3, $4, $5);

-- name: MarkPendingObjectsForUpload :exec
-- Flip objects the closure originally treated as present to needs_upload,
-- used when an S3 integrity check finds them missing.
UPDATE pending_objects SET needs_upload = true
WHERE pending_closure_id = sqlc.arg(pending_closure_id) AND key = any(sqlc.arg(keys)::varchar []);

-- name: Now :one
-- The database clock, so timestamps compared against columns stamped with
-- now() in SQL are not skewed by the application host's clock.
SELECT timezone('UTC', now())::timestamp AS now;

-- name: GetObjectStats :one
SELECT object_count, total_bytes FROM object_stats WHERE id;

-- name: CountPendingClosures :one
SELECT count(*) FROM pending_closures;

-- name: GetPendingObjectKeys :many
SELECT key FROM pending_objects
WHERE pending_closure_id = $1;

-- name: LockExistingObjects :many
-- Share-locks the object rows for the rest of the transaction so the
-- deletion claim (FOR UPDATE SKIP LOCKED) either sees the pending_objects
-- rows this transaction inserts or skips the objects for this run. Rows are
-- locked in key order to keep the lock order consistent across writers.
SELECT
    key,
    (deleted_at IS NOT NULL)::boolean AS tombstoned,
    deleting_at
FROM objects
WHERE key = any($1::varchar [])
ORDER BY key
FOR SHARE;

-- name: GetExistingObjects :many
SELECT
    key,
    (deleted_at IS NOT NULL)::boolean AS tombstoned,
    deleting_at
FROM objects
WHERE key = any($1::varchar []);

-- name: GetPresentObjects :many
-- GC-marked objects may vanish from S3 any moment, so they count as absent.
SELECT key FROM objects
WHERE key = any($1::varchar []) AND deleted_at IS NULL;

-- name: TouchPresentClosures :many
-- Refresh the closures whose narinfo is live and report which ones were
-- found. Doing the check and the touch in one statement means a closure
-- deleted by concurrent GC is never reported as present.
UPDATE closures SET updated_at = timezone('UTC', now())
FROM objects
WHERE closures.key = any($1::varchar [])
  AND objects.key = closures.key
  AND objects.deleted_at IS NULL
RETURNING closures.key;

-- name: CommitPendingClosure :exec
SELECT commit_pending_closure($1::bigint);

-- name: RegisterCompletedObject :exec
-- Record an object as soon as its upload completes so later closures don't
-- re-offer it if this closure never commits. Conflict handling matches
-- commit_pending_closure: merge refs, keep a known size, resurrect tombstones.
INSERT INTO objects (key, refs, size)
VALUES (sqlc.arg(key), sqlc.arg(refs)::varchar [], sqlc.arg(size))
ON CONFLICT (key) DO UPDATE SET
    refs = (
        SELECT ARRAY(
            SELECT DISTINCT unnest(objects.refs || excluded.refs)
        )
    ),
    size = coalesce(objects.size, excluded.size),
    deleted_at = NULL,
    first_deleted_at = NULL,
    deleting_at = NULL;

-- name: GetPendingObject :one
SELECT refs, size FROM pending_objects
WHERE pending_closure_id = $1 AND key = $2;

-- name: GetPendingObjectByKey :one
-- Any pending closure's row for this key; used to recover refs/size when the
-- upload is registered outside closure commit.
SELECT refs, size FROM pending_objects
WHERE key = $1
LIMIT 1;

-- name: CleanupPendingClosures :execrows
-- The cutoff is passed in so it matches the one used to select the multipart
-- uploads that were aborted just before; recomputing now() here would let
-- closures that aged past the cutoff in between be deleted without an abort.
WITH old_closures AS (
    SELECT id
    FROM pending_closures
    WHERE started_at < sqlc.arg(cutoff)::timestamp
),

-- Insert pending objects into objects table if they don't already exist
-- We mark them as deleted so they can be cleaned up later
inserted_objects AS (
    INSERT INTO objects (key, refs, deleted_at, first_deleted_at)
    SELECT
        po.key,
        po.refs,
        sqlc.arg(cutoff)::timestamp,
        sqlc.arg(cutoff)::timestamp
    FROM pending_objects AS po
    JOIN old_closures oc ON po.pending_closure_id = oc.id
    ON CONFLICT (key) DO NOTHING
    RETURNING key
),

-- Delete pending objects that were inserted into the objects table
deleted_pending_objects AS (
    DELETE FROM pending_objects
    USING old_closures
    WHERE pending_objects.pending_closure_id = old_closures.id
    RETURNING pending_closure_id
)

-- Delete pending closures older than the specified interval
-- This will cascade to pending_objects
DELETE FROM pending_closures
USING old_closures
WHERE pending_closures.id = old_closures.id;

-- name: GetClosure :one
SELECT updated_at FROM closures
WHERE key = $1 LIMIT 1;

-- name: GetClosureObjects :many
-- Return objects reachable from the given closure key
WITH RECURSIVE closure_reach AS (
    -- Start with the provided closure key
    SELECT o.key, o.refs
    FROM objects o
    WHERE o.key = $1
    UNION
    -- Recursively add all referenced objects
    SELECT o.key, o.refs
    FROM objects o
    INNER JOIN closure_reach cr ON o.key = ANY(cr.refs)
)
SELECT DISTINCT key FROM closure_reach;

-- name: DeleteClosures :execrows
-- Delete old closures, but exclude any that are pinned
DELETE FROM closures
WHERE closures.updated_at < $1
  AND closures.key NOT IN (SELECT narinfo_key FROM pins);

-- name: MarkObjectsAsActive :exec
-- Undo a deletion claim whose S3 delete failed. first_deleted_at is kept so
-- the next mark makes the object eligible again without a new grace period.
UPDATE objects SET deleted_at = NULL, deleting_at = NULL
WHERE key = any($1::varchar []);

-- name: DeleteObjects :exec
DELETE FROM objects
WHERE key = any($1::varchar []);

-- name: InsertMultipartUpload :exec
INSERT INTO multipart_uploads (pending_closure_id, object_key, upload_id)
VALUES ($1, $2, $3);

-- name: GetOldMultipartUploads :many
SELECT upload_id, object_key
FROM multipart_uploads mu
JOIN pending_closures pc ON mu.pending_closure_id = pc.id
WHERE pc.started_at < sqlc.arg(cutoff)::timestamp;

-- name: DeleteMultipartUpload :exec
DELETE FROM multipart_uploads
WHERE upload_id = $1;

-- name: GetRedundantMultipartUploads :many
-- Upload IDs other pending_closures opened for object_key, used to abort
-- duplicates once one upload of the NAR completes.
SELECT upload_id
FROM multipart_uploads
WHERE object_key = $1 AND upload_id <> $2;

-- name: GetMultipartUpload :one
SELECT pending_closure_id, object_key, upload_id
FROM multipart_uploads
WHERE upload_id = $1 AND object_key = $2;

-- name: MarkStaleObjects :execrows
WITH RECURSIVE ct AS (
    SELECT timezone('UTC', now()) AS now
),
-- Find all objects reachable from any closure
closure_reach AS (
    -- Start with all closure keys
    SELECT o.key, o.refs
    FROM objects o
    INNER JOIN closures c ON o.key = c.key
    UNION
    -- Recursively add all referenced objects
    SELECT o.key, o.refs
    FROM objects o
    INNER JOIN closure_reach cr ON o.key = ANY(cr.refs)
),
reachable_objects AS (
    SELECT DISTINCT key FROM closure_reach
),
stale_objects AS (
    SELECT o.key
    FROM objects AS o, ct
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM reachable_objects ro
            WHERE ro.key = o.key
        )
        AND NOT EXISTS (
            SELECT 1
            FROM pending_objects AS po
            WHERE po.key = o.key
        )
        AND o.deleted_at IS NULL  -- Only mark fresh objects
    FOR UPDATE
)
UPDATE objects
SET
    deleted_at = ct.now,
    first_deleted_at = COALESCE(first_deleted_at, ct.now)
FROM stale_objects, ct
WHERE objects.key = stale_objects.key;

-- name: ClaimObjectsForDeletion :many
-- Claims one batch of tombstoned objects past the grace period for S3
-- deletion by stamping deleting_at. Claimed rows drop out of the next call,
-- so a GC run terminates even when some deletions fail; rows still claimed
-- from a run that died (deleting_at before this run started) are picked up
-- again. Objects referenced by an in-flight closure are left alone, and rows
-- share-locked by a closure being created are skipped for this run.
UPDATE objects SET deleting_at = timezone('UTC', now())
WHERE key IN (
    SELECT key
    FROM objects
    WHERE deleted_at IS NOT NULL
      AND (deleting_at IS NULL OR deleting_at < sqlc.arg(run_started_at)::timestamp)
      AND first_deleted_at <= timezone('UTC', now()) - interval '1 second' * sqlc.arg(grace_period_seconds)::int
      AND NOT EXISTS (
          SELECT 1 FROM pending_objects AS po WHERE po.key = objects.key
      )
    ORDER BY key
    LIMIT sqlc.arg(limit_count)
    FOR UPDATE SKIP LOCKED
)
RETURNING key;

-- name: TouchClosureForPin :one
-- Refresh and lock the closure row so concurrent GC cannot delete it before
-- the pin upsert commits: DeleteClosures blocks on the row and then
-- re-evaluates updated_at against its cutoff, which now fails.
UPDATE closures SET updated_at = timezone('UTC', now())
WHERE key = $1
RETURNING updated_at;

-- name: UpsertPin :exec
-- Create or update a pin. Updates the narinfo_key, store_path, and updated_at if the pin already exists.
INSERT INTO pins (name, narinfo_key, store_path, created_at, updated_at)
VALUES ($1, $2, $3, timezone('UTC', now()), timezone('UTC', now()))
ON CONFLICT (name) DO UPDATE SET
    narinfo_key = EXCLUDED.narinfo_key,
    store_path = EXCLUDED.store_path,
    updated_at = timezone('UTC', now());

-- name: GetPin :one
SELECT name, narinfo_key, store_path, created_at, updated_at
FROM pins
WHERE name = $1;

-- name: DeletePin :exec
DELETE FROM pins
WHERE name = $1;

-- name: ListPins :many
SELECT name, narinfo_key, store_path, created_at, updated_at
FROM pins
ORDER BY name;
