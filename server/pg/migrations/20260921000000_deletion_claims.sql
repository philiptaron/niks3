-- +goose Up
-- +goose StatementBegin

-- deleting_at marks a tombstoned object that garbage collection has claimed
-- for S3 deletion. A claimed row is never re-fetched by the same GC run, so
-- the deletion loop terminates, and clients creating a closure that contains
-- the key wait for the deletion to finish instead of racing it with a PUT.
ALTER TABLE objects ADD COLUMN deleting_at timestamp;

-- Every object of a closure gets a pending_objects row while the closure is
-- in flight, so MarkStaleObjects and the deletion claim leave it alone until
-- commit. needs_upload is false for objects that already exist in S3; the
-- client is not asked to upload those.
ALTER TABLE pending_objects ADD COLUMN needs_upload boolean NOT NULL DEFAULT true;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

ALTER TABLE pending_objects DROP COLUMN needs_upload;
ALTER TABLE objects DROP COLUMN deleting_at;

-- +goose StatementEnd
