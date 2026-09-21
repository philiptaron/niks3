-- +goose up

-- +goose statementbegin
-- Maintain object_stats running totals for the live object set.
--
-- A row contributes (1, size) while alive (deleted_at IS NULL), else (0, 0).
-- Applying contribution(new rows) - contribution(old rows) handles insert,
-- delete, tombstone and resurrect uniformly without branching on the
-- operation.
--
-- The triggers are statement-level and read the transition tables, so a
-- statement touching thousands of objects updates the single stats row once
-- instead of once per row. That keeps the stats row from becoming a chain of
-- dead tuples inside large commits and shortens the time the row lock is
-- held. Because AFTER triggers run at the end of the statement, every writer
-- locks its object rows first and the stats row last, which keeps the lock
-- order consistent across concurrent transactions.
CREATE OR REPLACE FUNCTION object_stats_apply()
RETURNS trigger AS $$
DECLARE
    d_count bigint := 0;
    d_bytes bigint := 0;
    n_count bigint;
    n_bytes bigint;
BEGIN
    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
        SELECT count(*), COALESCE(sum(size), 0)
        INTO n_count, n_bytes
        FROM new_rows
        WHERE deleted_at IS NULL;

        d_count := d_count + n_count;
        d_bytes := d_bytes + n_bytes;
    END IF;

    IF TG_OP = 'DELETE' OR TG_OP = 'UPDATE' THEN
        SELECT count(*), COALESCE(sum(size), 0)
        INTO n_count, n_bytes
        FROM old_rows
        WHERE deleted_at IS NULL;

        d_count := d_count - n_count;
        d_bytes := d_bytes - n_bytes;
    END IF;

    IF d_count <> 0 OR d_bytes <> 0 THEN
        UPDATE object_stats
        SET object_count = object_count + d_count,
            total_bytes = total_bytes + d_bytes
        WHERE id;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Transition tables cannot be declared on a trigger that fires for more than
-- one event, so there is one trigger per event.
DROP TRIGGER IF EXISTS object_stats_trigger ON objects;
DROP TRIGGER IF EXISTS object_stats_insert_trigger ON objects;
DROP TRIGGER IF EXISTS object_stats_update_trigger ON objects;
DROP TRIGGER IF EXISTS object_stats_delete_trigger ON objects;

CREATE TRIGGER object_stats_insert_trigger
AFTER INSERT ON objects
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION object_stats_apply();

CREATE TRIGGER object_stats_update_trigger
AFTER UPDATE ON objects
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION object_stats_apply();

CREATE TRIGGER object_stats_delete_trigger
AFTER DELETE ON objects
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT EXECUTE FUNCTION object_stats_apply();
-- +goose statementend
