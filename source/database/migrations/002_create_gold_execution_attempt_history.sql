
-- ============================================================
-- Gold Execution Attempt History
-- Phase B.4.9.3f
-- ============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS public.gold_execution_attempt_history (
    dataset_name TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    fencing_token BIGINT NOT NULL,
    allocated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT gold_attempt_history_pkey
        PRIMARY KEY (
            dataset_name,
            snapshot_id,
            attempt_id
        ),

    CONSTRAINT gold_attempt_history_token_unique
        UNIQUE (
            dataset_name,
            snapshot_id,
            fencing_token
        ),

    CONSTRAINT gold_attempt_history_token_check
        CHECK (fencing_token > 0),

    CONSTRAINT gold_attempt_history_attempt_check
        CHECK (
            attempt_id ~ '^attempt_[0-9a-f]{32}$'
        ),

    CONSTRAINT gold_attempt_history_dataset_check
        CHECK (
            dataset_name ~ '^[a-z][a-z0-9_]*$'
        ),

    CONSTRAINT gold_attempt_history_snapshot_check
        CHECK (
            snapshot_id ~ '^gold_snap_[0-9a-f]{32}$'
        )
);

COMMENT ON TABLE public.gold_execution_attempt_history IS
    'Append-only history of Gold fencing token allocations.';

COMMIT;
