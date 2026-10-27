-- ============================================================
-- Gold Execution Fencing Registry
-- Phase B.4.9.3b
-- ============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS public.gold_execution_fencing (
    dataset_name TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,

    fencing_token BIGINT NOT NULL
        CHECK (fencing_token > 0),

    attempt_id TEXT NOT NULL,

    updated_at TIMESTAMPTZ NOT NULL
        DEFAULT NOW(),

    CONSTRAINT gold_execution_fencing_pkey
        PRIMARY KEY (dataset_name, snapshot_id),

    CONSTRAINT gold_execution_fencing_attempt_check
        CHECK (
            attempt_id ~ '^attempt_[0-9a-f]{32}$'
        ),

    CONSTRAINT gold_execution_fencing_dataset_check
        CHECK (
            dataset_name ~ '^[a-z][a-z0-9_]*$'
        ),

    CONSTRAINT gold_execution_fencing_snapshot_check
        CHECK (
            snapshot_id ~ '^gold_snap_[0-9a-f]{32}$'
        )
);

COMMENT ON TABLE public.gold_execution_fencing IS
    'Current fencing token and execution attempt per Gold snapshot.';

COMMIT;