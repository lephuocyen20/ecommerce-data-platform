-- Author-only migration. Do not apply to a shared or production database.
BEGIN;

CREATE TABLE IF NOT EXISTS public.gold_run_metadata (
    run_id VARCHAR(255) NOT NULL,
    dataset_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('PASS', 'FAIL', 'DRY_RUN')),
    code_sha VARCHAR(64) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    duration_seconds DOUBLE PRECISION NOT NULL CHECK (duration_seconds >= 0),
    rows_read BIGINT NOT NULL CHECK (rows_read >= 0),
    rows_written BIGINT NOT NULL CHECK (rows_written >= 0),
    rows_rejected BIGINT NOT NULL CHECK (rows_rejected >= 0),
    metadata JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, dataset_name)
);

COMMIT;
