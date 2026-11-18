-- Metadata only for the isolated historical backfill.  This migration is not
-- executed by the application and intentionally does not alter incremental state.
CREATE TABLE IF NOT EXISTS public.historical_backfill_batches (
    batch_id VARCHAR(32) PRIMARY KEY CHECK (batch_id ~ '^[0-9a-f]{32}$'),
    run_id VARCHAR(64) NOT NULL,
    table_name VARCHAR(100) NOT NULL CHECK (table_name IN (
        'customers', 'products', 'orders', 'order_items', 'payments', 'shipments'
    )),
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    object_key TEXT NOT NULL UNIQUE,
    checksum_sha256 VARCHAR(64) NOT NULL CHECK (checksum_sha256 ~ '^[0-9a-f]{64}$'),
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1 CHECK (attempt > 0),
    status VARCHAR(20) NOT NULL CHECK (status IN ('PENDING', 'UPLOADED', 'VERIFIED', 'FAILED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- The window is part of the logical idempotency key; reject inverted windows
-- before a batch can participate in a retry or state transition.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'historical_backfill_window_order'
          AND conrelid = 'public.historical_backfill_batches'::regclass
    ) THEN
        ALTER TABLE public.historical_backfill_batches
            ADD CONSTRAINT historical_backfill_window_order CHECK (window_start < window_end);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_historical_backfill_batches_run
    ON public.historical_backfill_batches (run_id, table_name, created_at);

CREATE UNIQUE INDEX IF NOT EXISTS uq_historical_backfill_identity
    ON public.historical_backfill_batches (run_id, table_name, window_start, window_end);
