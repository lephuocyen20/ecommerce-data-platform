-- Author-only migration. Do not apply to a shared or production database.
BEGIN;

ALTER TABLE public.order_items
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN IF NOT EXISTS _is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;

ALTER TABLE public.payments
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN IF NOT EXISTS _is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;

ALTER TABLE raw.order_items
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN IF NOT EXISTS _is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;

ALTER TABLE raw.payments
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN IF NOT EXISTS _is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;

DO $$
DECLARE
    table_name TEXT;
    constraint_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY['order_items', 'payments'] LOOP
        constraint_name := table_name || '_tombstone_state_ck';
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = format('public.%I', table_name)::regclass
              AND conname = constraint_name
        ) THEN
            EXECUTE format(
                'ALTER TABLE public.%I ADD CONSTRAINT %I CHECK ((_is_deleted AND deleted_at IS NOT NULL) OR (NOT _is_deleted AND deleted_at IS NULL))',
                table_name, constraint_name
            );
        END IF;
    END LOOP;
END $$;

DO $$
DECLARE
    table_name TEXT;
    constraint_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY['order_items', 'payments'] LOOP
        constraint_name := table_name || '_tombstone_state_ck';
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = format('raw.%I', table_name)::regclass
              AND conname = constraint_name
        ) THEN
            EXECUTE format(
                'ALTER TABLE raw.%I ADD CONSTRAINT %I CHECK ((_is_deleted AND deleted_at IS NOT NULL) OR (NOT _is_deleted AND deleted_at IS NULL))',
                table_name, constraint_name
            );
        END IF;
    END LOOP;
END $$;

ALTER TABLE public.ingestion_watermarks
    ADD COLUMN IF NOT EXISTS replay_cursor_timestamp TIMESTAMP,
    ADD COLUMN IF NOT EXISTS replay_cursor_id BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS replay_upper_timestamp TIMESTAMP;
ALTER TABLE public.ingestion_watermarks ADD COLUMN IF NOT EXISTS replay_upper_id BIGINT NOT NULL DEFAULT 0;

ALTER TABLE public.ingestion_batches
    ADD COLUMN IF NOT EXISTS replay_cursor_timestamp TIMESTAMP,
    ADD COLUMN IF NOT EXISTS replay_cursor_id BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS replay_upper_timestamp TIMESTAMP;
ALTER TABLE public.ingestion_batches ADD COLUMN IF NOT EXISTS replay_upper_id BIGINT NOT NULL DEFAULT 0;

-- Convert legacy ID checkpoints only when every referenced source row can
-- supply a deterministic timestamp cursor. Missing source history aborts the
-- migration before any checkpoint is changed; operators must reconcile that
-- continuity gap explicitly.
DO $$
DECLARE
    table_name TEXT;
    missing_count BIGINT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY['order_items', 'payments'] LOOP
        EXECUTE format(
            'SELECT count(*) FROM public.ingestion_watermarks w
             WHERE w.table_name = %L AND w.watermark_type = ''ID''
               AND w.last_id > 0
               AND NOT EXISTS (SELECT 1 FROM public.%I s WHERE s.%I = w.last_id)',
            table_name, table_name,
            CASE table_name WHEN 'order_items' THEN 'order_item_id' ELSE 'payment_id' END
        ) INTO missing_count;
        IF missing_count > 0 THEN
            RAISE EXCEPTION 'Cannot convert % ID watermark: source checkpoint row is missing', table_name;
        END IF;
        EXECUTE format(
            'SELECT count(*) FROM public.ingestion_batches b
             WHERE b.table_name = %L AND b.watermark_type = ''ID''
               AND (NOT EXISTS (SELECT 1 FROM public.%I s WHERE s.%I = b.end_id)
                    OR (b.start_id > 0 AND NOT EXISTS (SELECT 1 FROM public.%I s WHERE s.%I = b.start_id)))',
            table_name, table_name,
            CASE table_name WHEN 'order_items' THEN 'order_item_id' ELSE 'payment_id' END,
            table_name, CASE table_name WHEN 'order_items' THEN 'order_item_id' ELSE 'payment_id' END
        ) INTO missing_count;
        IF missing_count > 0 THEN
            RAISE EXCEPTION 'Cannot convert % ID batches: batch end row is missing', table_name;
        END IF;
        EXECUTE format(
            'UPDATE public.ingestion_batches b SET watermark_type = ''TIMESTAMP'',
                start_timestamp = CASE WHEN b.start_id = 0 THEN NULL ELSE (SELECT s.updated_at FROM public.%I s WHERE s.%I = b.start_id) END,
                end_timestamp = (SELECT s.updated_at FROM public.%I s WHERE s.%I = b.end_id)
             WHERE b.table_name = %L AND b.watermark_type = ''ID''',
            table_name, CASE table_name WHEN 'order_items' THEN 'order_item_id' ELSE 'payment_id' END,
            table_name, CASE table_name WHEN 'order_items' THEN 'order_item_id' ELSE 'payment_id' END, table_name
        );
        EXECUTE format(
            'UPDATE public.ingestion_watermarks w SET watermark_type = ''TIMESTAMP'',
                last_timestamp = (SELECT s.updated_at FROM public.%I s WHERE s.%I = w.last_id)
             WHERE w.table_name = %L AND w.watermark_type = ''ID''',
            table_name, CASE table_name WHEN 'order_items' THEN 'order_item_id' ELSE 'payment_id' END, table_name
        );
    END LOOP;
END $$;

CREATE INDEX IF NOT EXISTS idx_order_items_updated_at_order_item_id
    ON public.order_items (updated_at, order_item_id);
CREATE INDEX IF NOT EXISTS idx_payments_updated_at_payment_id
    ON public.payments (updated_at, payment_id);

DROP TRIGGER IF EXISTS trg_order_items_updated_at ON public.order_items;
CREATE TRIGGER trg_order_items_updated_at
BEFORE UPDATE ON public.order_items
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS trg_payments_updated_at ON public.payments;
CREATE TRIGGER trg_payments_updated_at
BEFORE UPDATE ON public.payments
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

COMMIT;
