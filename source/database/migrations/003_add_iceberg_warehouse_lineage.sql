-- Preserve legacy Parquet checkpoints while allowing a Warehouse load to be
-- traced to an immutable Iceberg snapshot on a Nessie reference.
ALTER TABLE warehouse.loaded_batches
    ADD COLUMN IF NOT EXISTS source_type TEXT NOT NULL DEFAULT 'parquet',
    ADD COLUMN IF NOT EXISTS iceberg_table TEXT,
    ADD COLUMN IF NOT EXISTS iceberg_catalog_ref TEXT,
    ADD COLUMN IF NOT EXISTS iceberg_snapshot_id BIGINT;

ALTER TABLE warehouse.loaded_batches
    DROP CONSTRAINT IF EXISTS loaded_batches_source_lineage_check;

ALTER TABLE warehouse.loaded_batches
    ADD CONSTRAINT loaded_batches_source_lineage_check CHECK (
        (source_type = 'parquet'
            AND iceberg_table IS NULL
            AND iceberg_catalog_ref IS NULL
            AND iceberg_snapshot_id IS NULL)
        OR
        (source_type = 'iceberg'
            AND iceberg_table IS NOT NULL
            AND iceberg_catalog_ref IS NOT NULL
            AND iceberg_snapshot_id IS NOT NULL)
    );
