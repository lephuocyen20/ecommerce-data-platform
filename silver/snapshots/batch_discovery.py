from psycopg2.extras import RealDictCursor

from ingestion.incremental.config import get_table_config


def get_committed_batches(conn, table_name):
    """Get all committed ingestion batches for one table."""
    get_table_config(table_name)

    query = """
        SELECT
            batch_id,
            table_name,
            watermark_type,
            start_timestamp,
            start_id,
            end_timestamp,
            end_id,
            row_count,
            object_key,
            checksum_sha256
        FROM public.ingestion_batches
        WHERE status = 'COMMITTED'
          AND table_name = %s
        ORDER BY
            end_timestamp ASC NULLS FIRST,
            end_id ASC,
            batch_id ASC
    """

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query, (table_name,))
        return [dict(row) for row in cursor.fetchall()]