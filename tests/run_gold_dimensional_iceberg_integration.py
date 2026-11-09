"""Disposable, opt-in integration harness; deliberately not collected by pytest.

Run only against an isolated Nessie ref and disposable MinIO bucket.  It does
not create clients, read environment credentials, or execute writes by itself.
Deployment automation must inject those dependencies explicitly.
"""

from gold.implementation.dimensional.writer import DRY_RUN, run_dimensional_graph


def dry_run_harness(spark, client, bucket, lineage, *, dim_date_start, dim_date_end):
    """Exercise verified snapshot reads and all quality gates without Gold writes."""
    return run_dimensional_graph(
        spark, client, bucket, lineage,
        dim_date_start=dim_date_start, dim_date_end=dim_date_end, mode=DRY_RUN,
    )


def write_scenarios():
    """Manual disposable scenarios: global-fence contention and target drift."""
    return ("concurrent_distinct_graph_identities", "existing_target_schema_or_property_drift")
