"""Native Iceberg v2 writer for the first dimensional Gold graph.

The module keeps all writes behind an explicit caller-supplied WRITE mode and
feature flag.  Source reads and transformations are always pinned and pure up
to the final MERGE boundary.
"""

import hashlib
import json
import os

from .contracts import DIMENSIONAL_CATALOG, DIMENSIONAL_NAMESPACE, TABLE_COLUMNS, TABLE_DDLS, TABLE_MERGE_KEYS, TABLE_SCHEMA, WRITE_ORDER
from .manifest import (build_graph_manifest, build_graph_run_manifest, build_table_manifest, graph_manifest_key,
                       acquire_global_fence, acquire_graph_fence, assert_global_fence, assert_graph_fence,
                       publish_manifest, read_completed_graph, release_global_fence,
                       read_table_manifests, table_manifest_key)
from .quality import validate_order_id_parity, validate_table_frame
from .coverage import validate_cross_table_coverage
from .date_coverage import derive_order_date_coverage
from .reader import read_dimensional_sources
from .transformations import build_dim_customer, build_dim_date, build_dim_product, build_fact_orders, build_fact_order_items, build_fact_payments, build_fact_shipments
from .snapshot_reuse import GOLD_SNAPSHOT_REUSE, SnapshotReuseContractError, snapshot_reuse_enabled, validate_incremental_merge_plan
from common.bounded_e2e_scope import TEST_BUCKET

DRY_RUN = "DRY_RUN"
WRITE = "WRITE"

ICEBERG_TARGET_PROPERTIES = {
    "format-version": "2",
    "write.format.default": "parquet",
}


class DimensionalWriteError(ValueError):
    pass


def _namespace(scope=None):
    return scope.gold_namespace if scope is not None else DIMENSIONAL_NAMESPACE


def _identifier(table, scope=None):
    if table not in WRITE_ORDER:
        raise DimensionalWriteError(f"Target table is not allowlisted: {table}")
    return f"{DIMENSIONAL_CATALOG}.{_namespace(scope)}.{table}"


def assert_write_allowed(mode, *, write_guard=None):
    if mode not in {DRY_RUN, WRITE}:
        raise DimensionalWriteError("mode must be DRY_RUN or WRITE")
    if mode == WRITE:
        if write_guard is not None:
            authorize = getattr(write_guard, "authorize_dimensional_write", None)
            if not callable(authorize) or authorize() is not True:
                raise DimensionalWriteError("WRITE requires an approved dimensional write guard")
        elif os.getenv("GOLD_DIMENSIONAL_WRITE_ENABLED", "false").lower() != "true":
            raise DimensionalWriteError("WRITE requires GOLD_DIMENSIONAL_WRITE_ENABLED=true")


def create_target_table(spark, table, scope=None):
    identifier = _identifier(table, scope)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {DIMENSIONAL_CATALOG}.{_namespace(scope)}")
    properties = ", ".join(f"'{key}'='{value}'" for key, value in ICEBERG_TARGET_PROPERTIES.items())
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {identifier} ({TABLE_DDLS[table]}) USING iceberg "
        f"TBLPROPERTIES ({properties})"
    )
    catalog = getattr(spark, "catalog", None)
    refresh_table = getattr(catalog, "refreshTable", None)
    if callable(refresh_table):
        refresh_table(identifier)
    validate_target_table(spark, table, scope)


def validate_target_table(spark, table, scope=None):
    """Reject pre-existing Gold tables that drift from the frozen v1 contract."""
    identifier = _identifier(table, scope)
    schema = tuple((field.name, field.dataType.simpleString(), field.nullable) for field in spark.table(identifier).schema.fields)
    if schema != TABLE_SCHEMA[table]:
        raise DimensionalWriteError(f"Existing Iceberg target schema drift for {table}")
    rows = spark.sql(f"SHOW TBLPROPERTIES {identifier}").collect()
    properties = {row[0]: row[1] for row in rows}
    for property_name, expected_value in ICEBERG_TARGET_PROPERTIES.items():
        if properties.get(property_name) != expected_value:
            raise DimensionalWriteError(f"Existing Iceberg target {property_name} mismatch for {table}")
    return True


def merge_table(spark, table, dataframe, scope=None):
    """Idempotently upsert one validated target using its declared grain."""
    identifier = _identifier(table, scope)
    if tuple(dataframe.columns) != TABLE_COLUMNS[table]:
        raise DimensionalWriteError(f"Refusing schema drift for {table}")
    view = f"_gold_dimensional_{table}"
    dataframe.createOrReplaceTempView(view)
    keys = TABLE_MERGE_KEYS[table]
    predicate = " AND ".join(f"target.{key} = source.{key}" for key in keys)
    columns = TABLE_COLUMNS[table]
    assignments = ", ".join(f"{column} = source.{column}" for column in columns)
    column_list = ", ".join(columns)
    values = ", ".join(f"source.{column}" for column in columns)
    spark.sql(
        f"MERGE INTO {identifier} target USING {view} source ON {predicate} "
        f"WHEN MATCHED THEN UPDATE SET {assignments} "
        f"WHEN NOT MATCHED THEN INSERT ({column_list}) VALUES ({values})"
    )


def current_snapshot_id(spark, table, scope=None):
    identifier = _identifier(table, scope)
    rows = spark.sql(f"SELECT snapshot_id FROM {identifier}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    if len(rows) != 1 or not isinstance(rows[0].snapshot_id, int) or rows[0].snapshot_id <= 0:
        raise DimensionalWriteError(f"No current Iceberg snapshot for {table}")
    return rows[0].snapshot_id


def read_table_readback(spark, table, snapshot_id, scope=None):
    """Read-only snapshot/file/row evidence for a completed Gold table."""
    identifier = _identifier(table, scope)
    try:
        # Iceberg's `.files` metadata table is current-snapshot scoped when
        # queried directly. Use the documented reader option to pin the
        # metadata table itself to the requested historical snapshot.
        pinned_files = (
            spark.read.format("iceberg")
            .option("snapshot-id", snapshot_id)
            .load(f"{identifier}.files")
        )
        file_row = pinned_files.selectExpr(
            "count(*) AS file_count",
            "coalesce(sum(file_size_in_bytes), 0) AS file_bytes",
        ).first()
        # The data read must use the same pinned snapshot, never table head.
        pinned_rows = (
            spark.read.format("iceberg")
            .option("snapshot-id", snapshot_id)
            .load(identifier)
        )
        row_count = pinned_rows.count()
    except Exception as exc:
        raise DimensionalWriteError(
            f"Pinned snapshot row read-back is unavailable for {table}@{snapshot_id}"
        ) from exc
    if file_row is None or file_row.file_count <= 0 or row_count < 0:
        raise DimensionalWriteError(f"Read-back evidence is incomplete for {table}")
    evidence = {
        "table": table,
        "snapshot_id": int(snapshot_id),
        "file_count": int(file_row.file_count),
        "file_bytes": int(file_row.file_bytes),
        "row_count": int(row_count),
        "status": "PASS",
    }
    evidence["identity"] = hashlib.sha256(
        json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    evidence["readback_identity"] = evidence["identity"]
    return evidence


def build_graph_frames(spark, sources, *, dim_date_start, dim_date_end, lineage=None):
    if lineage is not None:
        validate_order_id_parity(sources, lineage)
    frames = {
        "dim_date": build_dim_date(spark, dim_date_start, dim_date_end),
        "dim_customer": build_dim_customer(sources["customers"]),
        "dim_product": build_dim_product(sources["products"]),
    }
    frames["fact_orders"] = build_fact_orders(sources["orders"], sources["order_items"], frames["dim_customer"])
    frames["fact_order_items"] = build_fact_order_items(sources["order_items"], frames["dim_product"])
    frames["fact_payments"] = build_fact_payments(sources["payments"])
    frames["fact_shipments"] = build_fact_shipments(sources["shipments"])
    quality = {table: validate_table_frame(table, frames[table], frames) for table in WRITE_ORDER}
    return frames, quality


def build_graph_frames_from_merge_plan(spark, merge_plan, *, dim_date_start, dim_date_end, lineage=None):
    """Build frames from verified plan data; never perform an implicit snapshot read."""
    try:
        validate_incremental_merge_plan(merge_plan)
    except SnapshotReuseContractError as exc:
        raise DimensionalWriteError(f"Invalid incremental Gold merge plan: {exc}") from exc
    plan_sources = merge_plan["sources"]
    sources = {name: item["data"] for name, item in plan_sources.items()}
    if lineage is not None:
        validate_order_id_parity(sources, lineage)
    frames = {
        "dim_date": build_dim_date(spark, dim_date_start, dim_date_end),
        "dim_customer": sources["customers"] if plan_sources["customers"]["kind"] == GOLD_SNAPSHOT_REUSE else build_dim_customer(sources["customers"]),
        "dim_product": sources["products"] if plan_sources["products"]["kind"] == GOLD_SNAPSHOT_REUSE else build_dim_product(sources["products"]),
    }
    frames["fact_orders"] = build_fact_orders(sources["orders"], sources["order_items"], frames["dim_customer"])
    frames["fact_order_items"] = build_fact_order_items(sources["order_items"], frames["dim_product"])
    frames["fact_payments"] = build_fact_payments(sources["payments"])
    frames["fact_shipments"] = build_fact_shipments(sources["shipments"])
    quality = {table: validate_table_frame(table, frames[table], frames) for table in WRITE_ORDER}
    return frames, quality


def recovery_state(graph_published, table_states):
    """Classify a retry without mutating state; caller supplies manifest evidence."""
    if graph_published:
        return "ALREADY_PUBLISHED"
    completed = {table for table, state in table_states.items() if state in {"PUBLISHED", "ALREADY_PUBLISHED"}}
    if completed:
        return "PARTIAL_REQUIRES_RECONCILIATION"
    return "NEW"


def run_dimensional_graph(spark, client, bucket, lineage, *, dim_date_start, dim_date_end, mode=DRY_RUN,
                          scope=None, configured_date_start=None, configured_date_end=None, merge_plan=None,
                          write_guard=None, captured_at=None, nessie_ref=None):
    """Validate and optionally publish the whole Gold graph in dependency order.

    A failed write intentionally leaves the graph manifest unpublished.  The
    returned partial table list is evidence for an operator-led reconciliation
    before any retry; this avoids treating a partial run as a successful graph.
    """
    assert_write_allowed(mode, write_guard=write_guard)
    if merge_plan is not None and mode != DRY_RUN and not snapshot_reuse_enabled():
        raise DimensionalWriteError(
            "Incremental snapshot reuse requires GOLD_INCREMENTAL_SNAPSHOT_REUSE_ENABLED=true"
        )
    if bucket == TEST_BUCKET and scope is None:
        raise DimensionalWriteError("ecommerce-data-lake WRITE requires a validated bounded E2E scope")
    if scope is not None and bucket != scope.bucket:
        raise DimensionalWriteError("E2E scope bucket does not match writer bucket")
    effective_ref = scope.nessie_ref if scope is not None else nessie_ref
    if not isinstance(bucket, str) or not bucket:
        raise DimensionalWriteError("Writer requires an explicit runtime bucket")
    if mode == WRITE and (not isinstance(effective_ref, str) or not effective_ref):
        raise DimensionalWriteError("Writer requires an explicit runtime Nessie ref")
    validate_cross_table_coverage(lineage)
    historical_coverage = configured_date_start is not None or configured_date_end is not None
    sources = None
    coverage = None
    if merge_plan is not None:
        if historical_coverage:
            raise DimensionalWriteError("Incremental merge plan cannot use historical coverage")
        graph = build_graph_manifest(lineage, dim_date_start=dim_date_start, dim_date_end=dim_date_end, scope=scope)
    elif historical_coverage:
        sources = read_dimensional_sources(spark, client, bucket, lineage, scope)
        coverage = derive_order_date_coverage(
            sources["orders"], configured_date_start or dim_date_start, configured_date_end or dim_date_end
        )
        graph = build_graph_manifest(
            lineage, dim_date_start=coverage["effective_start"], dim_date_end=coverage["effective_end"], scope=scope,
            configured_date_start=configured_date_start or dim_date_start,
            configured_date_end=configured_date_end or dim_date_end,
            observed_order_date_min=coverage["observed_min"], observed_order_date_max=coverage["observed_max"],
        )
    else:
        graph = build_graph_manifest(lineage, dim_date_start=dim_date_start, dim_date_end=dim_date_end, scope=scope)
    completed = read_completed_graph(client, bucket, graph, scope)
    if completed is not None:
        manifests = read_table_manifests(client, bucket, graph, scope)
        return {
            "status": "ALREADY_PUBLISHED", "graph": graph, "graph_run": completed,
            "tables": [{"table": table, "state": "ALREADY_PUBLISHED", "manifest": manifests[table]}
                       for table in WRITE_ORDER if table in manifests],
        }
    table_manifests = read_table_manifests(client, bucket, graph, scope)
    state = recovery_state(False, {table: "PUBLISHED" for table in table_manifests})
    if state == "PARTIAL_REQUIRES_RECONCILIATION":
        return {"status": state, "graph": graph, "tables": sorted(table_manifests)}
    # A persistent immutable fence serializes writers.  It is acquired before
    # source reads so no Spark MERGE can begin under an ambiguous ownership.
    # Shared-bucket E2E runs are isolated by their immutable prefix and must
    # never create or remove the legacy bucket-global coordination object.
    global_fence = acquire_global_fence(client, bucket, graph, scope) if mode == WRITE and scope is None else None
    try:
        fence = acquire_graph_fence(client, bucket, graph, scope) if mode == WRITE else None
    except Exception:
        # No target write occurred when the per-graph fence cannot be obtained.
        # Releasing the just-created global coordination object is safe here.
        if global_fence is not None:
            release_global_fence(client, bucket, global_fence)
        raise
    if mode == WRITE:
        # Another writer can complete after the first preflight; recheck under
        # the fence before any read or write.
        completed = read_completed_graph(client, bucket, graph, scope)
        table_manifests = read_table_manifests(client, bucket, graph, scope)
        if completed is not None:
            if global_fence is not None:
                release_global_fence(client, bucket, global_fence)
            return {
                "status": "ALREADY_PUBLISHED", "graph": graph, "graph_run": completed,
                "tables": [{"table": table, "state": "ALREADY_PUBLISHED", "manifest": table_manifests[table]}
                           for table in WRITE_ORDER if table in table_manifests],
            }
        if table_manifests:
            if global_fence is not None:
                release_global_fence(client, bucket, global_fence)
            return {"status": "PARTIAL_REQUIRES_RECONCILIATION", "graph": graph, "tables": sorted(table_manifests)}
        if global_fence is not None:
            assert_global_fence(client, bucket, global_fence, scope)
        assert_graph_fence(client, bucket, graph, fence, scope)
    if merge_plan is not None:
        coverage = {"effective_start": dim_date_start, "effective_end": dim_date_end}
        frames, quality = build_graph_frames_from_merge_plan(
            spark, merge_plan, dim_date_start=coverage["effective_start"],
            dim_date_end=coverage["effective_end"], lineage=lineage
        )
    elif sources is None:
        sources = read_dimensional_sources(spark, client, bucket, lineage, scope)
        coverage = {"effective_start": dim_date_start, "effective_end": dim_date_end}
    if merge_plan is None:
        frames, quality = build_graph_frames(
            spark, sources, dim_date_start=coverage["effective_start"], dim_date_end=coverage["effective_end"], lineage=lineage
        )
    if mode == DRY_RUN:
        # A dry run may expose quality/readback-shaped diagnostics, but never
        # emits a reusable PASS manifest or snapshot metadata.
        return {"status": DRY_RUN, "graph": graph, "quality": quality,
                "tables": list(WRITE_ORDER), "reusable": False}

    published = []
    current_table = None
    try:
        for table in WRITE_ORDER:
            current_table = table
            if global_fence is not None:
                assert_global_fence(client, bucket, global_fence, scope)
            assert_graph_fence(client, bucket, graph, fence, scope)
            create_target_table(spark, table, scope)
            merge_table(spark, table, frames[table], scope)
            snapshot_id = current_snapshot_id(spark, table, scope)
            table_quality = {**quality[table], "readback": read_table_readback(spark, table, snapshot_id, scope)}
            manifest = build_table_manifest(
                graph,
                table=table,
                snapshot_id=snapshot_id,
                quality=table_quality,
                namespace=_namespace(scope),
                ref=effective_ref,
                captured_at=captured_at,
            )
            state = publish_manifest(client, bucket, manifest, table_manifest_key(graph["identity"], table, scope))
            published.append({"table": table, "state": state, "manifest": manifest})
    except Exception as exc:
        raise DimensionalWriteError(
            "Gold dimensional graph failed at table "
            f"{current_table!r} ({type(exc).__name__}: {exc}); "
            f"published tables: {[item['table'] for item in published]}"
        ) from exc
    graph_run = build_graph_run_manifest(graph, [item["manifest"] for item in published])
    if global_fence is not None:
        assert_global_fence(client, bucket, global_fence, scope)
    assert_graph_fence(client, bucket, graph, fence, scope)
    graph_state = publish_manifest(client, bucket, graph_run, graph_manifest_key(graph["identity"], scope))
    if global_fence is not None:
        release_global_fence(client, bucket, global_fence, scope)
    return {"status": graph_state, "graph": graph, "graph_run": graph_run, "quality": quality, "tables": published}
