"""Native Iceberg v2 writer for the first dimensional Gold graph.

The module keeps all writes behind an explicit caller-supplied WRITE mode and
feature flag.  Source reads and transformations are always pinned and pure up
to the final MERGE boundary.
"""

import os

from .contracts import DIMENSIONAL_CATALOG, DIMENSIONAL_NAMESPACE, TABLE_COLUMNS, TABLE_DDLS, TABLE_MERGE_KEYS, TABLE_SCHEMA, WRITE_ORDER
from .manifest import (build_graph_manifest, build_graph_run_manifest, build_table_manifest, graph_manifest_key,
                       acquire_global_fence, acquire_graph_fence, assert_global_fence, assert_graph_fence,
                       publish_manifest, read_completed_graph, release_global_fence,
                       read_table_manifests, table_manifest_key)
from .quality import validate_table_frame
from .reader import read_dimensional_sources
from .transformations import build_dim_customer, build_dim_date, build_dim_product, build_fact_orders

DRY_RUN = "DRY_RUN"
WRITE = "WRITE"


class DimensionalWriteError(ValueError):
    pass


def _identifier(table):
    if table not in WRITE_ORDER:
        raise DimensionalWriteError(f"Target table is not allowlisted: {table}")
    return f"{DIMENSIONAL_CATALOG}.{DIMENSIONAL_NAMESPACE}.{table}"


def assert_write_allowed(mode):
    if mode not in {DRY_RUN, WRITE}:
        raise DimensionalWriteError("mode must be DRY_RUN or WRITE")
    if mode == WRITE and os.getenv("GOLD_DIMENSIONAL_WRITE_ENABLED", "false").lower() != "true":
        raise DimensionalWriteError("WRITE requires GOLD_DIMENSIONAL_WRITE_ENABLED=true")


def create_target_table(spark, table):
    identifier = _identifier(table)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {DIMENSIONAL_CATALOG}.{DIMENSIONAL_NAMESPACE}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {identifier} ({TABLE_DDLS[table]}) USING iceberg "
        "TBLPROPERTIES ('format-version'='2')"
    )
    validate_target_table(spark, table)


def validate_target_table(spark, table):
    """Reject pre-existing Gold tables that drift from the frozen v1 contract."""
    identifier = _identifier(table)
    schema = tuple((field.name, field.dataType.simpleString(), field.nullable) for field in spark.table(identifier).schema.fields)
    if schema != TABLE_SCHEMA[table]:
        raise DimensionalWriteError(f"Existing Iceberg target schema drift for {table}")
    rows = spark.sql(f"SHOW TBLPROPERTIES {identifier}").collect()
    properties = {row[0]: row[1] for row in rows}
    if properties.get("format-version") != "2":
        raise DimensionalWriteError(f"Existing Iceberg target format-version mismatch for {table}")
    return True


def merge_table(spark, table, dataframe):
    """Idempotently upsert one validated target using its declared grain."""
    identifier = _identifier(table)
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


def current_snapshot_id(spark, table):
    identifier = _identifier(table)
    rows = spark.sql(f"SELECT snapshot_id FROM {identifier}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    if len(rows) != 1 or not isinstance(rows[0].snapshot_id, int) or rows[0].snapshot_id <= 0:
        raise DimensionalWriteError(f"No current Iceberg snapshot for {table}")
    return rows[0].snapshot_id


def build_graph_frames(spark, sources, *, dim_date_start, dim_date_end):
    frames = {
        "dim_date": build_dim_date(spark, dim_date_start, dim_date_end),
        "dim_customer": build_dim_customer(sources["customers"]),
        "dim_product": build_dim_product(sources["products"]),
    }
    frames["fact_orders"] = build_fact_orders(sources["orders"], sources["order_items"], frames["dim_customer"])
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


def run_dimensional_graph(spark, client, bucket, lineage, *, dim_date_start, dim_date_end, mode=DRY_RUN):
    """Validate and optionally publish the whole Gold graph in dependency order.

    A failed write intentionally leaves the graph manifest unpublished.  The
    returned partial table list is evidence for an operator-led reconciliation
    before any retry; this avoids treating a partial run as a successful graph.
    """
    assert_write_allowed(mode)
    graph = build_graph_manifest(lineage, dim_date_start=dim_date_start, dim_date_end=dim_date_end)
    completed = read_completed_graph(client, bucket, graph)
    if completed is not None:
        return {"status": "ALREADY_PUBLISHED", "graph": graph, "graph_run": completed, "tables": []}
    table_manifests = read_table_manifests(client, bucket, graph)
    state = recovery_state(False, {table: "PUBLISHED" for table in table_manifests})
    if state == "PARTIAL_REQUIRES_RECONCILIATION":
        return {"status": state, "graph": graph, "tables": sorted(table_manifests)}
    # A persistent immutable fence serializes writers.  It is acquired before
    # source reads so no Spark MERGE can begin under an ambiguous ownership.
    global_fence = acquire_global_fence(client, bucket, graph) if mode == WRITE else None
    try:
        fence = acquire_graph_fence(client, bucket, graph) if mode == WRITE else None
    except Exception:
        # No target write occurred when the per-graph fence cannot be obtained.
        # Releasing the just-created global coordination object is safe here.
        release_global_fence(client, bucket, global_fence)
        raise
    if mode == WRITE:
        # Another writer can complete after the first preflight; recheck under
        # the fence before any read or write.
        completed = read_completed_graph(client, bucket, graph)
        table_manifests = read_table_manifests(client, bucket, graph)
        if completed is not None:
            release_global_fence(client, bucket, global_fence)
            return {"status": "ALREADY_PUBLISHED", "graph": graph, "graph_run": completed, "tables": []}
        if table_manifests:
            release_global_fence(client, bucket, global_fence)
            return {"status": "PARTIAL_REQUIRES_RECONCILIATION", "graph": graph, "tables": sorted(table_manifests)}
        assert_global_fence(client, bucket, global_fence)
        assert_graph_fence(client, bucket, graph, fence)
    sources = read_dimensional_sources(spark, client, bucket, lineage)
    frames, quality = build_graph_frames(spark, sources, dim_date_start=dim_date_start, dim_date_end=dim_date_end)
    if mode == DRY_RUN:
        return {"status": DRY_RUN, "graph": graph, "quality": quality, "tables": list(WRITE_ORDER)}

    published = []
    try:
        for table in WRITE_ORDER:
            assert_global_fence(client, bucket, global_fence)
            assert_graph_fence(client, bucket, graph, fence)
            create_target_table(spark, table)
            merge_table(spark, table, frames[table])
            manifest = build_table_manifest(graph, table=table, snapshot_id=current_snapshot_id(spark, table), quality=quality[table])
            state = publish_manifest(client, bucket, manifest, table_manifest_key(graph["identity"], table))
            published.append({"table": table, "state": state, "manifest": manifest})
    except Exception as exc:
        raise DimensionalWriteError(f"Gold dimensional graph partially published: {[item['table'] for item in published]}") from exc
    graph_run = build_graph_run_manifest(graph, [item["manifest"] for item in published])
    assert_global_fence(client, bucket, global_fence)
    assert_graph_fence(client, bucket, graph, fence)
    graph_state = publish_manifest(client, bucket, graph_run, graph_manifest_key(graph["identity"]))
    release_global_fence(client, bucket, global_fence)
    return {"status": graph_state, "graph": graph, "graph_run": graph_run, "quality": quality, "tables": published}
