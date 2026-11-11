"""Verified Silver source reads for the dimensional graph."""

import hashlib
import json

from common.manifest import read_json_object
from common.object_integrity import discover_parquet_objects
from lakehouse.attestation import read_snapshot_attestation
from lakehouse.evidence import verify_evidence
from lakehouse.models.spec import SPECS
from .contracts import DIMENSIONAL_SOURCE_TABLES
from .lineage import DimensionalLineageError, PARQUET_MANIFEST, validate_dimensional_lineage


def _read_evidence(client, bucket, identity, scope=None):
    key = f"lakehouse/evidence/v2/{identity}.json"
    if scope is not None:
        key = scope.key(key)
    document = read_json_object(client, bucket, key)
    if document.get("identity") != identity:
        raise DimensionalLineageError("Evidence identity mismatch")
    return document


def _read_parquet_sources(spark, client, bucket, lineage, scope=None):
    """Read only completion-manifest verified Silver Parquet data."""
    result = {}
    for table in DIMENSIONAL_SOURCE_TABLES:
        source = lineage["sources"][table]
        if source["bucket"] != bucket:
            raise DimensionalLineageError(f"Parquet bucket mismatch for {table}")
        frames = []
        for index, manifest_key in enumerate(source["manifest_keys"]):
            manifest = read_json_object(client, bucket, manifest_key)
            identity = hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            prefix = source["data_prefixes"][index]
            if identity != source["manifest_identities"][index]:
                raise DimensionalLineageError(f"Parquet manifest identity mismatch for {table}")
            if manifest.get("status") != "COMPLETED" or manifest.get("table_name") != table or manifest.get("data_prefix") != prefix:
                raise DimensionalLineageError(f"Parquet manifest metadata mismatch for {table}")
            expected = sorted(manifest.get("parquet_objects", []), key=lambda item: item.get("object_key", ""))
            actual = discover_parquet_objects(client, bucket, prefix, include_etag=True)
            if actual != expected:
                raise DimensionalLineageError(f"Silver Parquet inventory mismatch for {table}")
            try:
                frames.append(spark.read.parquet(f"s3a://{bucket}/{prefix}").select(*SPECS[table].columns))
            except Exception as exc:
                raise DimensionalLineageError(f"Silver Parquet source unavailable: {table}") from exc
        try:
            dataframe = frames[0]
            for frame in frames[1:]:
                dataframe = dataframe.unionByName(frame)
            if not hasattr(spark, "sparkContext"):
                dataframe = dataframe.dropDuplicates([SPECS[table].key])
            else:
                from pyspark.sql import Window, functions as F
                key = SPECS[table].key
                watermark = SPECS[table].watermark or SPECS[table].columns[-1]
                window = Window.partitionBy(key).orderBy(F.col(watermark).desc(), F.col(key).desc())
                dataframe = dataframe.withColumn("_silver_rank", F.row_number().over(window)).where(F.col("_silver_rank") == 1).drop("_silver_rank")
        except ImportError:
            dataframe = frames[0]
            for frame in frames[1:]:
                dataframe = dataframe.unionByName(frame)
            dataframe = dataframe.dropDuplicates([SPECS[table].key])
        except Exception as exc:
            raise DimensionalLineageError(f"Silver Parquet state assembly failed: {table}") from exc
        if tuple(dataframe.columns) != SPECS[table].columns:
            raise DimensionalLineageError(f"Silver schema drift for {table}")
        result[table] = dataframe
    return result


def read_dimensional_sources(spark, client, bucket, lineage, scope=None):
    """Verify Silver lineage before reading immutable source data."""
    validate_dimensional_lineage(lineage, scope=scope)
    if lineage["source_type"] == PARQUET_MANIFEST:
        return _read_parquet_sources(spark, client, bucket, lineage, scope=scope)
    result = {}
    source_catalog = "lakehouse"
    source_namespace = scope.silver_namespace if scope is not None else "silver"
    for table in DIMENSIONAL_SOURCE_TABLES:
        source = lineage["sources"][table]
        snapshot_id = source["snapshot_id"]
        source_ref = scope.nessie_ref if scope is not None else source.get("ref", "main")
        read_snapshot_attestation(client, bucket, source["snapshot_attestation_identity"], table=table,
                                  snapshot_id=snapshot_id, catalog=source_catalog, namespace=source_namespace, ref=source_ref,
                                  scope=scope)
        for kind, field in (("reconciliation", "reconciliation_evidence"), ("quality", "quality_evidence")):
            expected = source[field]
            document = (_read_evidence(client, bucket, expected["identity"], scope)
                        if scope is not None else _read_evidence(client, bucket, expected["identity"]))
            verify_evidence(document, kind=kind, table=table, snapshot_id=snapshot_id,
                            catalog=source_catalog, namespace=source_namespace, ref=source_ref,
                            snapshot_attestation_identity=source["snapshot_attestation_identity"])
            if {key: document[key] for key in expected} != expected:
                raise DimensionalLineageError(f"{kind} evidence mismatch for {table}")
        try:
            # Silver Iceberg tables intentionally carry ingestion metadata
            # columns (_batch_id/_ingested_at/_record_hash and audit fields).
            # Gold consumes the frozen business contract only; selecting it
            # explicitly prevents operational metadata from being mistaken for
            # schema drift.
            columns = ", ".join(f"`{column}`" for column in SPECS[table].columns)
            dataframe = spark.sql(
                f"SELECT {columns} FROM {source_catalog}.{source_namespace}.{table} VERSION AS OF {snapshot_id}"
            )
        except Exception as exc:
            raise DimensionalLineageError(f"Pinned Silver snapshot unavailable: {table}@{snapshot_id}") from exc
        if tuple(dataframe.columns) != SPECS[table].columns:
            raise DimensionalLineageError(f"Silver schema drift for {table}")
        result[table] = dataframe
    return result
