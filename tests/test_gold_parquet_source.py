import hashlib
import json

import pytest

from gold.implementation.dimensional import reader
from gold.implementation.dimensional.contracts import DIMENSIONAL_SOURCE_TABLES
from gold.implementation.dimensional.lineage import (
    DimensionalLineageError,
    PARQUET_MANIFEST,
    build_dimensional_lineage_from_artifacts,
    build_parquet_lineage_artifact,
)
from lakehouse.models.spec import SPECS
from tests.run_bounded_gold_parquet_fixture import APPROVED_RUN_IDS


def test_fresh_recovery_run_is_allowlisted_without_changing_run04():
    assert "run-20261112-gold-05" in APPROVED_RUN_IDS
    assert "run-20261111-gold-04" in APPROVED_RUN_IDS


def test_second_fresh_recovery_run_is_allowlisted_without_changing_run05():
    assert "run-20261112-gold-06" in APPROVED_RUN_IDS
    assert "run-20261112-gold-05" in APPROVED_RUN_IDS


def _manifest(table, batch, prefix):
    return {
        "status": "COMPLETED",
        "table_name": table,
        "batch_id": batch,
        "data_prefix": prefix,
        "silver_row_count": 1,
        "parquet_objects": [{"object_key": prefix + "part-00000.parquet", "size": 10, "etag": "etag"}],
    }


def _lineage_and_manifests():
    artifacts, manifests = {}, {}
    for index, table in enumerate(DIMENSIONAL_SOURCE_TABLES, start=1):
        batch = f"{index:032x}"
        prefix = f"silver_incremental/{table}/batch_id={batch}/runs/run/"
        manifest = _manifest(table, batch, prefix)
        identity = hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        artifacts[table] = build_parquet_lineage_artifact(
            table=table,
            bucket="ecommerce-quality-it",
            manifest_key=f"silver_incremental/{table}/batch_id={batch}/_BATCH_METADATA.json",
            data_prefix=prefix,
            manifest_identity=identity,
            batch_id=batch,
            row_count=1,
        )
        manifests[table] = manifest
    return build_dimensional_lineage_from_artifacts(artifacts), manifests


def test_gold_lineage_uses_verified_parquet_manifests():
    lineage, _ = _lineage_and_manifests()
    assert lineage["source_type"] == PARQUET_MANIFEST
    assert all("snapshot_id" not in source for source in lineage["sources"].values())


def test_reader_revalidates_manifest_inventory_before_reading(monkeypatch):
    lineage, manifests = _lineage_and_manifests()

    class Response:
        def __init__(self, value): self.value = value
        def read(self): return json.dumps(self.value).encode()
        def close(self): pass
        def release_conn(self): pass

    class Client:
        def get_object(self, bucket, key):
            table = key.split("/")[1]
            return Response(manifests[table])

    class Frame:
        def __init__(self, columns): self.columns = tuple(columns)
        def select(self, *columns): return Frame(columns)
        def count(self): return 1
        def unionByName(self, other): return self
        def dropDuplicates(self, _columns): return self

    class Spark:
        class Reader:
            def parquet(self, path): return Frame(SPECS[path.split("/")[4]].columns)
        read = Reader()

    monkeypatch.setattr(reader, "discover_parquet_objects", lambda *args, **kwargs: [{"object_key": args[2] + "part-00000.parquet", "size": 10, "etag": "etag"}])
    sources = reader.read_dimensional_sources(Spark(), Client(), "ecommerce-quality-it", lineage)
    assert set(sources) == set(DIMENSIONAL_SOURCE_TABLES)
    assert all(frame.columns == SPECS[table].columns for table, frame in sources.items())


def test_bounded_scope_rejects_historical_manifest_outside_prefix():
    from common.bounded_e2e_scope import build_bounded_e2e_scope

    lineage, _ = _lineage_and_manifests()
    scope = build_bounded_e2e_scope({
        "LAKEHOUSE_E2E_APPROVED": "1",
        "LAKEHOUSE_E2E_TEST_BUCKET": "ecommerce-data-lake",
        "LAKEHOUSE_E2E_RUN_ID": "run-20261111",
        "LAKEHOUSE_E2E_PREFIX": "quality-it/run-20261111/",
    })
    for source in lineage["sources"].values():
        source["bucket"] = scope.bucket
        source["manifest_keys"] = [scope.prefix + key for key in source["manifest_keys"]]
        source["manifest_key"] = source["manifest_keys"][0]
        source["data_prefixes"] = [scope.prefix + prefix for prefix in source["data_prefixes"]]
        source["data_prefix"] = source["data_prefixes"][0]
    lineage["sources"]["customers"]["manifest_keys"].append("silver_incremental/customers/old/_BATCH_METADATA.json")
    lineage["sources"]["customers"]["manifest_identities"].append("a" * 64)
    lineage["sources"]["customers"]["data_prefixes"].append("silver_incremental/customers/old/runs/")
    with pytest.raises(DimensionalLineageError, match="outside the requested E2E scope"):
        from gold.implementation.dimensional.lineage import validate_dimensional_lineage
        validate_dimensional_lineage(lineage, scope=scope)
