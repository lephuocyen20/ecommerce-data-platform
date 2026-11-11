"""Unit tests for the fail-closed Silver Parquet manifest contract."""

import json
from types import SimpleNamespace

import pytest

from lakehouse.migration import manifest as subject


TABLE = "customers"
BATCH = "a" * 32
PREFIX = f"silver_incremental/{TABLE}/batch_id={BATCH}/runs/attempt-1/"
KEY = f"silver_incremental/{TABLE}/batch_id={BATCH}/_BATCH_METADATA.json"


def valid_manifest():
    return {
        "status": "COMPLETED",
        "table_name": TABLE,
        "batch_id": BATCH,
        "data_prefix": PREFIX,
        "silver_row_count": 1,
        "parquet_objects": [{"object_key": f"{PREFIX}part-00000.parquet", "size": 1, "etag": "etag"}],
    }


@pytest.mark.parametrize("value", [None, [], "manifest"])
def test_validate_rejects_non_mapping_manifest_with_contract_error(value):
    with pytest.raises(ValueError, match="Invalid customers manifest"):
        subject._validate(TABLE, KEY, value)


def test_validate_accepts_complete_run_scoped_manifest():
    assert subject._validate(TABLE, KEY, valid_manifest()) == (BATCH, PREFIX)


def test_validate_rejects_malformed_parquet_object_metadata():
    manifest = valid_manifest()
    manifest["parquet_objects"] = [{"object_key": f"{PREFIX}part-00000.parquet", "size": 0, "etag": "etag"}]
    with pytest.raises(ValueError, match="customers manifest objects invalid"):
        subject._validate(TABLE, KEY, manifest)


class Response:
    def __init__(self, payload):
        self.payload = payload
        self.closed = False
        self.released = False

    def read(self):
        return self.payload

    def close(self):
        self.closed = True

    def release_conn(self):
        self.released = True


class Client:
    def __init__(self, response):
        self.response = response

    def get_object(self, bucket, key):
        assert (bucket, key) == ("bronze", KEY)
        return self.response


@pytest.mark.parametrize("payload", [b"not-json", json.dumps(None).encode()])
def test_read_manifest_releases_response_when_decoding_or_validation_fails(payload):
    response = Response(payload)
    with pytest.raises((json.JSONDecodeError, ValueError)):
        subject.read_manifest(Client(response), "bronze", KEY, TABLE)
    assert response.closed is True
    assert response.released is True


class _MigrationResult:
    def __init__(self, *, rows=None, first=None):
        self._rows = rows or []
        self._first = first

    def collect(self):
        return self._rows

    def first(self):
        return self._first


class _MigrationSpark:
    """Small stateful Spark double for the merge/progress crash window."""

    def __init__(self):
        self.progress_identity = None
        self.fail_progress_write = False
        self.concurrent_identity = None
        self.merges = 0
        self.read = SimpleNamespace(parquet=lambda _path: SimpleNamespace(count=lambda: 1))

    def sql(self, statement):
        if statement.startswith("SELECT manifest_identity, snapshot_id"):
            rows = ([] if self.progress_identity is None else [
                SimpleNamespace(manifest_identity=self.progress_identity, snapshot_id=99)
            ])
            return _MigrationResult(rows=rows)
        if ".snapshots ORDER BY" in statement:
            return _MigrationResult(first=SimpleNamespace(snapshot_id=99))
        if statement.startswith("MERGE INTO lakehouse.metadata"):
            if self.fail_progress_write:
                raise RuntimeError("injected progress write failure")
            if self.concurrent_identity is not None:
                self.progress_identity = self.concurrent_identity
            elif self.progress_identity is None:
                # The manifest digest is passed as the second quoted SELECT literal.
                self.progress_identity = statement.split(" manifest_identity")[0].split("'")[-2]
            return _MigrationResult()
        if statement.startswith("SELECT manifest_identity FROM"):
            row = None if self.progress_identity is None else SimpleNamespace(manifest_identity=self.progress_identity)
            return _MigrationResult(first=row)
        return _MigrationResult()

    def table(self, _name):
        return SimpleNamespace(count=lambda: 1)


def _migration_subject(monkeypatch, spark):
    manifest = valid_manifest()
    monkeypatch.setattr(subject, "discover_parquet_objects", lambda *_args, **_kwargs: manifest["parquet_objects"])
    monkeypatch.setattr(subject, "prepare", lambda source, spec, batch: source)

    def merged(*_args, **_kwargs):
        spark.merges += 1

    monkeypatch.setattr(subject, "merge", merged)
    return manifest


def test_retry_after_merge_before_progress_write_is_deterministic(monkeypatch):
    spark = _MigrationSpark()
    manifest = _migration_subject(monkeypatch, spark)
    spark.fail_progress_write = True

    with pytest.raises(RuntimeError, match="injected progress write failure"):
        subject.migrate_manifest(spark, object(), "bronze", KEY, TABLE, manifest)
    assert spark.merges == 1
    assert spark.progress_identity is None

    spark.fail_progress_write = False
    result = subject.migrate_manifest(spark, object(), "bronze", KEY, TABLE, manifest)
    assert result["status"] == "MIGRATED"
    assert spark.merges == 2

    replay = subject.migrate_manifest(spark, object(), "bronze", KEY, TABLE, manifest)
    assert replay == {"status": "ALREADY_MIGRATED", "batch_id": BATCH, "snapshot_id": 99}
    assert spark.merges == 2


def test_existing_or_racing_conflicting_progress_identity_fails_closed(monkeypatch):
    spark = _MigrationSpark()
    manifest = _migration_subject(monkeypatch, spark)
    spark.progress_identity = "other"

    with pytest.raises(RuntimeError, match="Conflicting customers manifest"):
        subject.migrate_manifest(spark, object(), "bronze", KEY, TABLE, manifest)
    assert spark.merges == 0

    spark.progress_identity = None
    spark.concurrent_identity = "other"
    with pytest.raises(RuntimeError, match="Conflicting customers migration progress"):
        subject.migrate_manifest(spark, object(), "bronze", KEY, TABLE, manifest)
    assert spark.merges == 1
