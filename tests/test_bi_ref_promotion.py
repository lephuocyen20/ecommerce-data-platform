import json
from pathlib import Path

import pytest

from gold.implementation.bi_ref_promotion import (
    EXPECTED_CANDIDATE_REF,
    EXPECTED_SCHEMA,
    EXPECTED_TABLES,
    NessieReference,
    NessieReferenceNotFound,
    PromotionError,
    NessieRestAdapter,
    load_candidate_manifest,
    main,
    promote_candidate,
    validate_candidate_manifest,
)


HASH = "a" * 64
CURRENT_HASH = "b" * 64


def manifest(**overrides):
    value = {
        "status": "VERIFIED",
        "can_write": False,
        "nessie_ref": EXPECTED_CANDIDATE_REF,
        "nessie_hash": HASH,
        "schema": EXPECTED_SCHEMA,
        "tables": [
            {"table": table, "status": "VERIFIED", "snapshot_id": index + 1}
            for index, table in enumerate(EXPECTED_TABLES)
        ],
    }
    value.update(overrides)
    return value


class FakeAdapter:
    def __init__(self, current_hash=CURRENT_HASH, ref_type="TAG"):
        self.current_hash = current_hash
        self.ref_type = ref_type
        self.get_calls = []
        self.assign_calls = []
        self.create_calls = []

    def get_ref(self, ref):
        self.get_calls.append(ref)
        return NessieReference(ref, self.current_hash, self.ref_type)

    def create_tag(self, ref, target_hash):
        self.create_calls.append((ref, target_hash))
        return NessieReference(ref, target_hash, "TAG")


class MissingTargetAdapter(FakeAdapter):
    def get_ref(self, ref):
        self.get_calls.append(ref)
        raise NessieReferenceNotFound("missing")


def test_manifest_requires_exact_verified_run_schema_and_table_set():
    candidate = load_candidate_manifest_from_value(manifest())
    assert candidate.candidate_ref == EXPECTED_CANDIDATE_REF
    assert candidate.schema == EXPECTED_SCHEMA
    assert candidate.tables == tuple(sorted(EXPECTED_TABLES))

    for field, value in (("status", "PASS"), ("can_write", True), ("schema", "gold")):
        invalid = manifest(**{field: value})
        with pytest.raises(PromotionError):
            load_candidate_manifest_from_value(invalid)


def test_manifest_rejects_missing_or_arbitrary_table():
    invalid = manifest(tables=manifest()["tables"][:-1])
    with pytest.raises(PromotionError, match="exactly seven"):
        load_candidate_manifest_from_value(invalid)
    invalid = manifest(tables=[*manifest()["tables"][:-1], {"table": "other", "status": "VERIFIED", "snapshot_id": 9}])
    with pytest.raises(PromotionError):
        load_candidate_manifest_from_value(invalid)


def test_dry_run_is_default_and_never_assigns():
    candidate = load_candidate_manifest_from_value(manifest())
    adapter = FakeAdapter(current_hash=HASH)
    result = promote_candidate(candidate, adapter=adapter, expected_current_hash=HASH)
    assert result["mode"] == "DRY_RUN"
    assert adapter.get_calls == ["bi_gold_current"]
    assert adapter.assign_calls == []
    assert adapter.create_calls == []


def test_apply_requires_expected_hash_and_enforces_cas_and_tag_immutability():
    candidate = load_candidate_manifest_from_value(manifest())
    adapter = FakeAdapter()
    with pytest.raises(PromotionError, match="expected_current_hash"):
        promote_candidate(candidate, adapter=adapter, apply=True)
    with pytest.raises(PromotionError, match="does not match"):
        promote_candidate(candidate, adapter=FakeAdapter("c" * 64), expected_current_hash=CURRENT_HASH, apply=True)
    assert adapter.create_calls == []

    matching_adapter = FakeAdapter(current_hash=HASH)
    result = promote_candidate(candidate, adapter=matching_adapter, expected_current_hash=HASH, apply=True)
    assert result == {"mode": "APPLY_NOOP", "target_ref": "bi_gold_current", "assigned_hash": HASH, "schema": EXPECTED_SCHEMA}
    assert adapter.create_calls == []

    with pytest.raises(PromotionError, match="immutable TAG"):
        promote_candidate(candidate, adapter=FakeAdapter(ref_type="BRANCH"), expected_current_hash=CURRENT_HASH)


def test_missing_target_dry_run_and_guarded_tag_creation():
    candidate = load_candidate_manifest_from_value(manifest())
    adapter = MissingTargetAdapter()
    result = promote_candidate(candidate, adapter=adapter)
    assert result["current_state"] == "ABSENT"
    assert result["planned_operation"] == "CREATE_TAG"
    assert adapter.create_calls == []

    with pytest.raises(PromotionError, match="ABSENT"):
        promote_candidate(candidate, adapter=adapter, expected_current_hash=CURRENT_HASH, apply=True)
    result = promote_candidate(candidate, adapter=adapter, expected_current_hash="ABSENT", apply=True)
    assert result["mode"] == "APPLY_CREATE_TAG"
    assert adapter.create_calls == [("bi_gold_current", HASH)]


def test_existing_tag_must_match_candidate_hash():
    candidate = load_candidate_manifest_from_value(manifest())
    with pytest.raises(PromotionError, match="differs"):
        promote_candidate(candidate, adapter=FakeAdapter(current_hash="c" * 64), expected_current_hash="c" * 64)


def test_nessie_v2_create_shape_is_immutable_tag(monkeypatch):
    adapter = NessieRestAdapter("http://host.docker.internal:19132/api/v2")
    calls = []

    def request(method, path, body=None):
        calls.append((method, path, body))
        return {"reference": {"name": "bi_gold_current", "hash": HASH, "type": "TAG"}}

    monkeypatch.setattr(adapter, "_request", request)
    created = adapter.create_tag("bi_gold_current", HASH)
    assert created == NessieReference("bi_gold_current", HASH, "TAG")
    assert calls == [(
        "POST",
        "/trees?name=bi_gold_current&type=TAG",
        {"type": "DETACHED", "hash": HASH},
    )]


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({}, "missing required reference object"),
        ({"reference": {"name": "bi_gold_current", "hash": HASH}}, "missing required field: type"),
        ({"reference": {"name": "other", "hash": HASH, "type": "TAG"}}, "name does not match"),
        ({"reference": {"name": "bi_gold_current", "hash": HASH, "type": "BRANCH"}}, "type must be exactly TAG"),
        ({"reference": {"name": "bi_gold_current", "type": "TAG"}}, "missing required field: hash"),
    ],
)
def test_nessie_create_response_is_fail_closed(monkeypatch, payload, message):
    adapter = NessieRestAdapter("http://host.docker.internal:19132/api/v2")
    monkeypatch.setattr(adapter, "_request", lambda method, path, body=None: payload)
    with pytest.raises(PromotionError, match=message):
        adapter.create_tag("bi_gold_current", HASH)


def test_cli_defaults_to_dry_run(tmp_path, capsys):
    path = tmp_path / "verified.json"
    path.write_text(json.dumps(manifest()), encoding="utf-8")
    assert main(["--manifest", str(path), "--candidate-hash", HASH]) == 0
    assert '"mode": "DRY_RUN"' in capsys.readouterr().out


def test_cli_apply_requires_explicit_environment_guard(tmp_path, monkeypatch, capsys):
    path = tmp_path / "verified.json"
    path.write_text(json.dumps(manifest()), encoding="utf-8")
    monkeypatch.delenv("BI_REF_PROMOTION_APPLY", raising=False)
    assert main([
        "--manifest", str(path),
        "--candidate-hash", HASH,
        "--nessie-uri", "http://host.docker.internal:19132/api/v2",
        "--expected-current-hash", CURRENT_HASH,
        "--apply",
    ]) == 2
    assert "BI_REF_PROMOTION_APPLY=1" in capsys.readouterr().err


def test_cli_rejects_non_local_nessie_uri_before_rest_request(tmp_path, capsys):
    path = tmp_path / "verified.json"
    path.write_text(json.dumps(manifest()), encoding="utf-8")
    assert main([
        "--manifest", str(path),
        "--candidate-hash", HASH,
        "--nessie-uri", "https://nessie.production.example/api/v2",
    ]) == 2
    assert "approved local" in capsys.readouterr().err
    with pytest.raises(PromotionError):
        NessieRestAdapter("http://nessie:19120/api/v2")


def test_source_has_no_data_engine_or_object_write_clients():
    source = Path("gold/implementation/bi_ref_promotion.py").read_text(encoding="utf-8")
    lowered = source.lower()
    for forbidden in ("from pyspark", "import pyspark", "import minio", "import boto3", "s3.upload", "put_object", "merge into"):
        assert forbidden not in lowered


def test_powershell_wrapper_is_local_and_apply_guarded():
    source = Path("scripts/promote_bi_gold_current.ps1").read_text(encoding="utf-8")
    assert "TRINO_NESSIE_URI" in source
    assert "http://host.docker.internal:19132/api/v2" in source
    assert "BI_REF_PROMOTION_APPLY" in source
    assert "if ($Apply)" in source
    assert "--apply" in source
    assert "ABSENT" in source
    assert "quality_it_run_20261112_gold_06" in source
    assert "gold_run_20261112_gold_06" in source


def load_candidate_manifest_from_value(value):
    return validate_candidate_manifest(value, candidate_hash=HASH)
