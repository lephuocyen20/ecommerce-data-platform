import copy
import json

import pytest

from gold.implementation.dimensional.contracts import DIMENSIONAL_SOURCE_TABLES
from gold.implementation.dimensional.lineage import (
    DimensionalLineageError,
    build_dimensional_lineage_from_artifacts,
    build_parquet_lineage_artifact,
    dimensional_lineage_identity,
    validate_dimensional_lineage,
)
from common.bounded_e2e_scope import build_bounded_e2e_scope


def dimensional_lineage():
    sources = {}
    for offset, table in enumerate(DIMENSIONAL_SOURCE_TABLES, start=1):
        attestation = f"{offset:064x}"
        sources[table] = {
            "source_type": "iceberg_snapshot", "table": table, "catalog": "lakehouse",
            "namespace": "silver", "ref": "main", "snapshot_id": offset,
            "snapshot_attestation_identity": attestation,
            "reconciliation_evidence": {"identity": f"{offset + 10:064x}", "status": "PASS", "snapshot_id": offset, "table": table},
            "quality_evidence": {"identity": f"{offset + 20:064x}", "status": "PASS", "snapshot_id": offset, "table": table},
        }
    return {"version": 1, "source_type": "iceberg_snapshot", "sources": sources}


def test_lineage_accepts_matching_bounded_catalog_scope():
    lineage = dimensional_lineage()
    scope = build_bounded_e2e_scope({
        "LAKEHOUSE_E2E_APPROVED": "1",
        "LAKEHOUSE_E2E_TEST_BUCKET": "ecommerce-data-lake",
        "LAKEHOUSE_E2E_RUN_ID": "run-20261110",
        "LAKEHOUSE_E2E_PREFIX": "quality-it/run-20261110/",
    })
    for source in lineage["sources"].values():
        source.update(namespace=scope.silver_namespace, ref=scope.nessie_ref)
    assert validate_dimensional_lineage(lineage, scope=scope)


def test_dimensional_lineage_requires_complete_pinned_silver_graph():
    lineage = dimensional_lineage()
    assert validate_dimensional_lineage(lineage) is True
    assert dimensional_lineage_identity(lineage) == dimensional_lineage_identity(copy.deepcopy(lineage))


def test_lineage_builder_requires_artifacts_not_manual_environment():
    from gold.implementation.dimensional.lineage import build_dimensional_lineage_from_artifacts
    with pytest.raises(DimensionalLineageError, match="six Silver artifact"):
        build_dimensional_lineage_from_artifacts({})


def test_artifact_normalizer_accepts_quality_list_and_pins_evidence():
    from gold.implementation.dimensional.lineage import normalize_dimensional_artifact
    artifact = normalize_dimensional_artifact(
        "customers",
        {"status": "PUBLISHED", "key": "lakehouse/attestations/v1/att-7.json", "attestation": {"status": "PUBLISHED", "table": "customers", "snapshot_id": 7, "identity": "att-7"}},
        {"status": "PASS", "table": "customers", "snapshot_id": 7, "identity": "rec-7"},
        [{"status": "PASS", "table": "customers", "snapshot_id": 7, "identity": "qual-7"}],
    )
    assert artifact["iceberg_lineage"]["snapshot_id"] == 7
    assert artifact["iceberg_lineage"]["quality_evidence"]["identity"] == "qual-7"


def test_artifact_normalizer_rejects_mismatched_snapshot():
    from gold.implementation.dimensional.lineage import normalize_dimensional_artifact
    with pytest.raises(DimensionalLineageError, match="snapshot mismatch"):
        normalize_dimensional_artifact(
            "customers",
            {"status": "PUBLISHED", "key": "lakehouse/attestations/v1/att-7.json", "attestation": {"status": "PUBLISHED", "table": "customers", "snapshot_id": 7, "identity": "att-7"}},
            {"status": "PASS", "table": "customers", "snapshot_id": 8, "identity": "rec-8"},
            [{"status": "PASS", "table": "customers", "snapshot_id": 7, "identity": "qual-7"}],
        )


def test_artifact_normalizer_accepts_actual_cli_nested_and_retry_shapes():
    from gold.implementation.dimensional.lineage import normalize_dimensional_artifact
    artifact = normalize_dimensional_artifact(
        "customers",
        {"status": "ALREADY_PUBLISHED", "key": "lakehouse/attestations/v1/att-9.json", "attestation": {"status": "PUBLISHED", "table": "customers", "snapshot_id": 9, "identity": "att-9"}},
        {"status": "PASS", "table_name": "customers", "snapshot_id": 9, "metrics": {"parity": "PASS"}, "reconciliation_evidence_identity": "rec-9"},
        {"status": "PASS", "table": "lakehouse.silver.customers", "snapshot_id": 9, "quality_evidence_identity": "qual-9"},
    )
    assert artifact["iceberg_lineage"]["snapshot_attestation_identity"] == "att-9"
    assert artifact["iceberg_lineage"]["reconciliation_evidence"]["identity"] == "rec-9"


def test_artifact_normalizer_accepts_actual_publish_attestation_shape():
    from gold.implementation.dimensional.lineage import normalize_dimensional_artifact
    from lakehouse.attestation import build_snapshot_attestation

    attestation = build_snapshot_attestation(
        table="customers", snapshot_id=9, manifest_identities=["a" * 64]
    )
    publication = {
        "status": "PUBLISHED",
        "key": "lakehouse/attestations/v1/" + attestation["identity"] + ".json",
        "identity": attestation["identity"],
        "attestation": attestation,
    }
    artifact = normalize_dimensional_artifact(
        "customers",
        publication,
        {"status": "PASS", "table_name": "customers", "snapshot_id": 9, "reconciliation_evidence_identity": "rec-9"},
        {"status": "PASS", "table": "lakehouse.silver.customers", "snapshot_id": 9, "quality_evidence_identity": "qual-9"},
    )

    assert artifact["iceberg_lineage"]["snapshot_attestation_identity"] == attestation["identity"]


def test_artifact_normalizer_unwraps_airflow_structured_event_envelope():
    from gold.implementation.dimensional.lineage import normalize_dimensional_artifact
    from lakehouse.attestation import build_snapshot_attestation

    attestation = build_snapshot_attestation(table="customers", snapshot_id=10, manifest_identities=["b" * 64])
    publication = {
        "status": "PUBLISHED",
        "key": f"lakehouse/attestations/v1/{attestation['identity']}.json",
        "identity": attestation["identity"],
        "attestation": attestation,
    }
    envelope = json.dumps({
        "timestamp": "2026-01-01T00:00:00Z", "level": "info",
        "event": json.dumps(publication, sort_keys=True), "logger": "airflow.task",
        "filename": "docker.py", "lineno": 71,
    })
    artifact = normalize_dimensional_artifact(
        "customers", envelope,
        {"status": "PASS", "table_name": "customers", "snapshot_id": 10, "reconciliation_evidence_identity": "rec-10"},
        {"status": "PASS", "table": "customers", "snapshot_id": 10, "quality_evidence_identity": "qual-10"},
    )
    assert artifact["iceberg_lineage"]["snapshot_attestation_identity"] == attestation["identity"]


def test_artifact_normalizer_unwraps_json_encoded_airflow_event_envelope():
    from gold.implementation.dimensional.lineage import normalize_dimensional_artifact
    from lakehouse.attestation import build_snapshot_attestation

    attestation = build_snapshot_attestation(table="customers", snapshot_id=11, manifest_identities=["c" * 64])
    publication = {
        "status": "PUBLISHED",
        "key": f"lakehouse/attestations/v1/{attestation['identity']}.json",
        "identity": attestation["identity"], "attestation": attestation,
    }
    envelope = json.dumps({"timestamp": "2026-01-01T00:00:00Z", "level": "info",
                           "event": json.dumps(publication), "logger": "airflow.task",
                           "filename": "docker.py", "lineno": 71})
    artifact = normalize_dimensional_artifact(
        "customers", json.dumps(envelope),
        {"status": "PASS", "table_name": "customers", "snapshot_id": 11, "reconciliation_evidence_identity": "rec-11"},
        {"status": "PASS", "table": "customers", "snapshot_id": 11, "quality_evidence_identity": "qual-11"},
    )
    assert artifact["iceberg_lineage"]["snapshot_attestation_identity"] == attestation["identity"]


def test_artifact_normalizer_accepts_deserialized_canonical_attestation():
    from gold.implementation.dimensional.lineage import normalize_dimensional_artifact
    from lakehouse.attestation import build_snapshot_attestation

    attestation = build_snapshot_attestation(
        table="customers", snapshot_id=9, manifest_identities=["a" * 64]
    )
    artifact = normalize_dimensional_artifact(
        "customers",
        attestation,
        {"status": "PASS", "table_name": "customers", "snapshot_id": 9, "reconciliation_evidence_identity": "rec-9"},
        {"status": "PASS", "table": "lakehouse.silver.customers", "snapshot_id": 9, "quality_evidence_identity": "qual-9"},
    )
    assert artifact["iceberg_lineage"]["snapshot_attestation_identity"] == attestation["identity"]


def test_artifact_normalizer_rejects_tampered_canonical_attestation():
    from gold.implementation.dimensional.lineage import normalize_dimensional_artifact
    from lakehouse.attestation import build_snapshot_attestation

    attestation = build_snapshot_attestation(
        table="customers", snapshot_id=9, manifest_identities=["a" * 64]
    )
    tampered = {**attestation, "identity": "b" * 64}
    with pytest.raises(DimensionalLineageError, match="Invalid attestation canonical document"):
        normalize_dimensional_artifact(
            "customers",
            tampered,
            {"status": "PASS", "table_name": "customers", "snapshot_id": 9, "reconciliation_evidence_identity": "rec-9"},
            {"status": "PASS", "table": "lakehouse.silver.customers", "snapshot_id": 9, "quality_evidence_identity": "qual-9"},
        )


def test_artifact_normalizer_accepts_flattened_publication_metadata():
    from gold.implementation.dimensional.lineage import normalize_dimensional_artifact
    from lakehouse.attestation import build_snapshot_attestation

    attestation = build_snapshot_attestation(
        table="customers", snapshot_id=9, manifest_identities=["a" * 64]
    )
    flattened = {
        **attestation,
        "status": "PUBLISHED",
        "key": f"lakehouse/attestations/v1/{attestation['identity']}.json",
    }
    artifact = normalize_dimensional_artifact(
        "customers", flattened,
        {"status": "PASS", "table_name": "customers", "snapshot_id": 9, "reconciliation_evidence_identity": "rec-9"},
        {"status": "PASS", "table": "lakehouse.silver.customers", "snapshot_id": 9, "quality_evidence_identity": "qual-9"},
    )
    assert artifact["iceberg_lineage"]["snapshot_attestation_identity"] == attestation["identity"]


def test_artifact_normalizer_accepts_stringified_nested_publication_metadata():
    from gold.implementation.dimensional.lineage import normalize_dimensional_artifact
    from lakehouse.attestation import build_snapshot_attestation
    import json

    attestation = build_snapshot_attestation(
        table="customers", snapshot_id=9, manifest_identities=["a" * 64]
    )
    publication = {
        "status": "PUBLISHED",
        "key": f"lakehouse/attestations/v1/{attestation['identity']}.json",
        "identity": attestation["identity"],
        "attestation": json.dumps(attestation, sort_keys=True),
    }
    artifact = normalize_dimensional_artifact(
        "customers", publication,
        {"status": "PASS", "table_name": "customers", "snapshot_id": 9, "reconciliation_evidence_identity": "rec-9"},
        {"status": "PASS", "table": "lakehouse.silver.customers", "snapshot_id": 9, "quality_evidence_identity": "qual-9"},
    )
    assert artifact["iceberg_lineage"]["snapshot_attestation_identity"] == attestation["identity"]


def test_artifact_normalizer_rejects_tampered_nested_publication_key():
    from gold.implementation.dimensional.lineage import normalize_dimensional_artifact
    from lakehouse.attestation import build_snapshot_attestation

    attestation = build_snapshot_attestation(table="customers", snapshot_id=9, manifest_identities=["a" * 64])
    with pytest.raises(DimensionalLineageError, match="publication key mismatch"):
        normalize_dimensional_artifact(
            "customers",
            {"status": "PUBLISHED", "key": "lakehouse/attestations/v1/" + "f" * 64 + ".json", "attestation": attestation},
            {"status": "PASS", "table_name": "customers", "snapshot_id": 9, "reconciliation_evidence_identity": "rec-9"},
            {"status": "PASS", "table": "lakehouse.silver.customers", "snapshot_id": 9, "quality_evidence_identity": "qual-9"},
        )


def test_artifact_normalizer_unwraps_bounded_airflow_return_value():
    from gold.implementation.dimensional.lineage import normalize_dimensional_artifact
    from lakehouse.attestation import build_snapshot_attestation

    attestation = build_snapshot_attestation(table="customers", snapshot_id=9, manifest_identities=["a" * 64])
    publication = {"return_value": {"status": "PUBLISHED", "key": f"lakehouse/attestations/v1/{attestation['identity']}.json", "attestation": attestation}, "status": "success"}
    artifact = normalize_dimensional_artifact(
        "customers", publication,
        {"status": "PASS", "table_name": "customers", "snapshot_id": 9, "reconciliation_evidence_identity": "rec-9"},
        {"status": "PASS", "table": "lakehouse.silver.customers", "snapshot_id": 9, "quality_evidence_identity": "qual-9"},
    )
    assert artifact["iceberg_lineage"]["snapshot_attestation_identity"] == attestation["identity"]


def test_artifact_normalizer_unwraps_list_transport_wrapper():
    from gold.implementation.dimensional.lineage import normalize_dimensional_artifact
    from lakehouse.attestation import build_snapshot_attestation

    attestation = build_snapshot_attestation(table="customers", snapshot_id=9, manifest_identities=["a" * 64])
    publication = [{"return_value": {"status": "PUBLISHED", "key": f"lakehouse/attestations/v1/{attestation['identity']}.json", "attestation": attestation}, "status": "success"}]
    artifact = normalize_dimensional_artifact(
        "customers", publication,
        {"status": "PASS", "table_name": "customers", "snapshot_id": 9, "reconciliation_evidence_identity": "rec-9"},
        {"status": "PASS", "table": "lakehouse.silver.customers", "snapshot_id": 9, "quality_evidence_identity": "qual-9"},
    )
    assert artifact["iceberg_lineage"]["snapshot_attestation_identity"] == attestation["identity"]


def test_artifact_normalizer_extracts_json_before_spark_shutdown_noise():
    from gold.implementation.dimensional.lineage import normalize_dimensional_artifact

    artifact = normalize_dimensional_artifact(
        "customers",
        'INFO Spark startup\n{"status":"PUBLISHED","key":"lakehouse/attestations/v1/att-9.json","attestation":{"status":"PUBLISHED","table":"customers","snapshot_id":9,"identity":"att-9"}}\nINFO SparkContext: shutdown',
        'INFO reconcile\n{"status":"PASS","table_name":"customers","snapshot_id":9,"reconciliation_evidence_identity":"rec-9"}\nINFO SparkContext: shutdown',
        'INFO quality\n[{"status":"PASS","table":"lakehouse.silver.customers","snapshot_id":9,"quality_evidence_identity":"qual-9"}]\nINFO SparkContext: shutdown',
    )

    assert artifact["iceberg_lineage"]["snapshot_attestation_identity"] == "att-9"
    assert artifact["iceberg_lineage"]["quality_evidence"]["identity"] == "qual-9"


def test_artifact_normalizer_rejects_tableless_json_in_noisy_log():
    from gold.implementation.dimensional.lineage import normalize_dimensional_artifact

    with pytest.raises(DimensionalLineageError, match="nested publication contract"):
        normalize_dimensional_artifact(
            "customers",
            'INFO startup\n{"status":"PUBLISHED","identity":"att-9"}\nINFO shutdown',
            {"status": "PASS", "table_name": "customers", "snapshot_id": 9, "reconciliation_evidence_identity": "rec-9"},
            {"status": "PASS", "table": "lakehouse.silver.customers", "snapshot_id": 9, "quality_evidence_identity": "qual-9"},
        )


def test_reconciliation_cli_emits_canonical_pass_contract():
    from pathlib import Path
    source = (Path(__file__).parents[1] / "lakehouse/migration/reconcile.py").read_text(encoding="utf-8")
    assert '"status": "PASS"' in source
    assert '"table_name": spec.name' in source
    assert '"reconciliation_evidence_identity"' in source


def test_quality_report_contract_exposes_published_evidence_identity():
    from lakehouse.quality.contracts import TableQualityReport
    report = TableQualityReport("lakehouse.silver.customers", "PASS", 9, 1, (), quality_evidence_identity="qual-9")
    assert report.as_dict()["quality_evidence_identity"] == "qual-9"


def test_artifact_normalizer_rejects_actual_cli_table_mismatch():
    from gold.implementation.dimensional.lineage import normalize_dimensional_artifact
    with pytest.raises(DimensionalLineageError, match="table mismatch"):
        normalize_dimensional_artifact(
            "customers",
            {"status": "PUBLISHED", "key": "lakehouse/attestations/v1/att-9.json", "attestation": {"status": "PUBLISHED", "table": "products", "snapshot_id": 9, "identity": "att-9"}},
            {"status": "PASS", "table_name": "customers", "snapshot_id": 9, "reconciliation_evidence_identity": "rec-9"},
            {"status": "PASS", "table": "lakehouse.silver.customers", "snapshot_id": 9, "quality_evidence_identity": "qual-9"},
        )


def test_dimensional_lineage_rejects_non_silver_or_missing_source():
    lineage = dimensional_lineage()
    lineage["sources"]["orders"]["namespace"] = "gold"
    with pytest.raises(DimensionalLineageError, match="lakehouse.silver"):
        validate_dimensional_lineage(lineage)
    lineage = dimensional_lineage()
    lineage["sources"].pop("products")
    with pytest.raises(DimensionalLineageError, match="source set"):
        validate_dimensional_lineage(lineage)


def test_dimensional_lineage_source_contract_defaults_to_tombstones_and_overlap():
    from gold.implementation.dimensional.lineage import source_contract_metadata

    metadata = source_contract_metadata(dimensional_lineage())
    assert metadata["delete"] == {"mode": "tombstone_only", "tombstone_column": "_is_deleted", "physical_delete": False}
    assert metadata["late_data"]["overlap_days"] == 2
    assert metadata["sources"]["orders"]["delete_count"] == 0


def test_parquet_lineage_source_contract_does_not_require_snapshot_id():
    artifacts = {
        table: build_parquet_lineage_artifact(
            table=table,
            bucket="ecommerce-data-lake",
            manifest_key=f"quality-it/run-20261111-gold-01/silver_incremental/{table}/manifest.json",
            data_prefix=f"quality-it/run-20261111-gold-01/silver_incremental/{table}/runs/",
            manifest_identity=f"{index:064x}",
            batch_id=f"{index:032x}",
            row_count=1,
        )
        for index, table in enumerate(DIMENSIONAL_SOURCE_TABLES, start=1)
    }
    lineage = build_dimensional_lineage_from_artifacts(artifacts)
    lineage["source_contract"] = {}

    assert validate_dimensional_lineage(lineage) is True
    from gold.implementation.dimensional.lineage import source_contract_metadata

    metadata = source_contract_metadata(lineage)
    assert "snapshot_id" not in metadata["sources"]["orders"]
    assert metadata["sources"]["orders"]["delete_count"] == 0


def test_dimensional_lineage_rejects_physical_delete_policy():
    lineage = dimensional_lineage()
    lineage["source_contract"] = {"delete": {"physical_delete": True}}
    with pytest.raises(DimensionalLineageError, match="Physical deletes"):
        validate_dimensional_lineage(lineage)


def test_dimensional_lineage_rejects_implicit_schema_evolution():
    lineage = dimensional_lineage()
    lineage["source_contract"] = {"schema_evolution": {"allow_additive": True}}
    with pytest.raises(DimensionalLineageError, match="schema evolution"):
        validate_dimensional_lineage(lineage)
