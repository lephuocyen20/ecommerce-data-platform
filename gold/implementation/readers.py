
from gold.implementation.contracts.datasets import (
    get_gold_contract,
)
from gold.implementation.contracts.validator import (
    validate_gold_contract,
)
from silver.snapshots.publisher import (
    get_snapshot_manifest_key,
    read_published_manifest,
)
from silver.snapshots.recovery import (
    inspect_snapshot_state,
)
from silver.snapshots.verifier import (
    verify_snapshot,
)
from gold.implementation.source_lineage import (
    ICEBERG_SNAPSHOT,
    PARQUET_SNAPSHOT,
    GoldSourceLineageError,
    validate_source_lineage,
)


def read_gold_source(
    spark,
    client,
    bucket,
    dataset_name,
    table_name,
    snapshot_id,
):
    """
    Read a verified, published Silver Snapshot
    as an input to a Gold dataset.

    Read-only. Does not publish or modify objects.
    """

    validate_gold_contract(dataset_name)
    contract = get_gold_contract(dataset_name)

    if table_name not in contract["sources"]:
        raise ValueError(
            f"{table_name} is not a source of {dataset_name}"
        )

    if not isinstance(snapshot_id, str) or not snapshot_id:
        raise ValueError("Invalid snapshot ID")

    state = inspect_snapshot_state(
        client=client,
        bucket=bucket,
        table_name=table_name,
        snapshot_id=snapshot_id,
    )

    if state["state"] != "PUBLISHED":
        raise ValueError(
            f"Silver Snapshot is not published: "
            f"{table_name}/{snapshot_id} "
            f"state={state['state']}"
        )

    manifest_key = get_snapshot_manifest_key(
        table_name=table_name,
        snapshot_id=snapshot_id,
    )

    manifest = read_published_manifest(
        client=client,
        bucket=bucket,
        manifest_key=manifest_key,
    )

    if manifest.get("snapshot_id") != snapshot_id:
        raise ValueError("Snapshot ID mismatch")

    if manifest.get("table_name") != table_name:
        raise ValueError("Snapshot table mismatch")

    verify_snapshot(
        spark=spark,
        client=client,
        bucket=bucket,
        manifest=manifest,
    )

    data_prefix = manifest["data_prefix"]

    df = spark.read.parquet(
        f"s3a://{bucket}/{data_prefix}"
    )

    return {
        "dataframe": df,
        "manifest": manifest,
        "table_name": table_name,
        "snapshot_id": snapshot_id,
    }


def read_gold_sources(
    spark,
    client,
    bucket,
    dataset_name,
    snapshot_ids,
):
    """
    Read all required Silver Snapshot sources
    for one Gold dataset.
    """

    validate_gold_contract(dataset_name)
    contract = get_gold_contract(dataset_name)

    expected_sources = set(contract["sources"])

    if not isinstance(snapshot_ids, dict):
        raise ValueError("Snapshot IDs must be a dictionary")

    if set(snapshot_ids) != expected_sources:
        raise ValueError(
            f"Snapshot source mismatch for {dataset_name}"
        )

    sources = {}

    for table_name in contract["sources"]:
        sources[table_name] = read_gold_source(
            spark=spark,
            client=client,
            bucket=bucket,
            dataset_name=dataset_name,
            table_name=table_name,
            snapshot_id=snapshot_ids[table_name],
        )

    return sources


def _read_evidence(client, bucket, identity):
    """Read an immutable evidence document; identity is content-addressed."""
    key = f"lakehouse/evidence/v2/{identity}.json"
    response = client.get_object(bucket, key)
    try:
        import json
        document = json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()
    if not isinstance(document, dict) or document.get("identity") != identity:
        raise GoldSourceLineageError("Lakehouse evidence identity mismatch")
    return document


def read_iceberg_gold_sources(spark, client, bucket, dataset_name, lineage):
    """Read only explicit Iceberg snapshots after evidence verification.

    SQL identifiers are assembled solely after strict contract validation; the
    snapshot is selected with time travel, never the mutable table head.
    """
    validate_source_lineage(lineage, dataset_name)
    if lineage["source_type"] != ICEBERG_SNAPSHOT:
        raise GoldSourceLineageError("Iceberg reader requires iceberg_snapshot lineage")
    contract = get_gold_contract(dataset_name)
    from lakehouse.evidence import verify_evidence
    from lakehouse.attestation import read_snapshot_attestation
    sources = {}
    for table in contract["sources"]:
        source = lineage["sources"][table]
        snapshot_id = source["snapshot_id"]
        read_snapshot_attestation(
            client, bucket, source["snapshot_attestation_identity"],
            table=table, catalog=source["catalog"], namespace=source["namespace"],
            ref=source["ref"], snapshot_id=snapshot_id,
        )
        for evidence_name in ("reconciliation_evidence", "quality_evidence"):
            expected = source[evidence_name]
            actual = _read_evidence(client, bucket, expected["identity"])
            kind = "reconciliation" if evidence_name == "reconciliation_evidence" else "quality"
            verify_evidence(
                actual, kind=kind, table=table, snapshot_id=snapshot_id,
                catalog=source["catalog"], namespace=source["namespace"],
                ref=source["ref"],
                snapshot_attestation_identity=source["snapshot_attestation_identity"],
            )
            if {key: actual[key] for key in expected} != expected:
                raise GoldSourceLineageError(f"{evidence_name} artifact mismatch")
        identifier = f"{source['catalog']}.{source['namespace']}.{source['table']}"
        try:
            dataframe = spark.sql(f"SELECT * FROM {identifier} VERSION AS OF {snapshot_id}")
        except Exception as exc:
            raise GoldSourceLineageError(
                f"Pinned Iceberg snapshot cannot be read: {identifier}@{snapshot_id}"
            ) from exc
        sources[table] = {
            "dataframe": dataframe,
            "table_name": table,
            "snapshot_id": snapshot_id,
            "source_type": ICEBERG_SNAPSHOT,
            "lineage": source,
        }
    return sources


def read_gold_sources_from_lineage(spark, client, bucket, dataset_name, lineage):
    """Compatibility router for v2 Gold lineage envelopes."""
    validate_source_lineage(lineage, dataset_name)
    if lineage["source_type"] == PARQUET_SNAPSHOT:
        snapshots = {table: source["snapshot_id"] for table, source in lineage["sources"].items()}
        return read_gold_sources(spark, client, bucket, dataset_name, snapshots)
    return read_iceberg_gold_sources(spark, client, bucket, dataset_name, lineage)
