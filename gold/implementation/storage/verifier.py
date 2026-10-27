
from pyspark.sql import functions as F

from gold.implementation.contracts.datasets import (
    get_gold_contract,
)
from gold.implementation.storage.contract import (
    GoldStorageContractError,
    build_gold_storage_metadata,
)
from gold.implementation.storage.manifest import (
    validate_gold_manifest_schema,
    validate_gold_parquet_objects,
)
from gold.implementation.storage.objects import (
    discover_gold_parquet_objects,
)

from datetime import datetime

class GoldSnapshotVerificationError(ValueError):
    pass


def verify_gold_manifest_metadata(manifest):
    """
    Validate manifest identity, contract, paths and lineage.
    """

    if not isinstance(manifest, dict):
        raise GoldSnapshotVerificationError(
            "Gold manifest must be a dictionary"
        )

    try:
        expected = build_gold_storage_metadata(
            dataset_name=manifest["dataset_name"],
            snapshot_id=manifest["snapshot_id"],
            source_snapshots=manifest["source_snapshots"],
        )

        for key, value in expected.items():
            if manifest.get(key) != value:
                raise GoldSnapshotVerificationError(
                    f"Gold manifest metadata mismatch: {key}"
                )

        if manifest.get("status") != "COMPLETED":
            raise GoldSnapshotVerificationError(
                "Gold manifest is not completed"
            )

        row_count = manifest.get("row_count")

        if (
            not isinstance(row_count, int)
            or isinstance(row_count, bool)
            or row_count < 0
        ):
            raise GoldSnapshotVerificationError(
                "Invalid Gold manifest row count"
            )


        created_at = manifest.get("created_at")

        if not isinstance(created_at, str):
            raise GoldSnapshotVerificationError(
                "Invalid Gold manifest created_at"
            )

        try:
            parsed_created_at = datetime.fromisoformat(created_at)
        except ValueError as exc:
            raise GoldSnapshotVerificationError(
                "Invalid Gold manifest created_at"
            ) from exc

        if parsed_created_at.tzinfo is None or (
            parsed_created_at.utcoffset() is None
        ):
            raise GoldSnapshotVerificationError(
                "Gold manifest created_at must include timezone"
            )

        validate_gold_manifest_schema(
            manifest["dataset_name"],
            manifest.get("schema"),
        )

        validate_gold_parquet_objects(
            manifest.get("parquet_objects"),
            expected["data_prefix"],
        )

        if manifest.get("parquet_file_count") != len(
            manifest["parquet_objects"]
        ):
            raise GoldSnapshotVerificationError(
                "Gold Parquet file count mismatch"
            )

    except (KeyError, GoldStorageContractError) as exc:
        raise GoldSnapshotVerificationError(
            f"Invalid Gold manifest: {exc}"
        ) from exc

    return True


def verify_gold_object_integrity(
    client,
    bucket,
    manifest,
):
    """
    Compare real MinIO objects against the manifest.
    """

    actual_objects = discover_gold_parquet_objects(
        client=client,
        bucket=bucket,
        data_prefix=manifest["data_prefix"],
    )

    if actual_objects != manifest["parquet_objects"]:
        raise GoldSnapshotVerificationError(
            "Gold Parquet object integrity mismatch"
        )

    return True


def verify_gold_dataframe(
    dataframe,
    manifest,
):
    """
    Verify schema, row count and primary-key uniqueness.
    """

    contract = get_gold_contract(
        manifest["dataset_name"]
    )

    expected_columns = list(contract["columns"])
    actual_columns = dataframe.columns

    if actual_columns != expected_columns:
        raise GoldSnapshotVerificationError(
            "Gold dataframe column mismatch"
        )

    # Normalize Spark SQL type aliases before comparison.
    TYPE_ALIASES = {
        "bigint": "long",
        "integer": "int",
        "smallint": "short",
        "tinyint": "byte",
    }

    actual_types = {
        field.name: TYPE_ALIASES.get(
        field.dataType.simpleString(),
        field.dataType.simpleString(),
        )
        for field in dataframe.schema.fields
    }

    if actual_types != contract["columns"]:
        raise GoldSnapshotVerificationError(
            "Gold dataframe type mismatch"
        )

    actual_count = dataframe.count()

    if actual_count != manifest["row_count"]:
        raise GoldSnapshotVerificationError(
            "Gold dataframe row count mismatch"
        )

    primary_key = contract["primary_key"]

    null_condition = None

    for column in primary_key:
        condition = F.col(column).isNull()

        null_condition = (
            condition
            if null_condition is None
            else null_condition | condition
        )

    if dataframe.filter(null_condition).limit(1).count():
        raise GoldSnapshotVerificationError(
            "Gold primary key contains NULL"
        )

    duplicate_exists = (
        dataframe.groupBy(*primary_key)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicate_exists:
        raise GoldSnapshotVerificationError(
            "Gold primary key contains duplicates"
        )

    return True


def verify_gold_snapshot(
    spark,
    client,
    bucket,
    manifest,
):
    """
    Full verification before Gold snapshot publication.

    Does not publish or modify objects.
    """

    verify_gold_manifest_metadata(manifest)

    verify_gold_object_integrity(
        client=client,
        bucket=bucket,
        manifest=manifest,
    )

    output_path = (
        f"s3a://{bucket}/{manifest['data_prefix']}"
    )

    dataframe = spark.read.parquet(output_path)

    verify_gold_dataframe(
        dataframe=dataframe,
        manifest=manifest,
    )

    return {
        "dataset_name": manifest["dataset_name"],
        "snapshot_id": manifest["snapshot_id"],
        "row_count": manifest["row_count"],
        "status": "VERIFIED",
    }
