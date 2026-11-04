
"""Gold runner supporting read-only validation and immutable publication."""

import argparse
import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from gold.implementation.contracts.datasets import get_gold_contract
from gold.implementation.readers import read_gold_sources

from gold.implementation.transformations.daily_sales import (
    build_daily_sales,
)
from gold.implementation.transformations.product_performance import (
    build_product_performance,
)
from gold.implementation.transformations.customer_metrics import (
    build_customer_metrics,
)
from gold.implementation.transformations.payment_summary import (
    build_payment_summary,
)

from gold.implementation.quality.daily_sales import (
    validate_daily_sales_sources,
    validate_daily_sales_output,
)
from gold.implementation.quality.product_performance import (
    validate_product_performance_sources,
    validate_product_performance_output,
)
from gold.implementation.quality.customer_metrics import (
    validate_customer_metrics_sources,
    validate_customer_metrics_output,
)
from gold.implementation.quality.payment_summary import (
    validate_payment_summary_sources,
    validate_payment_summary_output,
)
from common.minio import configure_s3a, create_minio_client
from common.runtime_policy import DRY_RUN, WRITE, require_feature_enabled
from gold.implementation.fencing import (
    generate_gold_attempt_id,
    validate_publication_eligibility,
)
from gold.implementation.ownership import acquire_gold_ownership
from gold.implementation.preflight import run_gold_preflight
from gold.implementation.storage.manifest import build_gold_manifest
from gold.implementation.storage.objects import discover_gold_parquet_objects
from gold.implementation.storage.publisher import publish_gold_snapshot
from gold.implementation.storage.recovery import execute_gold_recovery
from gold.implementation.storage.writer import write_gold_snapshot_parquet


DATASET_HANDLERS = {
    "daily_sales": (
        build_daily_sales,
        validate_daily_sales_sources,
        validate_daily_sales_output,
    ),
    "product_performance": (
        build_product_performance,
        validate_product_performance_sources,
        validate_product_performance_output,
    ),
    "customer_metrics": (
        build_customer_metrics,
        validate_customer_metrics_sources,
        validate_customer_metrics_output,
    ),
    "payment_summary": (
        build_payment_summary,
        validate_payment_summary_sources,
        validate_payment_summary_output,
    ),
}


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dataset",
        required=True,
        choices=sorted(DATASET_HANDLERS),
    )
    parser.add_argument(
        "--source-snapshots",
        required=True,
        help="JSON mapping table_name to published Silver snapshot ID",
    )
    parser.add_argument(
        "--mode",
        choices=[DRY_RUN, WRITE],
        default=DRY_RUN,
    )

    return parser.parse_args()


def validate_snapshot_config(dataset_name, raw_json):
    snapshots = json.loads(raw_json)

    if not isinstance(snapshots, dict):
        raise ValueError("source-snapshots must be a JSON object")

    contract = get_gold_contract(dataset_name)
    expected_sources = set(contract["sources"])

    if set(snapshots) != expected_sources:
        raise ValueError(
            f"Silver source mismatch: expected={sorted(expected_sources)}, "
            f"actual={sorted(snapshots)}"
        )

    for table_name, snapshot_id in snapshots.items():
        if (
            not isinstance(snapshot_id, str)
            or not snapshot_id.startswith("snap_")
        ):
            raise ValueError(
                f"Invalid Silver snapshot ID for {table_name}"
            )

    return snapshots


def _write_gold_snapshot(
    spark,
    client,
    bucket,
    dataset_name,
    snapshots,
    gold_df,
    row_count,
):
    """Write, verify, and publish an immutable Gold snapshot."""
    preflight = run_gold_preflight(
        spark=spark,
        client=client,
        bucket=bucket,
        dataset_name=dataset_name,
        source_snapshots=snapshots,
    )

    if preflight["status"] == "VERIFIED_EXISTING":
        return {
            "snapshot_id": preflight["selected_snapshot_id"],
            "manifest_key": None,
            "storage_written": False,
            "published": True,
            "publication_status": "ALREADY_PUBLISHED",
        }

    if preflight["action"] not in {
        "NEW_SNAPSHOT_CANDIDATE",
        "REQUIRES_RECOVERY",
    }:
        raise RuntimeError(
            "Gold WRITE blocked by preflight: "
            f"{preflight['reason']}"
        )

    snapshot_id = preflight["deterministic_snapshot_id"]
    contract = get_gold_contract(dataset_name)

    with acquire_gold_ownership(dataset_name, snapshot_id) as ownership:
        attempt = ownership.allocate_fencing_token(
            generate_gold_attempt_id()
        )
        ownership.assert_alive()
        validate_publication_eligibility(
            attempt,
            attempt.fencing_token,
            ownership_alive=True,
        )

        # Kiểm tra lại trạng thái khi đang giữ execution lock.
        preflight = run_gold_preflight(
            spark=spark,
            client=client,
            bucket=bucket,
            dataset_name=dataset_name,
            source_snapshots=snapshots,
        )

        if preflight["status"] == "VERIFIED_EXISTING":
            return {
                "snapshot_id": preflight["selected_snapshot_id"],
                "manifest_key": None,
                "storage_written": False,
                "published": True,
                "publication_status": "ALREADY_PUBLISHED",
                "attempt_id": attempt.attempt_id,
                "fencing_token": attempt.fencing_token,
            }

        if preflight["action"] == "NEW_SNAPSHOT_CANDIDATE":
            write_result = write_gold_snapshot_parquet(
                spark=spark,
                gold_df=gold_df,
                bucket=bucket,
                dataset_name=dataset_name,
                snapshot_id=snapshot_id,
            )
            storage_written = True
        elif preflight["action"] == "REQUIRES_RECOVERY":
            from gold.implementation.storage.contract import get_gold_storage_paths

            write_result = {
                "snapshot_id": snapshot_id,
                **get_gold_storage_paths(dataset_name, snapshot_id),
            }
            storage_written = False
        else:
            raise RuntimeError(
                "Gold WRITE state changed while acquiring ownership: "
                f"{preflight['reason']}"
            )

        parquet_objects = discover_gold_parquet_objects(
            client=client,
            bucket=bucket,
            data_prefix=write_result["data_prefix"],
        )
        manifest = build_gold_manifest(
            dataset_name=dataset_name,
            snapshot_id=snapshot_id,
            source_snapshots=snapshots,
            row_count=row_count,
            schema=contract["columns"],
            parquet_objects=parquet_objects,
        )

        ownership.assert_alive()
        validate_publication_eligibility(
            attempt,
            attempt.fencing_token,
            ownership_alive=True,
        )

        if storage_written:
            publication = publish_gold_snapshot(
                spark=spark,
                client=client,
                bucket=bucket,
                manifest=manifest,
            )
        else:
            publication = execute_gold_recovery(
                spark=spark,
                client=client,
                bucket=bucket,
                dataset_name=dataset_name,
                snapshot_id=snapshot_id,
                candidate_manifest=manifest,
            )

        return {
            "snapshot_id": snapshot_id,
            "manifest_key": manifest["manifest_key"],
            "storage_written": storage_written,
            "published": publication["status"] in {
                "PUBLISHED",
                "ALREADY_PUBLISHED",
            },
            "publication_status": publication["status"],
            "attempt_id": attempt.attempt_id,
            "fencing_token": attempt.fencing_token,
        }


def run_dataset(
    spark,
    client,
    bucket,
    dataset_name,
    snapshots,
    mode=DRY_RUN,
):
    if mode not in {DRY_RUN, WRITE}:
        raise ValueError(f"Unsupported Gold mode: {mode}")
    if mode == WRITE:
        require_feature_enabled("GOLD_SNAPSHOT_WRITE_ENABLED")

    contract = get_gold_contract(dataset_name)

    sources = read_gold_sources(
        spark=spark,
        client=client,
        bucket=bucket,
        dataset_name=dataset_name,
        snapshot_ids=snapshots,
    )

    for table_name in contract["sources"]:
        if sources[table_name]["snapshot_id"] != snapshots[table_name]:
            raise RuntimeError(
                f"Silver lineage mismatch: {table_name}"
            )

    source_dfs = [
        sources[table_name]["dataframe"]
        for table_name in contract["sources"]
    ]

    transform, validate_sources, validate_output = (
        DATASET_HANDLERS[dataset_name]
    )

    source_result = validate_sources(*source_dfs)

    if source_result["status"] != "PASS":
        raise RuntimeError("Gold source quality failed")

    gold_df = transform(*source_dfs).cache()

    try:
        output_result = validate_output(gold_df)

        if output_result["status"] != "PASS":
            raise RuntimeError("Gold output quality failed")

        row_count = gold_df.count()

        if row_count <= 0:
            raise RuntimeError("Gold output is empty")

        pk = list(contract["primary_key"])

        grain_count = gold_df.select(*pk).distinct().count()

        if grain_count != row_count:
            raise RuntimeError("Gold grain mismatch")

        result = {
            "status": "PASS",
            "mode": mode,
            "dataset_name": dataset_name,
            "source_snapshots": snapshots,
            "row_count": row_count,
            "grain_count": grain_count,
            "source_quality": "PASS",
            "output_quality": "PASS",
            "storage_written": False,
            "published": False,
        }

        if dataset_name == "payment_summary":
            payment_count = gold_df.agg(
                F.sum("payment_count").alias("value")
            ).first()["value"]

            source_payment_count = source_dfs[0].count()

            if payment_count != source_payment_count:
                raise RuntimeError(
                    "Payment count reconciliation failed"
                )

            result["payment_count"] = payment_count

        if mode == WRITE:
            result.update(
                _write_gold_snapshot(
                    spark=spark,
                    client=client,
                    bucket=bucket,
                    dataset_name=dataset_name,
                    snapshots=snapshots,
                    gold_df=gold_df,
                    row_count=row_count,
                )
            )

        return result

    finally:
        gold_df.unpersist()


def main():
    args = parse_args()

    snapshots = validate_snapshot_config(
        args.dataset,
        args.source_snapshots,
    )

    spark = None

    try:
        spark = (
            SparkSession.builder
            .master("local[2]")
            .appName(f"GoldRunner_{args.dataset}")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")
        configure_s3a(spark)

        client = create_minio_client()

        result = run_dataset(
            spark=spark,
            client=client,
            bucket=os.environ["BRONZE_BUCKET"],
            dataset_name=args.dataset,
            snapshots=snapshots,
            mode=args.mode,
        )

        print(
            "GOLD_RUNNER_RESULT=" + json.dumps(
                result,
                sort_keys=True,
            ),
            flush=True,
        )

        print(
            f"GOLD RUNNER {args.dataset}: PASS",
            flush=True,
        )

    except Exception as exc:
        print(
            f"GOLD RUNNER FAILED: {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        raise

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
