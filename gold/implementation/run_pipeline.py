
"""Gold production runner - Phase A: read-only DRY_RUN."""

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
        choices=["DRY_RUN"],
        default="DRY_RUN",
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


def run_dataset(spark, client, bucket, dataset_name, snapshots):
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
            "mode": "DRY_RUN",
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

        return result

    finally:
        gold_df.unpersist()


def main():
    args = parse_args()

    if args.mode != "DRY_RUN":
        raise ValueError("Only DRY_RUN is supported")

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
