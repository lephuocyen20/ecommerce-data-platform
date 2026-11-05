
import os

from minio import Minio

from gold.implementation.storage.lineage import (
    discover_gold_lineage,
)


SOURCES = {
    "payment_summary": {
        "payments": "snap_4be50f0c3dd9715decad9d2456b4898c",
    },
    "daily_sales": {
        "orders": "snap_59aa507d4eb73cfc989daaff65fa0c8d",
        "order_items": "snap_6cdf60d315092edda63c3d396b67aa2b",
    },
    "product_performance": {
        "products": "snap_272c20e295221e271d0ac903ca631c24",
        "orders": "snap_59aa507d4eb73cfc989daaff65fa0c8d",
        "order_items": "snap_6cdf60d315092edda63c3d396b67aa2b",
    },
    "customer_metrics": {
        "customers": "snap_fdb8aabc86dbb4a98d1d4e01c78f7fc4",
        "orders": "snap_59aa507d4eb73cfc989daaff65fa0c8d",
        "order_items": "snap_6cdf60d315092edda63c3d396b67aa2b",
    },
}


EXPECTED = {
    "payment_summary": "gold_snap_98eaba83dfe04c22a50de73db0c0bd4e",
    "daily_sales": "gold_snap_fd4b1f8c20f3461a9f22902e97d9a4f5",
    "product_performance": "gold_snap_83963a2c1462490b904dfa9dd1b936b1",
    "customer_metrics": "gold_snap_12a2e84a51e745419b7324f7de9cf113",
}


def main():
    client = Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )

    bucket = os.environ["BRONZE_BUCKET"]

    for dataset_name, source_snapshots in SOURCES.items():
        matches = discover_gold_lineage(
            client=client,
            bucket=bucket,
            dataset_name=dataset_name,
            source_snapshots=source_snapshots,
        )

        matching_ids = {
            item["snapshot_id"] for item in matches
        }

        assert EXPECTED[dataset_name] in matching_ids, (
            f"Missing expected published snapshot: {dataset_name}"
        )

        assert len(matches) == 1, (
            f"Expected exactly one matching snapshot for "
            f"{dataset_name}, got {len(matches)}"
        )

        print(
            f"{dataset_name}: PASS "
            f"({EXPECTED[dataset_name]})"
        )

    print("GOLD LINEAGE DISCOVERY: PASS 4/4")


if __name__ == "__main__":
    main()
