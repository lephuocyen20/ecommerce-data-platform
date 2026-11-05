
"""Inspect matching Gold lineage without modifying MinIO."""

import json
import os

from minio import Minio

from gold.implementation.storage.lineage import (
    discover_gold_lineage,
)

from tests.test_gold_lineage_discovery_e2e import (
    SOURCES,
    EXPECTED,
)


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

        print(f"\nDATASET: {dataset_name}", flush=True)
        print(f"MATCHING MANIFESTS: {len(matches)}", flush=True)

        for item in matches:
            manifest = item["manifest"]

            print(
                json.dumps(
                    {
                        "snapshot_id": item["snapshot_id"],
                        "is_expected_snapshot": (
                            item["snapshot_id"] == EXPECTED[dataset_name]
                        ),
                        "created_at": manifest.get("created_at"),
                        "contract_version": manifest.get("contract_version"),
                        "transformation_version": manifest.get(
                            "transformation_version"
                        ),
                        "row_count": manifest.get("row_count"),
                        "manifest_key": item["manifest_key"],
                    },
                    sort_keys=True,
                ),
                flush=True,
            )

    print("\nGOLD LINEAGE INSPECTION: COMPLETE", flush=True)


if __name__ == "__main__":
    main()
