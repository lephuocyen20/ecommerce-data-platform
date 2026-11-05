import unittest
from unittest.mock import patch
from silver.snapshots.pipeline import run_snapshot_pipeline
from silver.snapshots.pipeline import (
    prepare_snapshot_batches,
)


class TestSnapshotPipeline(unittest.TestCase):

    @patch(
        "silver.snapshots.pipeline.validate_batch_continuity"
    )
    @patch(
        "silver.snapshots.pipeline.get_committed_batches"
    )
    def test_prepare_success(
        self,
        mock_discovery,
        mock_continuity,
    ):
        batches = [
            {"batch_id": "batch_001"},
            {"batch_id": "batch_002"},
        ]

        mock_discovery.return_value = batches

        conn = object()

        result = prepare_snapshot_batches(
            conn=conn,
            table_name="orders",
        )

        self.assertEqual(result, batches)

        mock_discovery.assert_called_once_with(
            conn=conn,
            table_name="orders",
        )

        mock_continuity.assert_called_once_with(batches)

    @patch(
        "silver.snapshots.pipeline.get_committed_batches"
    )
    def test_reject_empty_batches(self, mock_discovery):
        mock_discovery.return_value = []

        with self.assertRaisesRegex(
            ValueError,
            "No committed batches",
        ):
            prepare_snapshot_batches(
                conn=object(),
                table_name="orders",
            )

    @patch(
        "silver.snapshots.pipeline.validate_batch_continuity"
    )
    @patch(
        "silver.snapshots.pipeline.get_committed_batches"
    )
    def test_reject_duplicate_batch_ids(
        self,
        mock_discovery,
        mock_continuity,
    ):
        mock_discovery.return_value = [
            {"batch_id": "batch_001"},
            {"batch_id": "batch_001"},
        ]

        with self.assertRaisesRegex(
            ValueError,
            "Duplicate committed batch IDs",
        ):
            prepare_snapshot_batches(
                conn=object(),
                table_name="orders",
            )

    @patch(
        "silver.snapshots.pipeline.validate_batch_continuity"
    )
    @patch(
        "silver.snapshots.pipeline.get_committed_batches"
    )
    def test_continuity_failure_propagates(
        self,
        mock_discovery,
        mock_continuity,
    ):
        mock_discovery.return_value = [
            {"batch_id": "batch_001"}
        ]

        mock_continuity.side_effect = ValueError(
            "Watermark continuity violation"
        )

        with self.assertRaisesRegex(
            ValueError,
            "Watermark continuity violation",
        ):
            prepare_snapshot_batches(
                conn=object(),
                table_name="orders",
            )

    @patch("silver.snapshots.pipeline.inspect_snapshot_state")
    @patch("silver.snapshots.pipeline.generate_snapshot_id")
    @patch("silver.snapshots.pipeline.prepare_snapshot_batches")
    def test_incomplete_snapshot_rejected(
        self,
        mock_prepare,
        mock_identity,
        mock_inspect,
    ):
        mock_prepare.return_value = [
            {"table_name": "orders", "batch_id": "batch_001"}
        ]

        mock_identity.return_value = "snap_test001"

        mock_inspect.return_value = {
            "state": "INCOMPLETE",
        }

        with self.assertRaisesRegex(
            RuntimeError,
            "Incomplete snapshot detected",
        ):
            run_snapshot_pipeline(
                conn=object(),
                spark=object(),
                client=object(),
                bucket="test-bucket",
                table_name="orders",
            )

    def test_unimplemented_state_rejected(self):
        from unittest.mock import patch

        batches = [{
            "table_name": "orders",
            "batch_id": "batch_001",
        }]

        with (
            patch(
                "silver.snapshots.pipeline.prepare_snapshot_batches",
                return_value=batches,
            ),
            patch(
                "silver.snapshots.pipeline.generate_snapshot_id",
                return_value="snap_test001",
            ),
            patch(
                "silver.snapshots.pipeline.inspect_snapshot_state",
                return_value={
                    "state": "UNKNOWN_STATE",
                },
            ),
            patch(
                "silver.snapshots.pipeline.build_silver_snapshot"
            ) as mock_build,
        ):
            with self.assertRaisesRegex(
                NotImplementedError,
                "Snapshot state not implemented yet: UNKNOWN_STATE",
            ):
                run_snapshot_pipeline(
                    conn=object(),
                    spark=object(),
                    client=object(),
                    bucket="test-bucket",
                    table_name="orders",
                )

        mock_build.assert_not_called()


    def test_published_snapshot_success(self):
        from unittest.mock import patch

        batches = [
            {
                "table_name": "orders",
                "batch_id": "batch_001",
                "end_timestamp": None,
                "end_id": 100,
            }
        ]

        manifest = {
            "snapshot_id": "snap_test001",
            "table_name": "orders",
            "source_batch_ids": ["batch_001"],
            "source_batch_count": 1,
            "end_timestamp": None,
            "end_id": 100,
        }

        with (
            patch(
                "silver.snapshots.pipeline.prepare_snapshot_batches",
                return_value=batches,
            ),
            patch(
                "silver.snapshots.pipeline.generate_snapshot_id",
                return_value="snap_test001",
            ),
            patch(
                "silver.snapshots.pipeline.inspect_snapshot_state",
                return_value={
                    "state": "PUBLISHED",
                    "manifest_key": "test/manifest.json",
                },
            ),
            patch(
                "silver.snapshots.pipeline.read_published_manifest",
                return_value=manifest,
            ),
            patch(
                "silver.snapshots.pipeline.verify_snapshot"
            ) as mock_verify,
        ):
            result = run_snapshot_pipeline(
                conn=object(),
                spark=object(),
                client=object(),
                bucket="test-bucket",
                table_name="orders",
            )

        self.assertEqual(
            result["publication_status"],
            "ALREADY_PUBLISHED",
        )
        mock_verify.assert_called_once()

    def test_published_snapshot_source_mismatch(self):
        from unittest.mock import patch

        batches = [
            {
                "table_name": "orders",
                "batch_id": "batch_001",
                "end_timestamp": None,
                "end_id": 100,
            }
        ]

        manifest = {
            "snapshot_id": "snap_test001",
            "table_name": "orders",
            "source_batch_ids": ["different_batch"],
            "source_batch_count": 1,
            "end_timestamp": None,
            "end_id": 100,
        }

        with (
            patch(
                "silver.snapshots.pipeline.prepare_snapshot_batches",
                return_value=batches,
            ),
            patch(
                "silver.snapshots.pipeline.generate_snapshot_id",
                return_value="snap_test001",
            ),
            patch(
                "silver.snapshots.pipeline.inspect_snapshot_state",
                return_value={
                    "state": "PUBLISHED",
                    "manifest_key": "test/manifest.json",
                },
            ),
            patch(
                "silver.snapshots.pipeline.read_published_manifest",
                return_value=manifest,
            ),
            patch(
                "silver.snapshots.pipeline.verify_snapshot"
            ) as mock_verify,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "source batches mismatch",
            ):
                run_snapshot_pipeline(
                    conn=object(),
                    spark=object(),
                    client=object(),
                    bucket="test-bucket",
                    table_name="orders",
                )

        mock_verify.assert_not_called()


    def test_published_snapshot_identity_mismatch(self):
        from unittest.mock import patch

        batches = [{
            "table_name": "orders",
            "batch_id": "batch_001",
            "end_timestamp": None,
            "end_id": 100,
        }]

        manifest = {
            "snapshot_id": "snap_wrong",
            "table_name": "orders",
            "source_batch_ids": ["batch_001"],
            "source_batch_count": 1,
            "end_timestamp": None,
            "end_id": 100,
        }

        with (
            patch(
                "silver.snapshots.pipeline.prepare_snapshot_batches",
                return_value=batches,
            ),
            patch(
                "silver.snapshots.pipeline.generate_snapshot_id",
                return_value="snap_test001",
            ),
            patch(
                "silver.snapshots.pipeline.inspect_snapshot_state",
                return_value={
                    "state": "PUBLISHED",
                    "manifest_key": "test/manifest.json",
                },
            ),
            patch(
                "silver.snapshots.pipeline.read_published_manifest",
                return_value=manifest,
            ),
            patch(
                "silver.snapshots.pipeline.verify_snapshot"
            ) as mock_verify,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "identity mismatch",
            ):
                run_snapshot_pipeline(
                    conn=object(),
                    spark=object(),
                    client=object(),
                    bucket="test-bucket",
                    table_name="orders",
                )

        mock_verify.assert_not_called()

    def test_published_snapshot_watermark_mismatch(self):
        from unittest.mock import patch

        batches = [{
            "table_name": "orders",
            "batch_id": "batch_001",
            "end_timestamp": None,
            "end_id": 100,
        }]

        manifest = {
            "snapshot_id": "snap_test001",
            "table_name": "orders",
            "source_batch_ids": ["batch_001"],
            "source_batch_count": 1,
            "end_timestamp": None,
            "end_id": 999,
        }

        with (
            patch(
                "silver.snapshots.pipeline.prepare_snapshot_batches",
                return_value=batches,
            ),
            patch(
                "silver.snapshots.pipeline.generate_snapshot_id",
                return_value="snap_test001",
            ),
            patch(
                "silver.snapshots.pipeline.inspect_snapshot_state",
                return_value={
                    "state": "PUBLISHED",
                    "manifest_key": "test/manifest.json",
                },
            ),
            patch(
                "silver.snapshots.pipeline.read_published_manifest",
                return_value=manifest,
            ),
            patch(
                "silver.snapshots.pipeline.verify_snapshot"
            ) as mock_verify,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "watermark mismatch",
            ):
                run_snapshot_pipeline(
                    conn=object(),
                    spark=object(),
                    client=object(),
                    bucket="test-bucket",
                    table_name="orders",
                )

        mock_verify.assert_not_called()

    def test_published_snapshot_verification_failure(self):
        from unittest.mock import patch

        batches = [{
            "table_name": "orders",
            "batch_id": "batch_001",
            "end_timestamp": None,
            "end_id": 100,
        }]

        manifest = {
            "snapshot_id": "snap_test001",
            "table_name": "orders",
            "source_batch_ids": ["batch_001"],
            "source_batch_count": 1,
            "end_timestamp": None,
            "end_id": 100,
        }

        with (
            patch(
                "silver.snapshots.pipeline.prepare_snapshot_batches",
                return_value=batches,
            ),
            patch(
                "silver.snapshots.pipeline.generate_snapshot_id",
                return_value="snap_test001",
            ),
            patch(
                "silver.snapshots.pipeline.inspect_snapshot_state",
                return_value={
                    "state": "PUBLISHED",
                    "manifest_key": "test/manifest.json",
                },
            ),
            patch(
                "silver.snapshots.pipeline.read_published_manifest",
                return_value=manifest,
            ),
            patch(
                "silver.snapshots.pipeline.verify_snapshot",
                side_effect=ValueError("Snapshot row count mismatch"),
            ),
        ):
            with self.assertRaisesRegex(
                ValueError,
                "Snapshot row count mismatch",
            ):
                run_snapshot_pipeline(
                    conn=object(),
                    spark=object(),
                    client=object(),
                    bucket="test-bucket",
                    table_name="orders",
                )

    def test_unpublished_snapshot_recovery_success(self):
        from unittest.mock import patch

        batches = [{
            "table_name": "orders",
            "batch_id": "batch_001",
            "end_timestamp": None,
            "end_id": 100,
        }]

        data_prefix = (
            "silver_snapshots/orders/"
            "snapshots/snapshot_id=snap_test001/data/"
        )

        with (
            patch(
                "silver.snapshots.pipeline.prepare_snapshot_batches",
                return_value=batches,
            ),
            patch(
                "silver.snapshots.pipeline.generate_snapshot_id",
                return_value="snap_test001",
            ),
            patch(
                "silver.snapshots.pipeline.inspect_snapshot_state",
                return_value={
                    "state": "UNPUBLISHED",
                    "data_prefix": data_prefix,
                },
            ),
            patch(
                "silver.snapshots.pipeline.validate_recovery_snapshot",
                return_value={
                    "validation_status": "VERIFIED",
                    "row_count": 3,
                },
            ) as mock_validate,
            patch(
                "silver.snapshots.pipeline.discover_snapshot_parquet_objects",
                return_value=[{
                    "object_key": data_prefix + "part-00000.parquet",
                    "size": 1024,
                }],
            ),
            patch(
                "silver.snapshots.pipeline.build_snapshot_metadata",
                return_value={"snapshot_id": "snap_test001"},
            ),
            patch(
                "silver.snapshots.pipeline.publish_snapshot",
                return_value={
                    "snapshot_id": "snap_test001",
                    "publication_status": "PUBLISHED",
                },
            ) as mock_publish,
        ):
            result = run_snapshot_pipeline(
                conn=object(),
                spark=object(),
                client=object(),
                bucket="test-bucket",
                table_name="orders",
            )

        self.assertEqual(
            result["publication_status"],
            "PUBLISHED",
        )
        mock_validate.assert_called_once()
        mock_publish.assert_called_once()

    def test_unpublished_recovery_data_mismatch(self):
        from unittest.mock import patch

        batches = [{
            "table_name": "orders",
            "batch_id": "batch_001",
        }]

        with (
            patch(
                "silver.snapshots.pipeline.prepare_snapshot_batches",
                return_value=batches,
            ),
            patch(
                "silver.snapshots.pipeline.generate_snapshot_id",
                return_value="snap_test001",
            ),
            patch(
                "silver.snapshots.pipeline.inspect_snapshot_state",
                return_value={
                    "state": "UNPUBLISHED",
                    "data_prefix": (
                        "silver_snapshots/orders/"
                        "snapshots/snapshot_id=snap_test001/data/"
                    ),
                },
            ),
            patch(
                "silver.snapshots.pipeline.validate_recovery_snapshot",
                side_effect=ValueError(
                    "Recovery snapshot data mismatch"
                ),
            ),
            patch(
                "silver.snapshots.pipeline.publish_snapshot"
            ) as mock_publish,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "Recovery snapshot data mismatch",
            ):
                run_snapshot_pipeline(
                    conn=object(),
                    spark=object(),
                    client=object(),
                    bucket="test-bucket",
                    table_name="orders",
                )

        mock_publish.assert_not_called()

    def test_unpublished_recovery_not_verified(self):
        from unittest.mock import patch

        batches = [{
            "table_name": "orders",
            "batch_id": "batch_001",
        }]

        with (
            patch(
                "silver.snapshots.pipeline.prepare_snapshot_batches",
                return_value=batches,
            ),
            patch(
                "silver.snapshots.pipeline.generate_snapshot_id",
                return_value="snap_test001",
            ),
            patch(
                "silver.snapshots.pipeline.inspect_snapshot_state",
                return_value={
                    "state": "UNPUBLISHED",
                    "data_prefix": (
                        "silver_snapshots/orders/"
                        "snapshots/snapshot_id=snap_test001/data/"
                    ),
                },
            ),
            patch(
                "silver.snapshots.pipeline.validate_recovery_snapshot",
                return_value={
                    "validation_status": "FAILED",
                    "row_count": 3,
                },
            ),
            patch(
                "silver.snapshots.pipeline.publish_snapshot"
            ) as mock_publish,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "Snapshot recovery validation failed",
            ):
                run_snapshot_pipeline(
                    conn=object(),
                    spark=object(),
                    client=object(),
                    bucket="test-bucket",
                    table_name="orders",
                )

        mock_publish.assert_not_called()


    def test_new_snapshot_success(self):
        from unittest.mock import MagicMock, patch

        batches = [{
            "table_name": "orders",
            "batch_id": "batch_001",
        }]

        data_prefix = (
            "silver_snapshots/orders/"
            "snapshots/snapshot_id=snap_test001/data/"
        )

        snapshot_df = MagicMock()
        snapshot_df.count.return_value = 3

        with (
            patch(
                "silver.snapshots.pipeline.prepare_snapshot_batches",
                return_value=batches,
            ),
            patch(
                "silver.snapshots.pipeline.generate_snapshot_id",
                return_value="snap_test001",
            ),
            patch(
                "silver.snapshots.pipeline.inspect_snapshot_state",
                return_value={"state": "NEW"},
            ),
            patch(
                "silver.snapshots.pipeline.build_silver_snapshot",
                return_value=snapshot_df,
            ) as mock_build,
            patch(
                "silver.snapshots.pipeline.write_snapshot_parquet",
                return_value={"data_prefix": data_prefix},
            ) as mock_write,
            patch(
                "silver.snapshots.pipeline.discover_snapshot_parquet_objects",
                return_value=[{
                    "object_key": data_prefix + "part-00000.parquet",
                    "size": 1024,
                }],
            ),
            patch(
                "silver.snapshots.pipeline.build_snapshot_metadata",
                return_value={"snapshot_id": "snap_test001"},
            ),
            patch(
                "silver.snapshots.pipeline.publish_snapshot",
                return_value={
                    "snapshot_id": "snap_test001",
                    "publication_status": "PUBLISHED",
                },
            ) as mock_publish,
        ):
            result = run_snapshot_pipeline(
                conn=object(),
                spark=object(),
                client=object(),
                bucket="test-bucket",
                table_name="orders",
            )

        self.assertEqual(
            result["publication_status"],
            "PUBLISHED",
        )

        self.assertEqual(
            snapshot_df.count.call_count,
            1,
        )

        mock_build.assert_called_once()
        mock_write.assert_called_once()
        mock_publish.assert_called_once()



    def test_new_snapshot_build_failure(self):
        batches = [{
            "table_name": "orders",
            "batch_id": "batch_001",
        }]

        with (
            patch(
                "silver.snapshots.pipeline.prepare_snapshot_batches",
                return_value=batches,
            ),
            patch(
                "silver.snapshots.pipeline.generate_snapshot_id",
                return_value="snap_test001",
            ),
            patch(
                "silver.snapshots.pipeline.inspect_snapshot_state",
                return_value={"state": "NEW"},
            ),
            patch(
                "silver.snapshots.pipeline.build_silver_snapshot",
                side_effect=RuntimeError("Snapshot build failed"),
            ),
            patch(
                "silver.snapshots.pipeline.write_snapshot_parquet",
            ) as mock_write,
            patch(
                "silver.snapshots.pipeline.publish_snapshot",
            ) as mock_publish,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "Snapshot build failed",
            ):
                run_snapshot_pipeline(
                    conn=object(),
                    spark=object(),
                    client=object(),
                    bucket="test-bucket",
                    table_name="orders",
                )

        mock_write.assert_not_called()
        mock_publish.assert_not_called()



    def test_new_snapshot_write_failure(self):
        from unittest.mock import MagicMock

        batches = [{
            "table_name": "orders",
            "batch_id": "batch_001",
        }]

        snapshot_df = MagicMock()
        snapshot_df.count.return_value = 3

        with (
            patch(
                "silver.snapshots.pipeline.prepare_snapshot_batches",
                return_value=batches,
            ),
            patch(
                "silver.snapshots.pipeline.generate_snapshot_id",
                return_value="snap_test001",
            ),
            patch(
                "silver.snapshots.pipeline.inspect_snapshot_state",
                return_value={"state": "NEW"},
            ),
            patch(
                "silver.snapshots.pipeline.build_silver_snapshot",
                return_value=snapshot_df,
            ),
            patch(
                "silver.snapshots.pipeline.write_snapshot_parquet",
                side_effect=RuntimeError("Snapshot write failed"),
            ),
            patch(
                "silver.snapshots.pipeline.publish_snapshot",
            ) as mock_publish,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "Snapshot write failed",
            ):
                run_snapshot_pipeline(
                    conn=object(),
                    spark=object(),
                    client=object(),
                    bucket="test-bucket",
                    table_name="orders",
                )

        mock_publish.assert_not_called()



    def test_new_snapshot_discovery_failure(self):
        from unittest.mock import MagicMock

        batches = [{
            "table_name": "orders",
            "batch_id": "batch_001",
        }]

        snapshot_df = MagicMock()
        snapshot_df.count.return_value = 3

        data_prefix = (
            "silver_snapshots/orders/"
            "snapshots/snapshot_id=snap_test001/data/"
        )

        with (
            patch(
                "silver.snapshots.pipeline.prepare_snapshot_batches",
                return_value=batches,
            ),
            patch(
                "silver.snapshots.pipeline.generate_snapshot_id",
                return_value="snap_test001",
            ),
            patch(
                "silver.snapshots.pipeline.inspect_snapshot_state",
                return_value={"state": "NEW"},
            ),
            patch(
                "silver.snapshots.pipeline.build_silver_snapshot",
                return_value=snapshot_df,
            ),
            patch(
                "silver.snapshots.pipeline.write_snapshot_parquet",
                return_value={"data_prefix": data_prefix},
            ) as mock_write,
            patch(
                "silver.snapshots.pipeline.discover_snapshot_parquet_objects",
                side_effect=RuntimeError(
                    "Snapshot object discovery failed"
                ),
            ),
            patch(
                "silver.snapshots.pipeline.build_snapshot_metadata",
            ) as mock_metadata,
            patch(
                "silver.snapshots.pipeline.publish_snapshot",
            ) as mock_publish,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "Snapshot object discovery failed",
            ):
                run_snapshot_pipeline(
                    conn=object(),
                    spark=object(),
                    client=object(),
                    bucket="test-bucket",
                    table_name="orders",
                )

        mock_write.assert_called_once()
        mock_metadata.assert_not_called()
        mock_publish.assert_not_called()



    def test_new_snapshot_publication_failure(self):
        from unittest.mock import MagicMock

        batches = [{
            "table_name": "orders",
            "batch_id": "batch_001",
        }]

        snapshot_df = MagicMock()
        snapshot_df.count.return_value = 3

        data_prefix = (
            "silver_snapshots/orders/"
            "snapshots/snapshot_id=snap_test001/data/"
        )

        manifest = {
            "snapshot_id": "snap_test001",
            "table_name": "orders",
        }

        with (
            patch(
                "silver.snapshots.pipeline.prepare_snapshot_batches",
                return_value=batches,
            ),
            patch(
                "silver.snapshots.pipeline.generate_snapshot_id",
                return_value="snap_test001",
            ),
            patch(
                "silver.snapshots.pipeline.inspect_snapshot_state",
                return_value={"state": "NEW"},
            ),
            patch(
                "silver.snapshots.pipeline.build_silver_snapshot",
                return_value=snapshot_df,
            ),
            patch(
                "silver.snapshots.pipeline.write_snapshot_parquet",
                return_value={"data_prefix": data_prefix},
            ) as mock_write,
            patch(
                "silver.snapshots.pipeline.discover_snapshot_parquet_objects",
                return_value=[{
                    "object_key": data_prefix + "part-00000.parquet",
                    "size": 1024,
                }],
            ),
            patch(
                "silver.snapshots.pipeline.build_snapshot_metadata",
                return_value=manifest,
            ),
            patch(
                "silver.snapshots.pipeline.publish_snapshot",
                side_effect=RuntimeError(
                    "Snapshot publication failed"
                ),
            ) as mock_publish,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "Snapshot publication failed",
            ):
                run_snapshot_pipeline(
                    conn=object(),
                    spark=object(),
                    client=object(),
                    bucket="test-bucket",
                    table_name="orders",
                )

        mock_write.assert_called_once()
        mock_publish.assert_called_once()


    def test_new_snapshot_verification_failure(self):
        from unittest.mock import MagicMock

        batches = [{
            "table_name": "orders",
            "batch_id": "batch_001",
        }]

        snapshot_df = MagicMock()
        snapshot_df.count.return_value = 3

        data_prefix = (
            "silver_snapshots/orders/"
            "snapshots/snapshot_id=snap_test001/data/"
        )

        manifest = {
            "snapshot_id": "snap_test001",
            "table_name": "orders",
        }

        with (
            patch(
                "silver.snapshots.pipeline.prepare_snapshot_batches",
                return_value=batches,
            ),
            patch(
                "silver.snapshots.pipeline.generate_snapshot_id",
                return_value="snap_test001",
            ),
            patch(
                "silver.snapshots.pipeline.inspect_snapshot_state",
                return_value={"state": "NEW"},
            ),
            patch(
                "silver.snapshots.pipeline.build_silver_snapshot",
                return_value=snapshot_df,
            ),
            patch(
                "silver.snapshots.pipeline.write_snapshot_parquet",
                return_value={"data_prefix": data_prefix},
            ) as mock_write,
            patch(
                "silver.snapshots.pipeline.discover_snapshot_parquet_objects",
                return_value=[{
                    "object_key": data_prefix + "part-00000.parquet",
                    "size": 1024,
                }],
            ),
            patch(
                "silver.snapshots.pipeline.build_snapshot_metadata",
                return_value=manifest,
            ),
            patch(
                "silver.snapshots.publisher.verify_snapshot",
                side_effect=ValueError(
                    "Snapshot row count mismatch"
                ),
            ) as mock_verify,
            patch(
                "silver.snapshots.publisher.publish_manifest",
            ) as mock_publish_manifest,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "Snapshot row count mismatch",
            ):
                run_snapshot_pipeline(
                    conn=object(),
                    spark=object(),
                    client=object(),
                    bucket="test-bucket",
                    table_name="orders",
                )

        mock_write.assert_called_once()
        mock_verify.assert_called_once()
        mock_publish_manifest.assert_not_called()



if __name__ == "__main__":
    unittest.main()