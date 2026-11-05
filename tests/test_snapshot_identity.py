import unittest

from silver.snapshots.identity import generate_snapshot_id


class TestSnapshotIdentity(unittest.TestCase):

    def setUp(self):
        self.batches = [
            {"table_name": "orders", "batch_id": "batch_A"},
            {"table_name": "orders", "batch_id": "batch_B"},
        ]

    def test_deterministic_id(self):
        first = generate_snapshot_id("orders", self.batches)
        second = generate_snapshot_id("orders", self.batches)

        self.assertEqual(first, second)
        self.assertTrue(first.startswith("snap_"))

    def test_different_batches_produce_different_ids(self):
        first = generate_snapshot_id("orders", self.batches)

        changed = [
            {"table_name": "orders", "batch_id": "batch_A"},
            {"table_name": "orders", "batch_id": "batch_C"},
        ]

        second = generate_snapshot_id("orders", changed)

        self.assertNotEqual(first, second)

    def test_batch_order_changes_identity(self):
        first = generate_snapshot_id("orders", self.batches)
        second = generate_snapshot_id(
            "orders",
            list(reversed(self.batches)),
        )

        self.assertNotEqual(first, second)

    def test_reject_empty_batches(self):
        with self.assertRaisesRegex(
            ValueError,
            "without batches",
        ):
            generate_snapshot_id("orders", [])

    def test_reject_mixed_tables(self):
        batches = [
            {"table_name": "orders", "batch_id": "batch_A"},
            {"table_name": "customers", "batch_id": "batch_B"},
        ]

        with self.assertRaisesRegex(
            ValueError,
            "different tables",
        ):
            generate_snapshot_id("orders", batches)

    def test_reject_duplicate_batches(self):
        batches = [
            {"table_name": "orders", "batch_id": "batch_A"},
            {"table_name": "orders", "batch_id": "batch_A"},
        ]

        with self.assertRaisesRegex(
            ValueError,
            "Duplicate batch IDs",
        ):
            generate_snapshot_id("orders", batches)

    def test_reject_invalid_batch_id(self):
        batches = [
            {"table_name": "orders", "batch_id": ""},
        ]

        with self.assertRaisesRegex(
            ValueError,
            "Invalid batch ID",
        ):
            generate_snapshot_id("orders", batches)


if __name__ == "__main__":
    unittest.main()