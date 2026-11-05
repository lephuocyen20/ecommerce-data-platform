import unittest
from datetime import datetime
from silver.snapshots.continuity import validate_batch_continuity


class TestSnapshotContinuity(unittest.TestCase):

    def test_valid_batches(self):
        batches = [
            {
                "watermark_type": "ID",
                "start_timestamp": None,
                "end_timestamp": None,
                "start_id": 0,
                "end_id": 10,
            },
            {
                "watermark_type": "ID",
                "start_timestamp": None,
                "end_timestamp": None,
                "start_id": 10,
                "end_id": 20,
            },
        ]
        self.assertTrue(validate_batch_continuity(batches))

    def test_missing_batch(self):
        batches = [
            {
                "watermark_type": "ID",
                "start_timestamp": None,
                "end_timestamp": None,
                "start_id": 0,
                "end_id": 10,
            },
            {
                "watermark_type": "ID",
                "start_timestamp": None,
                "end_timestamp": None,
                "start_id": 15,
                "end_id": 20,
            },
        ]
        with self.assertRaises(ValueError):
            validate_batch_continuity(batches)



    def test_non_increasing_id(self):
        batch = {
            "watermark_type": "ID",
            "start_timestamp": None,
            "end_timestamp": None,
            "start_id": 10,
            "end_id": 10,
        }
        with self.assertRaises(ValueError):
            validate_batch_continuity([batch])

    def test_non_increasing_timestamp(self):
        batch = {
            "watermark_type": "TIMESTAMP",
            "start_timestamp": None,
            "end_timestamp": None,
            "start_id": 0,
            "end_id": 10,
        }
        with self.assertRaises(ValueError):
            validate_batch_continuity([batch])

    from datetime import datetime

# Thêm hàm này vào trong class
    def test_valid_timestamp_batches(self):
        t1 = datetime(2026, 10, 1, 10, 0)
        t2 = datetime(2026, 10, 1, 11, 0)

        batches = [
            {
                "watermark_type": "TIMESTAMP",
                "start_timestamp": None,
                "end_timestamp": t1,
                "start_id": 0,
                "end_id": 10,
            },
            {
                "watermark_type": "TIMESTAMP",
                "start_timestamp": t1,
                "end_timestamp": t2,
                "start_id": 10,
                "end_id": 20,
            },
        ]

        self.assertTrue(validate_batch_continuity(batches))