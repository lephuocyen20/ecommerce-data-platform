"""CSV serialization, checksums, and local staging."""

import csv
import hashlib
import io
import os
import tempfile
import uuid
from pathlib import Path

try:
    from .config import VALID_CHECKSUM, get_table_config, validate_batch_id
    from .minio_storage import build_object_key
except ImportError:
    from config import VALID_CHECKSUM, get_table_config, validate_batch_id
    from minio_storage import build_object_key


def records_to_csv(columns, rows):
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    writer.writerow(columns)
    writer.writerows(rows)
    return buffer.getvalue()


def generate_batch_id():
    return uuid.uuid4().hex


def prepare_batch_metadata(table_name, watermark, batch, csv_content):
    config = get_table_config(table_name)
    if batch["table_name"] != table_name:
        raise ValueError("Batch table mismatch")
    if batch["row_count"] <= 0:
        raise ValueError("Cannot register an empty batch")
    if batch["row_count"] != len(batch["rows"]):
        raise ValueError("Batch row count mismatch")
    if watermark["watermark_type"] != config["watermark_type"]:
        raise ValueError("Watermark type mismatch")

    columns = batch["columns"]
    if config["primary_key"] not in columns:
        raise ValueError("Primary key column missing")
    if config["watermark_column"] not in columns:
        raise ValueError("Watermark column missing")

    if csv_content != records_to_csv(columns, batch["rows"]):
        raise ValueError("CSV content does not match batch")

    last_row = dict(zip(columns, batch["rows"][-1]))
    batch_id = generate_batch_id()
    object_key = build_object_key(table_name, batch_id)
    checksum = hashlib.sha256(csv_content.encode("utf-8")).hexdigest()

    return {
        "batch_id": batch_id,
        "table_name": table_name,
        "watermark_type": config["watermark_type"],
        "start_timestamp": watermark["last_timestamp"],
        "start_id": watermark["last_id"],
        "end_timestamp": (
            last_row[config["watermark_column"]]
            if config["watermark_type"] == "TIMESTAMP"
            else None
        ),
        "end_id": last_row[config["primary_key"]],
        "row_count": batch["row_count"],
        "object_key": object_key,
        "checksum_sha256": checksum,
        "status": "PENDING",
    }


def stage_batch_csv(metadata, csv_content, staging_root="staging"):
    """Stage CSV using a temporary file and no-overwrite publish."""
    batch_id = metadata["batch_id"]
    table_name = metadata["table_name"]
    get_table_config(table_name)
    validate_batch_id(batch_id)
    if not isinstance(csv_content, str) or not csv_content:
        raise ValueError("Empty CSV content")

    csv_bytes = csv_content.encode("utf-8")
    expected_checksum = metadata["checksum_sha256"]
    if hashlib.sha256(csv_bytes).hexdigest() != expected_checksum:
        raise ValueError("CSV checksum mismatch")

    staging_dir = Path(staging_root) / table_name / f"batch_id={batch_id}"
    staging_dir.mkdir(parents=True, exist_ok=True)
    final_path = staging_dir / "data.csv"

    def verify_existing():
        existing_checksum = hashlib.sha256(final_path.read_bytes()).hexdigest()
        if existing_checksum != expected_checksum:
            raise RuntimeError("Existing staged CSV has a different checksum")

    if final_path.exists():
        verify_existing()
        return str(final_path)

    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=staging_dir, prefix=".tmp-", suffix=".csv", delete=False
        ) as temp_file:
            temp_path = Path(temp_file.name)
            temp_file.write(csv_bytes)
            temp_file.flush()
            os.fsync(temp_file.fileno())
        try:
            os.link(temp_path, final_path)
        except FileExistsError:
            verify_existing()
        return str(final_path)
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def load_staged_batch(metadata, staging_root="staging"):
    """Load an existing staged CSV and verify its SHA-256."""
    batch_id = metadata["batch_id"]
    table_name = metadata["table_name"]
    get_table_config(table_name)
    validate_batch_id(batch_id)
    expected_checksum = metadata["checksum_sha256"]
    if not isinstance(expected_checksum, str) or not VALID_CHECKSUM.fullmatch(
        expected_checksum
    ):
        raise ValueError("Invalid checksum")

    staged_path = Path(staging_root) / table_name / f"batch_id={batch_id}" / "data.csv"
    if not staged_path.is_file():
        raise FileNotFoundError(f"Staged CSV not found: {staged_path}")
    csv_bytes = staged_path.read_bytes()
    if hashlib.sha256(csv_bytes).hexdigest() != expected_checksum:
        raise RuntimeError(f"Staged CSV checksum mismatch: {staged_path}")
    return csv_bytes

__all__ = [
    "generate_batch_id",
    "load_staged_batch",
    "prepare_batch_metadata",
    "records_to_csv",
    "stage_batch_csv",
]
