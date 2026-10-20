import hashlib

import pytest

from ingestion.incremental.staging import (
    load_staged_batch,
    stage_batch_csv,
)


def make_metadata(csv_content, batch_id="a" * 32):
    return {
        "batch_id": batch_id,
        "table_name": "customers",
        "checksum_sha256": hashlib.sha256(
            csv_content.encode("utf-8")
        ).hexdigest(),
    }


def test_stage_and_load_csv(tmp_path):
    content = "customer_id,name\n1,Alice\n"
    metadata = make_metadata(content)

    path = stage_batch_csv(
        metadata, content, staging_root=tmp_path
    )

    assert load_staged_batch(
        metadata, staging_root=tmp_path
    ) == content.encode("utf-8")

    assert path.endswith("data.csv")


def test_stage_same_batch_same_checksum(tmp_path):
    content = "customer_id,name\n1,Alice\n"
    metadata = make_metadata(content)

    first_path = stage_batch_csv(
        metadata, content, staging_root=tmp_path
    )
    second_path = stage_batch_csv(
        metadata, content, staging_root=tmp_path
    )

    assert first_path == second_path
    assert load_staged_batch(
        metadata, staging_root=tmp_path
    ) == content.encode("utf-8")


def test_stage_rejects_existing_different_checksum(tmp_path):
    original = "customer_id,name\n1,Alice\n"
    replacement = "customer_id,name\n1,Bob\n"

    original_metadata = make_metadata(original)
    replacement_metadata = make_metadata(replacement)

    stage_batch_csv(
        original_metadata, original, staging_root=tmp_path
    )

    with pytest.raises(RuntimeError, match="different checksum"):
        stage_batch_csv(
            replacement_metadata,
            replacement,
            staging_root=tmp_path,
        )

    assert load_staged_batch(
        original_metadata, staging_root=tmp_path
    ) == original.encode("utf-8")


def test_load_rejects_tampered_csv(tmp_path):
    content = "customer_id,name\n1,Alice\n"
    metadata = make_metadata(content)

    path = stage_batch_csv(
        metadata, content, staging_root=tmp_path
    )

    from pathlib import Path

    Path(path).write_text(
        "customer_id,name\n1,Tampered\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="checksum mismatch"):
        load_staged_batch(metadata, staging_root=tmp_path)


def test_stage_rejects_metadata_checksum_mismatch(tmp_path):
    content = "customer_id,name\n1,Alice\n"
    metadata = make_metadata(content)

    metadata["checksum_sha256"] = "0" * 64

    with pytest.raises(ValueError, match="checksum mismatch"):
        stage_batch_csv(
            metadata, content, staging_root=tmp_path
        )
