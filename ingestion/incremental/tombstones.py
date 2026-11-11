"""Source-backed delete contract for incremental entities."""

TOMBSTONE_COLUMN = "_is_deleted"
DELETE_TIMESTAMP_COLUMN = "deleted_at"
MUTABLE_TABLES = frozenset({"order_items", "payments"})


def validate_tombstone_columns(table_name, columns):
    if table_name in MUTABLE_TABLES:
        required = {TOMBSTONE_COLUMN, DELETE_TIMESTAMP_COLUMN, "updated_at"}
        missing = required.difference(columns)
        if missing:
            raise ValueError(
                f"{table_name} source contract missing tombstone columns: "
                f"{sorted(missing)}"
            )
    return True


def reject_physical_delete(delete_intent):
    """Reject hard-delete requests; source rows must carry a tombstone."""
    if delete_intent is True:
        raise ValueError("Physical deletes are not permitted; use source tombstones")
    return False


def validate_tombstone_state(is_deleted, deleted_at, updated_at=True):
    if updated_at is None:
        raise ValueError("tombstone state invalid: updated_at is required")
    if is_deleted is True and deleted_at is None:
        raise ValueError("tombstone state invalid: deleted source rows require deleted_at")
    if is_deleted is False and deleted_at is not None:
        raise ValueError("tombstone state invalid: non-deleted source rows cannot carry deleted_at")
    if not isinstance(is_deleted, bool):
        raise ValueError("_is_deleted must be boolean")
    return True


def validate_tombstone_dataframe(dataframe):
    from pyspark.sql import functions as F
    invalid = dataframe.where(
        (F.col(TOMBSTONE_COLUMN).isNull())
        | F.col("updated_at").isNull()
        | (F.col(TOMBSTONE_COLUMN) & F.col(DELETE_TIMESTAMP_COLUMN).isNull())
        | (~F.col(TOMBSTONE_COLUMN) & F.col(DELETE_TIMESTAMP_COLUMN).isNotNull())
    ).limit(1).count()
    if invalid:
        raise ValueError("Invalid source tombstone state")
    return True
