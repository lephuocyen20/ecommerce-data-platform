def validate_batch_continuity(batches):
    """Validate watermark continuity in source order."""
    if not batches:
        return True

    first = batches[0]
    watermark_type = first["watermark_type"]

    if first["start_id"] != 0:
        raise ValueError("Missing initial batch")

    if watermark_type == "TIMESTAMP":
        if first["start_timestamp"] is not None:
            raise ValueError("Invalid initial timestamp")
    elif watermark_type == "ID":
        if first["start_timestamp"] is not None:
            raise ValueError("Invalid ID watermark")
    else:
        raise ValueError("Unsupported watermark type")

    for previous, current in zip(batches, batches[1:]):
        if current["watermark_type"] != watermark_type:
            raise ValueError("Watermark type mismatch")

        if (
            current["start_timestamp"] != previous["end_timestamp"]
            or current["start_id"] != previous["end_id"]
        ):
            raise ValueError("Watermark continuity violation")

    for batch in batches:
        if batch["watermark_type"] == "ID":
            if batch["end_timestamp"] is not None:
                raise ValueError("Invalid ID timestamp")

            if batch["end_id"] <= batch["start_id"]:
                raise ValueError("Non-increasing ID watermark")

        else:
            start = (
                batch["start_timestamp"],
                batch["start_id"],
            )
            end = (
                batch["end_timestamp"],
                batch["end_id"],
            )

            if end[0] is None:
                raise ValueError("Missing end timestamp")

            if start[0] is not None and end <= start:
                raise ValueError("Non-increasing TIMESTAMP watermark")


    return True
  