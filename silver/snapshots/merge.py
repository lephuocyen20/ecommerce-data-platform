from pyspark.sql import functions as F
from pyspark.sql.window import Window


def merge_silver_batches(df, primary_key, watermark_column):
    """Merge records deterministically; reject ambiguous versions."""

    required_columns = {
        primary_key,
        watermark_column,
        "_batch_sequence",
    }

    missing = required_columns - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}"
        )

    if df.filter(
        F.col(primary_key).isNull()
        | F.col(watermark_column).isNull()
        | F.col("_batch_sequence").isNull()
    ).limit(1).count():
        raise ValueError("Null merge key or ordering column")

    conflicts = (
        df.groupBy(
            primary_key,
            watermark_column,
            "_batch_sequence",
        )
        .count()
        .filter(F.col("count") > 1)
    )

    if conflicts.limit(1).count():
        raise ValueError("Ambiguous duplicate record versions")

    window = Window.partitionBy(primary_key).orderBy(
        F.col(watermark_column).desc(),
        F.col("_batch_sequence").desc(),
    )

    return (
        df.withColumn(
            "_row_number",
            F.row_number().over(window),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )