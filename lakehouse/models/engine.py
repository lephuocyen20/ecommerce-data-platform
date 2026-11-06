"""Shared Spark preparation and merge primitives driven by ``TableSpec``."""
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def prepare(source, spec, batch_id, ingested_at=None):
    missing = set(spec.columns) - set(source.columns)
    if missing or not batch_id: raise ValueError(f"{spec.name} source contract invalid: {sorted(missing)}")
    frame = source.select(*[F.col(c).cast(spec.casts[c]).alias(c) for c in spec.columns])
    condition = F.col(spec.key).isNotNull()
    for column in spec.required: condition = condition & F.col(column).isNotNull()
    for predicate in spec.predicates: condition = condition & F.expr(predicate)
    if spec.status and spec.allowed_statuses: condition = condition & F.col(spec.status).isin(*spec.allowed_statuses)
    frame = frame.filter(condition)
    hashed = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in spec.columns]), 256)
    frame = frame.withColumn("_batch_id", F.lit(batch_id)).withColumn("_ingested_at", F.current_timestamp() if ingested_at is None else F.lit(ingested_at).cast("timestamp")).withColumn("_record_hash", hashed)
    audit = spec.audit_column or "_source_updated_at"
    if spec.watermark: frame = frame.withColumn(audit, F.col(spec.watermark))
    order = F.col(audit).desc() if spec.watermark else F.col("_record_hash").desc()
    return frame.withColumn("_rank", F.row_number().over(Window.partitionBy(spec.key).orderBy(order, F.col("_record_hash").desc()))).filter("_rank = 1").drop("_rank")

def ensure_table(spark, spec, table=None):
    extras = "_batch_id STRING, _ingested_at TIMESTAMP, _record_hash STRING" + (f", {spec.audit_column or '_source_updated_at'} TIMESTAMP" if spec.watermark else "")
    target = table or f"lakehouse.silver.{spec.name}"; spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {'.'.join(target.split('.')[:-1])}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({spec.ddl}, {extras}) USING iceberg")

def merge(spark, prepared, spec, table=None):
    table, staged = table or f"lakehouse.silver.{spec.name}", f"lakehouse_staged_{spec.name}"; ensure_table(spark, spec, table); prepared.createOrReplaceTempView(staged)
    if spec.immutable:
        if spark.sql(f"SELECT count(*) n FROM {staged} s JOIN {table} t ON s.{spec.key}=t.{spec.key} WHERE s._record_hash <> t._record_hash").first().n: raise ValueError(f"{spec.name} immutable replay conflict")
        spark.sql(f"MERGE INTO {table} t USING {staged} s ON t.{spec.key}=s.{spec.key} WHEN NOT MATCHED THEN INSERT *")
    else:
        audit = spec.audit_column or "_source_updated_at"; spark.sql(f"MERGE INTO {table} t USING {staged} s ON t.{spec.key}=s.{spec.key} WHEN MATCHED AND s.{audit} > t.{audit} THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
