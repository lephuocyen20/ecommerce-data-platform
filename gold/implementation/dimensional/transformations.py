"""Pure Spark transformations for Type-1 dimensions and order-grain fact."""
from pyspark.sql import functions as F
from pyspark.sql import Window
from .contracts import UNKNOWN_KEY

def build_dim_date(spark, start_date, end_date):
    dates = spark.sql("SELECT explode(sequence(to_date(:start), to_date(:end), interval 1 day)) AS calendar_date", {"start": start_date, "end": end_date})
    return dates.select(F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"), "calendar_date", F.year("calendar_date").alias("year"), F.month("calendar_date").alias("month"), F.weekofyear("calendar_date").alias("iso_week"))

def _type1(source, natural_key, key_name, attributes, updated_at="updated_at"):
    columns = [natural_key, *attributes]
    # Latest source version wins; ties have a stable full-row hash tie-breaker.
    ordering = [F.col(updated_at).desc_nulls_last(), F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in columns]), 256).desc()]
    base = source.select(*[F.col(c) for c in columns]).withColumn("_rn", F.row_number().over(Window.partitionBy(natural_key).orderBy(*ordering))).where("_rn = 1").drop("_rn")
    rows = base.withColumn(key_name, F.col(natural_key).cast("long"))
    unknown = rows.limit(0)
    for name in [key_name, natural_key, *attributes]:
        value = F.lit(UNKNOWN_KEY) if name == key_name else F.lit(None).cast(rows.schema[name].dataType)
        unknown = unknown.withColumn(name, value)
    return rows.select(key_name, natural_key, *attributes).unionByName(unknown)

def build_dim_customer(customers): return _type1(customers, "customer_id", "customer_key", ("full_name", "email", "created_at", "updated_at"))
def build_dim_product(products): return _type1(products, "product_id", "product_key", ("product_name", "stock_quantity", "price", "created_at", "updated_at"))

def build_fact_orders(orders, order_items, dim_customer):
    items = order_items.groupBy("order_id").agg(F.count("*").alias("line_count"), F.sum("quantity").cast("long").alias("item_count"), F.sum(F.col("quantity") * F.col("unit_price")).alias("gross_amount"), F.sum("discount_amount").alias("discount_amount"))
    customers = dim_customer.where(F.col("customer_key") != UNKNOWN_KEY).select("customer_id", "customer_key")
    return orders.join(items, "order_id", "left").join(customers, "customer_id", "left").select(
        "order_id", F.date_format("order_date", "yyyyMMdd").cast("int").alias("order_date_key"), F.coalesce("customer_key", F.lit(UNKNOWN_KEY)).alias("customer_key"), F.col("customer_id").alias("customer_natural_key"), F.col("status").alias("order_status"),
        F.coalesce("line_count", F.lit(0)).cast("long").alias("line_count"), F.coalesce("item_count", F.lit(0)).cast("long").alias("item_count"), F.coalesce("gross_amount", F.lit(0)).alias("gross_amount"), F.coalesce("discount_amount", F.lit(0)).alias("discount_amount"), (F.coalesce("gross_amount", F.lit(0)) - F.coalesce("discount_amount", F.lit(0))).alias("net_amount"))
