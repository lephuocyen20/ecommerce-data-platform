"""Optimized Silver data-quality validation.

This version reads the same YAML contract as ``validate_silver.py`` but
collects all single-table validation metrics in one Spark aggregation per
table. Foreign-key checks still require a join and run separately.
"""

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

try:
    from validation.validate_silver import (
        load_contract,
        print_summary,
        validate_foreign_key,
    )
except ModuleNotFoundError:  # Supports: python validation/validate_silver_optimized.py
    from validate_silver import load_contract, print_summary, validate_foreign_key


SINGLE_TABLE_RULES = ("not_null", "positive", "non_negative", "unique")


def configured_columns(rules):
    """Return the columns needed for single-table rules, grouped by rule."""
    return {
        "primary_key": [rules["primary_key"]],
        **{rule: rules.get(rule, []) for rule in SINGLE_TABLE_RULES},
    }


def schema_error(df, expected_schema):
    """Return a contract-schema error, or None when required fields match."""
    actual_schema = {
        field.name: field.dataType.simpleString() for field in df.schema.fields
    }
    missing_columns = [
        column_name
        for column_name in expected_schema
        if column_name not in actual_schema
    ]
    type_mismatches = [
        f"{column_name}: expected {expected_type}, got {actual_schema[column_name]}"
        for column_name, expected_type in expected_schema.items()
        if column_name in actual_schema
        and actual_schema[column_name].lower() != expected_type.lower()
    ]

    details = []
    if missing_columns:
        details.append(f"missing column(s): {', '.join(missing_columns)}")
    if type_mismatches:
        details.append(f"type mismatch(es): {'; '.join(type_mismatches)}")
    return "; ".join(details) if details else None


def collect_table_metrics(df, rules):
    """Collect all single-table validation metrics with one Spark action.

    Columns absent from the DataFrame are omitted here and are reported by
    ``validate_table_rules`` as failed contract checks.
    """
    columns_by_rule = configured_columns(rules)
    existing_columns = set(df.columns)
    aggregations = [F.count("*").alias("__row_count")]
    aliases = {"__row_count"}

    def add(aggregation, alias):
        if alias not in aliases:
            aggregations.append(aggregation.alias(alias))
            aliases.add(alias)

    pk_column = rules["primary_key"]
    if pk_column in existing_columns:
        add(
            F.coalesce(
                F.sum(F.when(F.col(pk_column).isNull(), F.lit(1)).otherwise(F.lit(0))),
                F.lit(0),
            ),
            f"null__{pk_column}",
        )
        add(F.countDistinct(pk_column), f"distinct__{pk_column}")

    # Hashing prevents a metadata-only count optimization and forces Spark to
    # read values from every contract-defined field, including fields without
    # a separate quality rule.
    for column_name in rules.get("schema", {}):
        if column_name in existing_columns:
            add(F.max(F.xxhash64(F.col(column_name))), f"scan__{column_name}")

    for column_name in columns_by_rule["not_null"]:
        if column_name in existing_columns:
            add(
                F.coalesce(
                    F.sum(
                        F.when(F.col(column_name).isNull(), F.lit(1)).otherwise(
                            F.lit(0)
                        )
                    ),
                    F.lit(0),
                ),
                f"null__{column_name}",
            )

    for column_name in columns_by_rule["positive"]:
        if column_name in existing_columns:
            add(
                F.coalesce(
                    F.sum(
                        F.when(F.col(column_name) <= 0, F.lit(1)).otherwise(F.lit(0))
                    ),
                    F.lit(0),
                ),
                f"non_positive__{column_name}",
            )

    for column_name in columns_by_rule["non_negative"]:
        if column_name in existing_columns:
            add(
                F.coalesce(
                    F.sum(
                        F.when(F.col(column_name) < 0, F.lit(1)).otherwise(F.lit(0))
                    ),
                    F.lit(0),
                ),
                f"negative__{column_name}",
            )

    for column_name in columns_by_rule["unique"]:
        if column_name in existing_columns:
            add(F.count(column_name), f"non_null__{column_name}")
            add(F.countDistinct(column_name), f"distinct__{column_name}")

    return df.agg(*aggregations).first().asDict()


def validate_table_rules(df, table_name, rules, record_check):
    """Evaluate one table's contract using pre-collected metrics only."""
    metrics = collect_table_metrics(df, rules)
    record_check(f"SCHEMA {table_name}", schema_error(df, rules.get("schema", {})))
    existing_columns = set(df.columns)

    def missing(columns):
        return [column_name for column_name in columns if column_name not in existing_columns]

    pk_column = rules["primary_key"]
    if pk_column not in existing_columns:
        pk_error = f"Primary key column '{pk_column}' does not exist."
    elif metrics[f"null__{pk_column}"]:
        pk_error = (
            f"Primary key column '{pk_column}' contains "
            f"{metrics[f'null__{pk_column}']} NULL value(s)."
        )
    elif metrics["__row_count"] != metrics[f"distinct__{pk_column}"]:
        duplicate_count = metrics["__row_count"] - metrics[f"distinct__{pk_column}"]
        pk_error = (
            f"Primary key column '{pk_column}' contains {duplicate_count} "
            "duplicate value(s)."
        )
    else:
        pk_error = None
    record_check(f"PK {table_name}", pk_error)

    for rule_name, display_name, error_label, metric_prefix in (
        ("not_null", "NOT NULL", "NULL", "null"),
        ("positive", "POSITIVE", "non-positive", "non_positive"),
        ("non_negative", "NON-NEGATIVE", "negative", "negative"),
    ):
        columns = rules.get(rule_name)
        if not columns:
            continue

        absent_columns = missing(columns)
        if absent_columns:
            error = f"Column(s) do not exist: {', '.join(absent_columns)}."
        else:
            invalid_counts = {
                column_name: metrics[f"{metric_prefix}__{column_name}"]
                for column_name in columns
                if metrics[f"{metric_prefix}__{column_name}"]
            }
            error = None
            if invalid_counts:
                details = ", ".join(
                    f"{column_name}={count}" for column_name, count in invalid_counts.items()
                )
                error = f"{error_label} value count(s): {details}."

        record_check(f"{display_name} {table_name} ({', '.join(columns)})", error)

    unique_columns = rules.get("unique")
    if unique_columns:
        absent_columns = missing(unique_columns)
        if absent_columns:
            unique_error = f"Column(s) do not exist: {', '.join(absent_columns)}."
        else:
            duplicate_counts = {
                column_name: (
                    metrics[f"non_null__{column_name}"]
                    - metrics[f"distinct__{column_name}"]
                )
                for column_name in unique_columns
                if metrics[f"non_null__{column_name}"]
                != metrics[f"distinct__{column_name}"]
            }
            unique_error = None
            if duplicate_counts:
                details = ", ".join(
                    f"{column_name}={count}"
                    for column_name, count in duplicate_counts.items()
                )
                unique_error = f"Duplicate value count(s): {details}."

        record_check(f"UNIQUE {table_name} ({', '.join(unique_columns)})", unique_error)


def main():
    from transformation.bronze_to_silver import configure_s3
    from transformation.config import BRONZE_BUCKET

    spark = SparkSession.builder.master("local[2]").appName(
        "ValidateSilverOptimized"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    passed_checks = 0
    failed_checks = 0
    tables = {}

    try:
        configure_s3(spark)
        contract = load_contract()

        for table_name in contract:
            path = f"s3a://{BRONZE_BUCKET}/silver/{table_name}/"
            try:
                table_df = spark.read.parquet(path).persist(StorageLevel.MEMORY_AND_DISK)
            except Exception as error:
                failed_checks += 1
                print(f"READ {table_name}: FAIL - {error}")
            else:
                tables[table_name] = table_df

        if failed_checks:
            print_summary(len(contract), passed_checks, failed_checks)
            return 1

        def record_check(description, error=None):
            nonlocal passed_checks, failed_checks
            if error:
                failed_checks += 1
                print(f"{description}: FAIL - {error}")
            else:
                passed_checks += 1
                #print(f"{description}: PASS")

        validated_tables = set()
        for table_name, rules in contract.items():
            try:
                validate_table_rules(
                    tables[table_name], table_name, rules, record_check
                )
            except Exception as error:
                failed_checks += 1
                print(f"TABLE {table_name}: FAIL - unable to scan Parquet data: {error}")
            else:
                validated_tables.add(table_name)

        for child_table, rules in contract.items():
            for fk in rules.get("foreign_keys", []):
                parent = fk["references"]
                if (
                    child_table not in validated_tables
                    or parent["table"] not in validated_tables
                ):
                    print(
                        f"FK {child_table}.{fk['column']} -> "
                        f"{parent['table']}.{parent['column']}: "
                        "SKIPPED - source table unavailable"
                    )
                    continue
                try:
                    validate_foreign_key(
                        tables[child_table],
                        tables[parent["table"]],
                        fk["column"],
                        parent["column"],
                    )
                except ValueError as error:
                    record_check(
                        f"FK {child_table}.{fk['column']} -> "
                        f"{parent['table']}.{parent['column']}",
                        str(error),
                    )
                else:
                    record_check(
                        f"FK {child_table}.{fk['column']} -> "
                        f"{parent['table']}.{parent['column']}"
                    )

        print_summary(len(contract), passed_checks, failed_checks)
        return 1 if failed_checks else 0
    finally:
        for table_df in tables.values():
            table_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
