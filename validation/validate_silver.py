from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pathlib import Path
import yaml

CONTRACT_PATH = Path(__file__).parent / "contracts" / "silver.yml"


def load_contract(path=CONTRACT_PATH):
    with path.open(encoding="utf-8") as file:
        contract = yaml.safe_load(file)

    if not contract or "tables" not in contract:
        raise ValueError("Invalid contract: missing 'tables'.")

    return contract["tables"]


def validate_primary_key(df, pk_column):
    """Ensure ``pk_column`` is non-null and unique in a PySpark DataFrame.

    Raises:
        ValueError: If the key column is missing, contains NULLs, or contains
            duplicate values.
    """
    if pk_column not in df.columns:
        raise ValueError(f"Primary key column '{pk_column}' does not exist.")

    null_count = df.filter(col(pk_column).isNull()).count()
    if null_count:
        raise ValueError(
            f"Primary key column '{pk_column}' contains {null_count} NULL value(s)."
        )

    total_count = df.count()
    distinct_count = df.select(pk_column).distinct().count()
    if total_count != distinct_count:
        duplicate_count = total_count - distinct_count
        raise ValueError(
            f"Primary key column '{pk_column}' contains {duplicate_count} "
            "duplicate value(s)."
        )

def validate_foreign_key(child_df, parent_df, fk_column, pk_column):
    """Ensure every foreign-key value in ``child_df`` exists in ``parent_df``.

    Raises:
        ValueError: If either key column is missing or one or more child rows
            have no corresponding parent primary-key value.
    """
    if fk_column not in child_df.columns:
        raise ValueError(f"Foreign key column '{fk_column}' does not exist in child DataFrame.")
    if pk_column not in parent_df.columns:
        raise ValueError(f"Primary key column '{pk_column}' does not exist in parent DataFrame.")

    parent_keys = parent_df.select(pk_column).distinct()
    orphan_rows = child_df.join(
        parent_keys,
        child_df[fk_column] == parent_keys[pk_column],
        "left_anti",
    )
    orphan_count = orphan_rows.count()

    if orphan_count:
        raise ValueError(
            f"Foreign key column '{fk_column}' contains {orphan_count} value(s) "
            f"that do not exist in parent primary key column '{pk_column}'."
        )

def validate_not_null(df, columns):
    """Ensure one or more DataFrame columns contain no NULL values.

    ``columns`` may be a single column name or a list/tuple of names.

    Raises:
        ValueError: If a requested column is missing or contains NULL values.
    """
    if isinstance(columns, str):
        columns = [columns]
    else:
        columns = list(columns)

    if not columns:
        raise ValueError("At least one column name must be provided.")

    for column_name in columns:
        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' does not exist.")

        null_count = df.filter(col(column_name).isNull()).count()
        if null_count:
            raise ValueError(
                f"Column '{column_name}' contains {null_count} NULL value(s)."
            )

def validate_non_negative(df, columns):
    """Ensure one or more DataFrame columns contain only non-negative values.

    ``columns`` may be a single column name or a list/tuple of names.

    Raises:
        ValueError: If a requested column is missing or contains negative values.
    """
    if isinstance(columns, str):
        columns = [columns]
    else:
        columns = list(columns)

    if not columns:
        raise ValueError("At least one column name must be provided.")

    for column_name in columns:
        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' does not exist.")

        negative_count = df.filter(col(column_name) < 0).count()
        if negative_count:
            raise ValueError(
                f"Column '{column_name}' contains {negative_count} negative value(s)."
            )

def validate_positive(df, columns):
    """Ensure one or more DataFrame columns contain only positive values.

    ``columns`` may be a single column name or a list/tuple of names.

    Raises:
        ValueError: If a requested column is missing or contains non-positive values.
    """
    if isinstance(columns, str):
        columns = [columns]
    else:
        columns = list(columns)

    if not columns:
        raise ValueError("At least one column name must be provided.")

    for column_name in columns:
        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' does not exist.")

        non_positive_count = df.filter(col(column_name) <= 0).count()
        if non_positive_count:
            raise ValueError(
                f"Column '{column_name}' contains {non_positive_count} non-positive value(s)."
            )
def validate_unique(df, columns):
    """Ensure non-NULL values in one or more columns are unique.

    ``columns`` may be a single column name or a list/tuple of names. NULL
    values are allowed and excluded from the uniqueness check.

    Raises:
        ValueError: If a requested column is missing or contains duplicate values.
    """
    if isinstance(columns, str):
        columns = [columns]
    else:
        columns = list(columns)

    if not columns:
        raise ValueError("At least one column name must be provided.")

    for column_name in columns:
        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' does not exist.")
        non_null_df = df.filter(col(column_name).isNotNull())
        total_count = non_null_df.count()
        distinct_count = non_null_df.select(column_name).distinct().count()
        if total_count != distinct_count:
            duplicate_count = total_count - distinct_count
            raise ValueError(
                f"Column '{column_name}' contains {duplicate_count} duplicate value(s)."
            )


def print_summary(table_count, passed_checks, failed_checks):
    """Print the final Silver data-quality validation summary."""
    status = "PASS" if failed_checks == 0 else "FAIL"
    print("\n========== SILVER DATA QUALITY ==========")
    print(f"Tables validated : {table_count}")
    print(f"Checks passed     : {passed_checks}")
    print(f"Checks failed     : {failed_checks}")
    print(f"Status            : {status}")
    print("=========================================")


def main():
    from transformation.bronze_to_silver import configure_s3
    from transformation.config import BRONZE_BUCKET

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("ValidateSilver")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    passed_checks = 0
    failed_checks = 0

    try:
        configure_s3(spark)

        contract = load_contract()

        def run_check(description, validator, *args):
            nonlocal passed_checks, failed_checks

            try:
                validator(*args)
            except ValueError as error:
                failed_checks += 1
                print(f"{description}: FAIL - {error}")
            else:
                passed_checks += 1

        tables = {}
        for table_name in contract:
            path = f"s3a://{BRONZE_BUCKET}/silver/{table_name}/"

            try:
                table_df = spark.read.parquet(path)
                # Spark reads lazily; trigger a small action to verify that the
                # path, credentials, and Parquet data are readable.
                table_df.limit(1).count()
            except Exception as error:
                failed_checks += 1
                print(f"READ {table_name}: FAIL - {error}")
            else:
                tables[table_name] = table_df
                passed_checks += 1
                print(f"READ {table_name}: PASS")

        if failed_checks:
            print_summary(len(contract), passed_checks, failed_checks)
            return 1

        for table_name, rules in contract.items():
            df = tables[table_name]

            run_check(
                f"PK {table_name}",
                validate_primary_key,
                df,
                rules["primary_key"],
            )

            if columns := rules.get("not_null"):
                run_check(
                    f"NOT NULL {table_name} ({', '.join(columns)})",
                    validate_not_null,
                    df,
                    columns,
                )

            if columns := rules.get("positive"):
                run_check(
                    f"POSITIVE {table_name} ({', '.join(columns)})",
                    validate_positive,
                    df,
                    columns,
                )

            if columns := rules.get("non_negative"):
                run_check(
                    f"NON-NEGATIVE {table_name} ({', '.join(columns)})",
                    validate_non_negative,
                    df,
                    columns,
                )

            if columns := rules.get("unique"):
                run_check(
                    f"UNIQUE {table_name} ({', '.join(columns)})",
                    validate_unique,
                    df,
                    columns,
                )

        for child_table, rules in contract.items():
            for fk in rules.get("foreign_keys", []):
                parent = fk["references"]

                run_check(
                    f"FK {child_table}.{fk['column']} -> "
                    f"{parent['table']}.{parent['column']}",
                    validate_foreign_key,
                    tables[child_table],
                    tables[parent["table"]],
                    fk["column"],
                    parent["column"],
                )

        print_summary(len(contract), passed_checks, failed_checks)
        return 1 if failed_checks else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
