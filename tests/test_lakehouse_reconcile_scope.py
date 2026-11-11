from lakehouse.migration.reconcile import read_iceberg_snapshot, scope_target_to_source_keys


class _Frame:
    def __init__(self, rows):
        self.rows = rows

    def select(self, key):
        return _Frame([{key: row[key]} for row in self.rows])

    def distinct(self):
        seen = set()
        rows = []
        for row in self.rows:
            marker = tuple(sorted(row.items()))
            if marker not in seen:
                seen.add(marker)
                rows.append(row)
        return _Frame(rows)

    def join(self, other, key, how):
        assert how == "left_semi"
        keys = {row[key] for row in other.rows}
        return _Frame([row for row in self.rows if row[key] in keys])


class _Reader:
    def __init__(self, spark):
        self.spark = spark

    def format(self, value):
        assert value == "iceberg"
        return self

    def option(self, name, value):
        self.spark.options[name] = value
        return self

    def load(self, identifier):
        self.spark.loaded_identifier = identifier
        return self.spark.snapshots[self.spark.options["snapshot-id"]]


class _Spark:
    def __init__(self):
        self.options = {}
        self.loaded_identifier = None
        self.snapshots = {"41": {"rows": 27}, "42": {"rows": 6316}}
        self.read = _Reader(self)

    def table(self, identifier):
        return self.snapshots["42"]


def test_reconcile_scopes_historical_iceberg_head_to_manifest_keys():
    source = _Frame([{"order_item_id": value} for value in range(27)])
    historical_iceberg = _Frame([{"order_item_id": value} for value in range(6316)])

    scoped = scope_target_to_source_keys(historical_iceberg, source, "order_item_id")

    assert len(scoped.rows) == 27
    assert {row["order_item_id"] for row in scoped.rows} == set(range(27))


def test_reconcile_reads_authorized_snapshot_not_newer_table_head():
    spark = _Spark()

    result = read_iceberg_snapshot(spark, "lakehouse.silver.order_items", 41)

    assert result == {"rows": 27}
    assert spark.loaded_identifier == "lakehouse.silver.order_items"
    assert spark.options == {"snapshot-id": "41"}
