"""Pure dependency policy for optional Iceberg branches.

The completion gate deliberately treats a successful no-op/skip branch as a
valid parent outcome while never masking a failed migration or quality check.

This module is imported while Airflow parses DAGs.  It must remain independent
of ``lakehouse`` because the Airflow image intentionally does not mount that
Spark-only package.  The unit test suite compares this graph to ``TableSpec``.
"""


PARENT_COMPLETION_TRIGGER_RULE = "none_failed_min_one_success"

# Keep this as data rather than importing Spark-side TableSpec at DAG parse
# time.  Update it together with TableSpec foreign_keys; the regression test
# enforces that compatibility in the repository test environment.
FOREIGN_KEY_PARENTS = {
    "orders": ("customers",),
    "order_items": ("orders", "products"),
    "payments": ("orders",),
    "shipments": ("orders",),
}


def foreign_key_parents():
    """Return a copy of the Airflow-safe, declarative FK parent graph."""
    return dict(FOREIGN_KEY_PARENTS)


def parent_completion_permits_child(upstream_states):
    """Model Airflow's ``none_failed_min_one_success`` completion contract."""
    states = tuple(upstream_states)
    failed = {"failed", "upstream_failed"}
    return bool(states) and not failed.intersection(states) and "success" in states
