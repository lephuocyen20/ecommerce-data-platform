import pytest

from common.bounded_e2e_scope import BoundedE2EScopeError, build_bounded_e2e_scope, validate_bounded_e2e_scope
from common.paths import silver_incremental_batch_paths


def env():
    return {"LAKEHOUSE_E2E_APPROVED": "1", "LAKEHOUSE_E2E_TEST_BUCKET": "ecommerce-data-lake", "LAKEHOUSE_E2E_RUN_ID": "run-20261110", "LAKEHOUSE_E2E_PREFIX": "quality-it/run-20261110/"}


def test_accepts_exact_run_scoped_prefix_before_external_connection():
    assert validate_bounded_e2e_scope(env(), object_key="quality-it/run-20261110/bronze/a")["bucket"] == "ecommerce-data-lake"


@pytest.mark.parametrize("field,value", [("LAKEHOUSE_E2E_APPROVED", "0"), ("LAKEHOUSE_E2E_TEST_BUCKET", "other"), ("LAKEHOUSE_E2E_PREFIX", "quality-it/shared/"), ("LAKEHOUSE_E2E_RUN_ID", "../bad")])
def test_rejects_missing_or_mismatched_authorization(field, value):
    values = env(); values[field] = value
    with pytest.raises(BoundedE2EScopeError):
        validate_bounded_e2e_scope(values)


def test_rejects_object_outside_prefix():
    with pytest.raises(BoundedE2EScopeError):
        validate_bounded_e2e_scope(env(), object_key="lakehouse/gold/table")


def test_scope_derives_immutable_storage_and_catalog_names():
    scope = build_bounded_e2e_scope(env())
    assert scope.warehouse == "s3a://ecommerce-data-lake/quality-it/run-20261110/lakehouse"
    assert scope.nessie_ref == "quality_it_run_20261110"
    assert scope.gold_namespace == "gold_run_20261110"
    assert scope.silver_namespace == "silver_run_20261110"
    assert scope.key("gold/dimensional/graphs/v1/x.json").startswith(scope.prefix)


def test_scope_rejects_escape_key():
    scope = build_bounded_e2e_scope(env())
    with pytest.raises(BoundedE2EScopeError):
        scope.key("gold/../outside.json")


def test_scope_prefixes_silver_incremental_paths_without_changing_local_default():
    scope = build_bounded_e2e_scope(env())
    batch_id = "a" * 32
    bounded = silver_incremental_batch_paths("orders", batch_id, scope=scope)
    local = silver_incremental_batch_paths("orders", batch_id)
    assert bounded["manifest_key"] == f"{scope.prefix}silver_incremental/orders/batch_id={batch_id}/_BATCH_METADATA.json"
    assert local["manifest_key"] == f"silver_incremental/orders/batch_id={batch_id}/_BATCH_METADATA.json"
