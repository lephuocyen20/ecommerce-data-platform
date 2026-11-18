import pytest

from gold.implementation.dimensional import manifest
from gold.implementation.dimensional.manifest import DimensionalManifestError


GRAPH = {"identity": "g" * 64}
FENCE = {"version": 1, "kind": "gold_dimensional_global_fence", "graph_identity": GRAPH["identity"]}


def _coverage_valid_lineage():
    orders = {
        "batch_id": "a" * 32,
        "manifest_key": "silver/orders/manifest.json",
        "manifest_identity": "b" * 64,
        "coverage": {"runtime_window_id": "window-1", "coverage_identity": "orders-coverage"},
    }
    child_reference = {
        "table": "orders",
        "batch_id": orders["batch_id"],
        "manifest_key": orders["manifest_key"],
        "manifest_identity": orders["manifest_identity"],
    }
    sources = {"orders": orders}
    for table, identity in (("payments", "payments-coverage"), ("shipments", "shipments-coverage")):
        sources[table] = {
            "coverage": {
                "runtime_window_id": "window-1",
                "coverage_identity": identity,
                "parent_coverage_identity": orders["coverage"]["coverage_identity"],
                "parent_orders_reference": child_reference,
            }
        }
    return {"version": 1, "source_type": "iceberg_snapshot", "sources": sources}


def test_immutable_manifest_mismatch_is_rejected(monkeypatch):
    monkeypatch.setattr(manifest, "read_json_object", lambda *_: {"identity": "different"})
    with pytest.raises(DimensionalManifestError, match="Conflicting immutable manifest"):
        manifest.publish_manifest(object(), "ecommerce-lakehouse-quality-it", {"identity": "expected"}, "key")


def test_global_fence_release_refuses_non_owner(monkeypatch):
    monkeypatch.setattr(manifest, "read_json_object", lambda *_: {**FENCE, "graph_identity": "other"})
    with pytest.raises(DimensionalManifestError, match="another writer"):
        manifest.release_global_fence(object(), "ecommerce-lakehouse-quality-it", FENCE)


def test_global_fence_conflict_returns_recovery_metadata_and_does_not_release(monkeypatch):
    requested = {"identity": "r" * 64}
    holder = {"version": 1, "kind": "gold_dimensional_global_fence", "graph_identity": "h" * 64}
    removed = []

    class PreconditionFailed(Exception):
        code = "PreconditionFailed"

    monkeypatch.setattr(manifest, "put_json_object_if_absent",
                        lambda *args: (_ for _ in ()).throw(PreconditionFailed()))
    monkeypatch.setattr(manifest, "read_json_object", lambda *_: holder)
    client = type("Client", (), {"remove_object": lambda *args: removed.append(args)})()

    with pytest.raises(DimensionalManifestError) as caught:
        manifest.acquire_global_fence(client, "ecommerce-lakehouse-quality-it", requested)

    error = caught.value
    assert error.code == "GLOBAL_FENCE_CONFLICT"
    assert error.recovery_state == "GLOBAL_FENCE_RECOVERY_REQUIRED"
    assert error.lock_key == manifest.global_lock_key()
    assert error.graph_identity == requested["identity"]
    assert error.lock_metadata == holder
    assert f"holder_graph_identity={holder['graph_identity']}" in str(error)
    assert f"requested_graph_identity={requested['identity']}" in str(error)
    assert removed == []


def test_global_fence_conflict_is_fail_closed_for_unexpected_lock_metadata(monkeypatch):
    requested = {"identity": "r" * 64}

    class ConditionalConflict(Exception):
        code = "ConditionalRequestConflict"

    monkeypatch.setattr(manifest, "put_json_object_if_absent",
                        lambda *args: (_ for _ in ()).throw(ConditionalConflict()))
    monkeypatch.setattr(manifest, "read_json_object", lambda *_: {"kind": "unexpected-lock"})

    with pytest.raises(DimensionalManifestError) as caught:
        manifest.acquire_global_fence(object(), "ecommerce-lakehouse-quality-it", requested)

    error = caught.value
    assert error.code == "GLOBAL_FENCE_CONFLICT"
    assert error.recovery_state == "GLOBAL_FENCE_RECOVERY_REQUIRED"
    assert error.lock_metadata == {"kind": "unexpected-lock"}


def test_graph_fence_assertion_refuses_non_owner(monkeypatch):
    monkeypatch.setattr(manifest, "read_json_object", lambda *_: {"graph_identity": "other"})
    with pytest.raises(DimensionalManifestError, match="ownership"):
        manifest.assert_graph_fence(object(), "ecommerce-lakehouse-quality-it", GRAPH, {"graph_identity": "expected"})


def test_recovery_classifier_does_not_mutate_state(monkeypatch):
    import gold.implementation.dimensional.writer as writer

    calls = []
    monkeypatch.setattr(writer, "build_graph_manifest", lambda *args, **kwargs: {"identity": "g" * 64})
    monkeypatch.setattr(writer, "read_completed_graph", lambda *_: None)
    monkeypatch.setattr(writer, "read_table_manifests", lambda *_: {"dim_customer": {"table": "dim_customer"}})
    monkeypatch.setattr(writer, "read_dimensional_sources", lambda *_: calls.append("read"))
    result = writer.run_dimensional_graph(
        None,
        object(),
        "ecommerce-lakehouse-quality-it",
        _coverage_valid_lineage(),
        dim_date_start="2026-01-01",
        dim_date_end="2026-01-01",
    )
    assert result["status"] == "PARTIAL_REQUIRES_RECONCILIATION"
    assert calls == []
