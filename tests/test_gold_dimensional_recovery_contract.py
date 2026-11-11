import pytest

from gold.implementation.dimensional import manifest
from gold.implementation.dimensional.manifest import DimensionalManifestError


GRAPH = {"identity": "g" * 64}
FENCE = {"version": 1, "kind": "gold_dimensional_global_fence", "graph_identity": GRAPH["identity"]}


def test_immutable_manifest_mismatch_is_rejected(monkeypatch):
    monkeypatch.setattr(manifest, "read_json_object", lambda *_: {"identity": "different"})
    with pytest.raises(DimensionalManifestError, match="Conflicting immutable manifest"):
        manifest.publish_manifest(object(), "ecommerce-quality-it", {"identity": "expected"}, "key")


def test_global_fence_release_refuses_non_owner(monkeypatch):
    monkeypatch.setattr(manifest, "read_json_object", lambda *_: {**FENCE, "graph_identity": "other"})
    with pytest.raises(DimensionalManifestError, match="another writer"):
        manifest.release_global_fence(object(), "ecommerce-quality-it", FENCE)


def test_graph_fence_assertion_refuses_non_owner(monkeypatch):
    monkeypatch.setattr(manifest, "read_json_object", lambda *_: {"graph_identity": "other"})
    with pytest.raises(DimensionalManifestError, match="ownership"):
        manifest.assert_graph_fence(object(), "ecommerce-quality-it", GRAPH, {"graph_identity": "expected"})


def test_recovery_classifier_does_not_mutate_state(monkeypatch):
    import gold.implementation.dimensional.writer as writer

    calls = []
    monkeypatch.setattr(writer, "build_graph_manifest", lambda *args, **kwargs: {"identity": "g" * 64})
    monkeypatch.setattr(writer, "read_completed_graph", lambda *_: None)
    monkeypatch.setattr(writer, "read_table_manifests", lambda *_: {"dim_customer": {"table": "dim_customer"}})
    monkeypatch.setattr(writer, "read_dimensional_sources", lambda *_: calls.append("read"))
    result = writer.run_dimensional_graph(None, object(), "ecommerce-quality-it", {"version": 1, "source_type": "iceberg_snapshot", "sources": {}}, dim_date_start="2026-01-01", dim_date_end="2026-01-01")
    assert result["status"] == "PARTIAL_REQUIRES_RECONCILIATION"
    assert calls == []
