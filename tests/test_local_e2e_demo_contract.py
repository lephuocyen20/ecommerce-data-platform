from pathlib import Path
import json
import subprocess

import pytest


SCRIPT = Path("scripts/run_local_e2e_demo.ps1").read_text(encoding="utf-8")


def test_demo_contract_declares_six_silver_and_seven_gold_outputs():
    for table in ("customers", "products", "orders", "order_items", "payments", "shipments"):
        assert f"'{table}'" in SCRIPT
    for table in ("dim_date", "dim_customer", "dim_product", "fact_orders", "fact_order_items", "fact_payments", "fact_shipments"):
        assert f"'{table}'" in SCRIPT
    assert "silver_input_count = $silver.Count" in SCRIPT
    assert "gold_output_count = $gold.Count" in SCRIPT


def test_demo_is_contract_only_and_fail_closed_on_scope_or_write_flags():
    assert "runtime_invoked = $false" in SCRIPT
    assert "ecommerce-quality-it" in SCRIPT
    assert "GOLD_DIMENSIONAL_WRITE_ENABLED" in SCRIPT
    assert "NESSIE_URI" in SCRIPT
    assert "quality-it/$runId/" in SCRIPT
    assert "docker compose" not in SCRIPT.lower()
    assert "spark-submit" not in SCRIPT.lower()


def _manifest(tmp_path, kind, tables, *, mode="DRY_RUN", can_write=False):
    items = [{"table": table, "snapshot_id": 100 + i, "identity": f"{table}-identity", "lineage": f"{table}-lineage", "evidence": {"identity": f"{table}-evidence", "status": "PASS", "snapshot_id": 100 + i, "table": table}} for i, table in enumerate(tables)]
    key = "silver_inputs" if kind == "silver" else "gold_outputs"
    payload = {"version": 1, "run_id": "qa-run", "nessie_ref": "qa_ref", "mode": mode, "can_write": can_write, key: items}
    path = tmp_path / f"{kind}.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_demo_validates_existing_manifest_paths_and_content(tmp_path):
    silver = _manifest(tmp_path, "silver", ["customers", "products", "orders", "order_items", "payments", "shipments"])
    gold = _manifest(tmp_path, "gold", ["dim_date", "dim_customer", "dim_product", "fact_orders", "fact_order_items", "fact_payments", "fact_shipments"])
    env = {**__import__("os").environ, "QUALITY_IT_RUN_ID": "qa-run", "QUALITY_IT_BUCKET": "ecommerce-quality-it", "QUALITY_IT_PREFIX": "quality-it/qa-run/", "NESSIE_URI": "http://localhost:19120/api/v2", "NESSIE_REF": "qa_ref", "GOLD_RUN_MODE": "DRY_RUN", "GOLD_DIMENSIONAL_WRITE_ENABLED": "false"}
    result = subprocess.run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "scripts/run_local_e2e_demo.ps1", "-Mode", "verify", "-SilverArtifactManifest", str(silver), "-GoldArtifactManifest", str(gold)], capture_output=True, text=True, env=env)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["manifest_validation"] == "PASS"


def test_demo_rejects_incomplete_manifest(tmp_path):
    silver = _manifest(tmp_path, "silver", ["customers"])
    gold = _manifest(tmp_path, "gold", ["dim_date", "dim_customer", "dim_product", "fact_orders", "fact_order_items", "fact_payments", "fact_shipments"])
    env = {**__import__("os").environ, "QUALITY_IT_RUN_ID": "qa-run", "QUALITY_IT_BUCKET": "ecommerce-quality-it", "QUALITY_IT_PREFIX": "quality-it/qa-run/", "NESSIE_URI": "http://localhost:19120/api/v2", "NESSIE_REF": "qa_ref", "GOLD_RUN_MODE": "DRY_RUN", "GOLD_DIMENSIONAL_WRITE_ENABLED": "false"}
    result = subprocess.run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "scripts/run_local_e2e_demo.ps1", "-Mode", "verify", "-SilverArtifactManifest", str(silver), "-GoldArtifactManifest", str(gold)], capture_output=True, text=True, env=env)
    assert result.returncode != 0
    assert "incomplete or arbitrary" in result.stderr or "exactly 6" in result.stderr


@pytest.mark.parametrize(("name", "value"), [("GOLD_RUN_MODE", None), ("GOLD_RUN_MODE", "dry_run"), ("GOLD_DIMENSIONAL_WRITE_ENABLED", None), ("GOLD_DIMENSIONAL_WRITE_ENABLED", "FALSE")])
def test_demo_requires_exact_non_mutating_flags(tmp_path, name, value):
    silver = _manifest(tmp_path, "silver", ["customers", "products", "orders", "order_items", "payments", "shipments"])
    gold = _manifest(tmp_path, "gold", ["dim_date", "dim_customer", "dim_product", "fact_orders", "fact_order_items", "fact_payments", "fact_shipments"])
    env = {**__import__("os").environ, "QUALITY_IT_RUN_ID": "qa-run", "QUALITY_IT_BUCKET": "ecommerce-quality-it", "QUALITY_IT_PREFIX": "quality-it/qa-run/", "NESSIE_URI": "http://localhost:19120/api/v2", "NESSIE_REF": "qa_ref", "GOLD_RUN_MODE": "DRY_RUN", "GOLD_DIMENSIONAL_WRITE_ENABLED": "false"}
    if value is None:
        env.pop(name, None)
    else:
        env[name] = value
    result = subprocess.run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "scripts/run_local_e2e_demo.ps1", "-Mode", "verify", "-SilverArtifactManifest", str(silver), "-GoldArtifactManifest", str(gold)], capture_output=True, text=True, env=env)
    assert result.returncode != 0


def test_demo_rejects_empty_or_inconsistent_manifest_evidence(tmp_path):
    silver = _manifest(tmp_path, "silver", ["customers", "products", "orders", "order_items", "payments", "shipments"])
    gold = _manifest(tmp_path, "gold", ["dim_date", "dim_customer", "dim_product", "fact_orders", "fact_order_items", "fact_payments", "fact_shipments"])
    payload = json.loads(silver.read_text(encoding="utf-8"))
    payload["silver_inputs"][0]["evidence"] = {}
    silver.write_text(json.dumps(payload), encoding="utf-8")
    env = {**__import__("os").environ, "QUALITY_IT_RUN_ID": "qa-run", "QUALITY_IT_BUCKET": "ecommerce-quality-it", "QUALITY_IT_PREFIX": "quality-it/qa-run/", "NESSIE_URI": "http://localhost:19120/api/v2", "NESSIE_REF": "qa_ref", "GOLD_RUN_MODE": "DRY_RUN", "GOLD_DIMENSIONAL_WRITE_ENABLED": "false"}
    result = subprocess.run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "scripts/run_local_e2e_demo.ps1", "-Mode", "verify", "-SilverArtifactManifest", str(silver), "-GoldArtifactManifest", str(gold)], capture_output=True, text=True, env=env)
    assert result.returncode != 0

    payload["silver_inputs"][0]["evidence"] = {"identity": "customers-evidence", "status": "PASS", "snapshot_id": 999, "table": "customers"}
    silver.write_text(json.dumps(payload), encoding="utf-8")
    result = subprocess.run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "scripts/run_local_e2e_demo.ps1", "-Mode", "verify", "-SilverArtifactManifest", str(silver), "-GoldArtifactManifest", str(gold)], capture_output=True, text=True, env=env)
    assert result.returncode != 0
