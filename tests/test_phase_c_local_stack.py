from pathlib import Path


ROOT = Path(__file__).parents[1]
SCRIPT = ROOT / "tests" / "run_phase_c_local_stack.py"


def test_local_stack_runner_is_scoped_and_non_destructive():
    source = SCRIPT.read_text(encoding="utf-8")
    assert "LOCAL_BUCKET" in source
    assert "PROJECT_ROOT = Path(__file__).resolve().parents[1]" in source
    assert "sys.path.insert(0, str(PROJECT_ROOT))" in source
    assert "validate_local_scope" in source
    assert "run_dimensional_graph" in source
    assert "mode=DRY_RUN" in source
    assert "DROP TABLE" not in source
    assert "expire_snapshots" not in source
    assert "remove_object" not in source
