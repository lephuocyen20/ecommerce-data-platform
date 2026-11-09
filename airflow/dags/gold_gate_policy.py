"""Dependency-free policy for deciding whether a Gold snapshot may run."""


def decide_gold_snapshot_gate(pipeline_result: dict, *, write_enabled: bool):
    """Require a verified Silver summary before starting the Gold snapshot flow."""
    if not isinstance(pipeline_result, dict):
        raise ValueError("Missing Silver pipeline summary for Gold gate")
    if pipeline_result.get("status") != "PASS":
        raise ValueError("Gold requires a successful Silver pipeline summary")
    if pipeline_result.get("mode") not in {"WRITE", "DRY_RUN"}:
        raise ValueError("Gold received an invalid Silver pipeline mode")
    if not isinstance(pipeline_result.get("results"), dict):
        raise ValueError("Gold requires verified Silver result details")
    if pipeline_result["mode"] != "WRITE" or not write_enabled:
        return "skip_gold_layer"
    return "resolve_gold_snapshot_lineage"
