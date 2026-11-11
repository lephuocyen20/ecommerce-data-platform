"""Dependency-free policy for the native dimensional Gold graph."""


def decide_gold_dimensional_gate(pipeline_result: dict, *, enabled: bool):
    """Own the native dimensional branch; it never depends on aggregate Gold."""
    if not enabled:
        return "skip_gold_dimensional"
    if not isinstance(pipeline_result, dict) or pipeline_result.get("status") != "PASS":
        raise ValueError("Dimensional Gold requires a successful Silver pipeline summary")
    if pipeline_result.get("mode") != "WRITE" or not isinstance(pipeline_result.get("results"), dict):
        raise ValueError("Dimensional Gold requires verified Silver WRITE results")
    return "resolve_gold_dimensional_lineage"
