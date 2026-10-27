"""Dependency-free runtime safety policies shared by pipeline layers."""

import os

DRY_RUN = "DRY_RUN"
WRITE = "WRITE"
PIPELINE_MODES = frozenset({DRY_RUN, WRITE})


def is_feature_enabled(variable_name, environ=None):
    if not isinstance(variable_name, str) or not variable_name:
        raise ValueError("Feature flag name must be a non-empty string")
    value = (os.environ if environ is None else environ).get(variable_name, "")
    return isinstance(value, str) and value.lower() == "true"


def require_feature_enabled(variable_name, message=None, environ=None):
    if not is_feature_enabled(variable_name, environ=environ):
        raise RuntimeError(message or f"Write blocked: {variable_name}=true required")


def validate_pipeline_mode(mode):
    if mode not in PIPELINE_MODES:
        raise ValueError(f"Unsupported mode: {mode}")
    return mode
