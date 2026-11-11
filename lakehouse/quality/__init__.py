"""Typed, read-only metadata contracts for future Silver quality checks.

This package intentionally contains no Spark execution or ETL integration.
"""

from .contracts import CheckDefinition, QualityError, QualityResult, TableQualityReport, build_quality_plan

__all__ = ["CheckDefinition", "QualityError", "QualityResult", "TableQualityReport", "build_quality_plan"]
