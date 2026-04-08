"""Schema standardisation and validation helpers."""

from __future__ import annotations

import re
from typing import Any, Mapping

import pandas as pd

from src.key_utils import NULL_LIKE_TEXT


def to_snake_case(value: str) -> str:
    """Convert column names to lowercase snake case."""

    cleaned = re.sub(r"[^0-9A-Za-z]+", "_", value.strip())
    cleaned = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned)
    return cleaned.strip("_").lower()


def standardize_column_names(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str]]:
    """Return a copy with standardised columns and the original-to-new mapping."""

    mapping = {column: to_snake_case(column) for column in df.columns}
    standardized = df.rename(columns=mapping).copy()
    return standardized, mapping


def trim_string_values(df: pd.DataFrame) -> pd.DataFrame:
    """Trim leading and trailing whitespace from text-like columns."""

    trimmed = df.copy()
    for column in trimmed.columns:
        if pd.api.types.is_object_dtype(trimmed[column]) or pd.api.types.is_string_dtype(trimmed[column]):
            series = trimmed[column].astype("string").str.strip()
            trimmed[column] = series.mask(series.str.lower().isin(NULL_LIKE_TEXT))
    return trimmed


def validate_required_columns(df: pd.DataFrame, required_columns: list[str]) -> dict[str, Any]:
    """Validate required columns and capture extra columns for diagnostics."""

    actual_columns = list(df.columns)
    missing_columns = [column for column in required_columns if column not in actual_columns]
    extra_columns = [column for column in actual_columns if column not in required_columns]

    return {
        "required_columns": required_columns,
        "actual_columns": actual_columns,
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "is_valid": not missing_columns,
    }


def get_dataset_schema(schema_config: Mapping[str, Any], dataset_name: str) -> dict[str, Any]:
    """Fetch the configured schema block for a dataset."""

    datasets = schema_config.get("datasets", {})
    dataset_schema = datasets.get(dataset_name, {})
    if not isinstance(dataset_schema, dict):
        raise ValueError(f"Schema configuration for dataset '{dataset_name}' must be a mapping.")
    return dataset_schema


def summarize_schema_report(schema_report: Mapping[str, Any]) -> str:
    """Create a compact human-readable schema status string."""

    missing = schema_report.get("missing_columns", [])
    if not missing:
        return "All required columns present"
    return f"Missing required columns: {', '.join(missing)}"
