"""Lightweight dataframe profiling utilities for exploratory diagnostics."""

from __future__ import annotations

from typing import Any

import pandas as pd


def dataframe_overview(df: pd.DataFrame) -> dict[str, Any]:
    """Return compact dataframe summary stats."""

    if df.empty:
        return {
            "rows": 0,
            "columns": 0,
            "memory_mb": 0.0,
        }

    return {
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "memory_mb": round(float(df.memory_usage(deep=True).sum()) / (1024 * 1024), 2),
    }


def build_column_profile(df: pd.DataFrame) -> pd.DataFrame:
    """Create a reusable per-column profile table."""

    records: list[dict[str, Any]] = []

    for column in df.columns:
        series = df[column]
        non_null = int(series.notna().sum())
        null_count = int(series.isna().sum())
        missing_pct = round((null_count / len(df) * 100), 2) if len(df) else 0.0
        sample_values = [str(value) for value in series.dropna().head(3).tolist()]

        record: dict[str, Any] = {
            "column": column,
            "dtype": str(series.dtype),
            "non_null_count": non_null,
            "null_count": null_count,
            "missing_pct": missing_pct,
            "unique_count": int(series.nunique(dropna=True)),
            "sample_values": " | ".join(sample_values),
        }

        if pd.api.types.is_numeric_dtype(series) or pd.api.types.is_datetime64_any_dtype(series):
            record["min_value"] = series.min(skipna=True)
            record["max_value"] = series.max(skipna=True)
        else:
            record["min_value"] = None
            record["max_value"] = None

        records.append(record)

    return pd.DataFrame(records)
