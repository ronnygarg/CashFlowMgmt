"""Extensible data-quality checks and diagnostics."""

from __future__ import annotations

from typing import Any, Mapping

import pandas as pd

from src.constants import (
    DATE_STATUS_MISSING,
    DATE_STATUS_PARSED,
    DATE_STATUS_TIME_ONLY,
    LIMITATION_VEND_DATETIME,
)
from src.profiling import build_column_profile
from src.schema_utils import validate_required_columns


def build_missing_value_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Summarise missing values by column."""

    if df.empty:
        return pd.DataFrame(columns=["column", "missing_count", "missing_pct"])

    summary = pd.DataFrame(
        {
            "column": df.columns,
            "missing_count": df.isna().sum().values,
        }
    )
    summary["missing_pct"] = (summary["missing_count"] / len(df) * 100).round(2)
    return summary.sort_values(["missing_count", "column"], ascending=[False, True]).reset_index(drop=True)


def build_parse_summary(dataset_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Summarise parse outcomes for configured date fields."""

    if dataset_name == "consumption":
        total_rows = len(df)
        success = int(df.get("midnightdate_parse_success", pd.Series(dtype="bool")).fillna(False).sum())
        failed = max(total_rows - success, 0)
        return pd.DataFrame(
            [
                {"status": DATE_STATUS_PARSED, "count": success},
                {"status": "failed_or_missing", "count": failed},
            ]
        )

    if dataset_name == "vend":
        status_counts = (
            df.get("issuedate_parse_status", pd.Series(dtype="string"))
            .fillna("unknown")
            .value_counts(dropna=False)
            .rename_axis("status")
            .reset_index(name="count")
        )
        return status_counts

    return pd.DataFrame(columns=["status", "count"])


def build_numeric_flag_summary(df: pd.DataFrame, numeric_columns: list[str]) -> pd.DataFrame:
    """Flag negative values, zeros, and nulls for configured numeric columns."""

    records: list[dict[str, Any]] = []
    for column in numeric_columns:
        if column not in df.columns:
            continue
        series = pd.to_numeric(df[column], errors="coerce")
        records.append(
            {
                "column": column,
                "null_count": int(series.isna().sum()),
                "negative_count": int((series < 0).sum()),
                "zero_count": int((series == 0).sum()),
            }
        )

    return pd.DataFrame(records)


def build_outlier_summary(df: pd.DataFrame, numeric_columns: list[str]) -> pd.DataFrame:
    """Use a simple IQR heuristic to count potential outliers."""

    records: list[dict[str, Any]] = []
    for column in numeric_columns:
        if column not in df.columns:
            continue

        series = pd.to_numeric(df[column], errors="coerce").dropna()
        if len(series) < 4:
            records.append(
                {
                    "column": column,
                    "outlier_count": 0,
                    "lower_bound": None,
                    "upper_bound": None,
                }
            )
            continue

        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outlier_count = int(((series < lower_bound) | (series > upper_bound)).sum())
        records.append(
            {
                "column": column,
                "outlier_count": outlier_count,
                "lower_bound": round(float(lower_bound), 3),
                "upper_bound": round(float(upper_bound), 3),
            }
        )

    return pd.DataFrame(records)


def build_file_level_diagnostics(file_inventory: pd.DataFrame) -> pd.DataFrame:
    """Return file ingest diagnostics in a consistent tabular form."""

    if file_inventory.empty:
        return pd.DataFrame(
            columns=[
                "dataset",
                "file_name",
                "rows_read",
                "column_count",
                "schema_valid",
                "missing_required_columns",
                "read_status",
            ]
        )

    columns = [
        "dataset",
        "file_name",
        "rows_read",
        "column_count",
        "schema_valid",
        "missing_required_columns",
        "read_status",
        "parse_warning_count",
        "file_size_mb",
    ]
    available_columns = [column for column in columns if column in file_inventory.columns]
    return file_inventory[available_columns].copy()


def build_quality_warnings(dataset_name: str, df: pd.DataFrame, app_config: Mapping[str, Any]) -> list[str]:
    """Generate high-signal quality warnings for the dashboard UI."""

    warnings: list[str] = []
    known_limitations = app_config.get("data", {}).get("known_limitations", [])
    warnings.extend(str(item) for item in known_limitations)

    if dataset_name == "vend":
        status_series = df.get("issuedate_parse_status", pd.Series(dtype="string"))
        time_only_count = int((status_series == DATE_STATUS_TIME_ONLY).sum())
        missing_count = int((status_series == DATE_STATUS_MISSING).sum())
        parsed_count = int((status_series == DATE_STATUS_PARSED).sum())

        warnings.append(LIMITATION_VEND_DATETIME)
        if time_only_count:
            warnings.append(
                f"{time_only_count:,} vend records contain time-only issuedate values and cannot support full date analysis."
            )
        if missing_count:
            warnings.append(f"{missing_count:,} vend records have missing issuedate values.")
        if parsed_count == 0 and not df.empty:
            warnings.append("No vend rows currently contain a parsed full datetime.")

    return list(dict.fromkeys(warnings))


def run_dataset_quality_checks(
    dataset_name: str,
    df: pd.DataFrame,
    schema: Mapping[str, Any],
    app_config: Mapping[str, Any],
    file_inventory: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Run the current suite of modular quality checks for one dataset."""

    schema_report = validate_required_columns(df, list(schema.get("required_columns", [])))
    missing_summary = build_missing_value_summary(df)
    parse_summary = build_parse_summary(dataset_name, df)
    numeric_flags = build_numeric_flag_summary(df, list(schema.get("numeric_columns", [])))
    outlier_summary = build_outlier_summary(df, list(schema.get("numeric_columns", [])))
    column_profile = build_column_profile(df)
    duplicate_rows = int(df.duplicated().sum()) if not df.empty else 0

    dataset_inventory = pd.DataFrame()
    if file_inventory is not None and not file_inventory.empty and "dataset" in file_inventory.columns:
        dataset_inventory = file_inventory[file_inventory["dataset"] == dataset_name].copy()

    return {
        "schema_report": schema_report,
        "missing_summary": missing_summary,
        "parse_summary": parse_summary,
        "numeric_flags": numeric_flags,
        "outlier_summary": outlier_summary,
        "column_profile": column_profile,
        "duplicate_rows": duplicate_rows,
        "warnings": build_quality_warnings(dataset_name, df, app_config),
        "file_diagnostics": build_file_level_diagnostics(dataset_inventory),
    }

