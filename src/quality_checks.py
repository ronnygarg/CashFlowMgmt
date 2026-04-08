"""Extensible data-quality checks and diagnostics."""

from __future__ import annotations

from typing import Any, Mapping

import pandas as pd

from src.constants import (
    DATE_STATUS_DATE_ONLY,
    DATE_STATUS_MISSING,
    DATE_STATUS_PARSED,
    DATE_STATUS_TIME_ONLY,
    LIMITATION_VEND_DATETIME,
)
from src.profiling import build_column_profile
from src.schema_utils import validate_required_columns


def _quality_thresholds(app_config: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return quality thresholds from app config in a safe shape."""

    thresholds = app_config.get("quality_checks", {}).get("thresholds", {})
    return thresholds if isinstance(thresholds, Mapping) else {}


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

    if dataset_name == "consumer_master":
        frames: list[pd.DataFrame] = []
        for field_name, status_column in (
            ("meterinstallationdate", "meterinstallationdate_parse_status"),
            ("balanceupdatedon", "balanceupdatedon_parse_status"),
        ):
            if status_column not in df.columns:
                continue
            summary = (
                df[status_column]
                .fillna("unknown")
                .value_counts(dropna=False)
                .rename_axis("status")
                .reset_index(name="count")
            )
            summary.insert(0, "field", field_name)
            frames.append(summary)
        if frames:
            return pd.concat(frames, ignore_index=True)
        return pd.DataFrame(columns=["field", "status", "count"])

    if dataset_name == "consumption":
        status_counts = (
            df.get("midnightdate_parse_status", pd.Series(dtype="string"))
            .fillna("unknown")
            .value_counts(dropna=False)
            .rename_axis("status")
            .reset_index(name="count")
        )
        status_counts.insert(0, "field", "midnightdate")
        return status_counts

    if dataset_name == "vend":
        status_counts = (
            df.get("issuedate_parse_status", pd.Series(dtype="string"))
            .fillna("unknown")
            .value_counts(dropna=False)
            .rename_axis("status")
            .reset_index(name="count")
        )
        status_counts.insert(0, "field", "issuedate")
        return status_counts

    return pd.DataFrame(columns=["field", "status", "count"])


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


def build_outlier_summary(df: pd.DataFrame, numeric_columns: list[str], iqr_multiplier: float = 1.5) -> pd.DataFrame:
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
        lower_bound = q1 - iqr_multiplier * iqr
        upper_bound = q3 + iqr_multiplier * iqr
        outlier_count = int(((series < lower_bound) | (series > upper_bound)).sum())
        records.append(
            {
                "column": column,
                "outlier_count": outlier_count,
                "lower_bound": round(float(lower_bound), 3),
                "upper_bound": round(float(upper_bound), 3),
                "iqr_multiplier": round(float(iqr_multiplier), 3),
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
        "duplicate_policy_mode",
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
        date_only_count = int((status_series == DATE_STATUS_DATE_ONLY).sum())
        total_rows = len(df)
        parsed_pct = (parsed_count / total_rows * 100) if total_rows else 0.0
        warning_threshold_pct = float(
            app_config.get("quality_checks", {})
            .get("thresholds", {})
            .get("vend_full_datetime_warning_pct", 70)
        )

        warnings.append(LIMITATION_VEND_DATETIME)
        if time_only_count:
            warnings.append(
                f"{time_only_count:,} vend records contain time-only issuedate values and cannot support full date analysis."
            )
        if date_only_count:
            warnings.append(
                f"{date_only_count:,} vend records contain date-only issuedate values and cannot support intraday timing analysis."
            )
        if missing_count:
            warnings.append(f"{missing_count:,} vend records have missing issuedate values.")
        if parsed_count == 0 and not df.empty:
            warnings.append("No vend rows currently contain a parsed full datetime.")
        if total_rows and parsed_pct < warning_threshold_pct:
            warnings.append(
                f"Vend full datetime coverage is {parsed_pct:.1f}% which is below the warning threshold "
                f"of {warning_threshold_pct:.1f}%. Date-based charts remain available with caution."
            )

    return list(dict.fromkeys(warnings))


def _dataset_date_column(dataset_name: str) -> str | None:
    if dataset_name == "consumer_master":
        return "balanceupdatedon_parsed"
    if dataset_name == "consumption":
        return "midnightdate_parsed"
    if dataset_name == "vend":
        return "issuedate_parsed"
    return None


def build_temporal_sanity_summary(
    dataset_name: str,
    df: pd.DataFrame,
    max_future_days: int = 0,
    stale_data_warning_days: int = 90,
    reference_timestamp: pd.Timestamp | None = None,
) -> dict[str, Any]:
    """Compute temporal sanity diagnostics for parsed datetime fields."""

    date_column = _dataset_date_column(dataset_name)
    if not date_column or date_column not in df.columns:
        return {
            "date_column": date_column,
            "parsed_rows": 0,
            "total_rows": int(len(df)),
            "parse_coverage_pct": 0.0,
            "future_row_count": 0,
            "latest_date": None,
            "oldest_date": None,
            "is_stale": False,
            "stale_days": stale_data_warning_days,
        }

    parsed_series = pd.to_datetime(df[date_column], errors="coerce")
    parsed_non_null = parsed_series.dropna()
    parsed_rows = int(parsed_non_null.shape[0])
    total_rows = int(len(df))
    parse_coverage_pct = round((parsed_rows / total_rows * 100), 2) if total_rows else 0.0

    now_ts = reference_timestamp if reference_timestamp is not None else pd.Timestamp.now()
    future_cutoff = now_ts + pd.Timedelta(days=max_future_days)
    future_row_count = int((parsed_non_null > future_cutoff).sum()) if not parsed_non_null.empty else 0

    oldest_date = parsed_non_null.min() if not parsed_non_null.empty else pd.NaT
    latest_date = parsed_non_null.max() if not parsed_non_null.empty else pd.NaT
    stale_cutoff = now_ts - pd.Timedelta(days=stale_data_warning_days)
    is_stale = bool(pd.notna(latest_date) and latest_date < stale_cutoff)

    return {
        "date_column": date_column,
        "parsed_rows": parsed_rows,
        "total_rows": total_rows,
        "parse_coverage_pct": parse_coverage_pct,
        "future_row_count": future_row_count,
        "latest_date": latest_date,
        "oldest_date": oldest_date,
        "is_stale": is_stale,
        "stale_days": stale_data_warning_days,
    }


def build_duplicate_diagnostics(dataset_name: str, df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Build duplicate diagnostics tables for future Data Quality visualizations."""

    if df.empty:
        empty = pd.DataFrame(columns=["label", "duplicate_rows"])
        return {
            "by_source_file": empty.copy(),
            "by_date": empty.copy(),
        }

    duplicated_mask = df.duplicated(keep=False)
    duplicated_df = df[duplicated_mask].copy()

    if duplicated_df.empty:
        empty = pd.DataFrame(columns=["label", "duplicate_rows"])
        return {
            "by_source_file": empty.copy(),
            "by_date": empty.copy(),
        }

    if "source_file" in duplicated_df.columns:
        by_source = (
            duplicated_df.groupby("source_file", dropna=False)
            .size()
            .reset_index(name="duplicate_rows")
            .rename(columns={"source_file": "label"})
            .sort_values(["duplicate_rows", "label"], ascending=[False, True])
            .reset_index(drop=True)
        )
    else:
        by_source = pd.DataFrame(columns=["label", "duplicate_rows"])

    date_column = "date" if dataset_name == "consumption" else "vend_date"
    if date_column in duplicated_df.columns:
        date_series = pd.to_datetime(duplicated_df[date_column], errors="coerce").dt.date.astype("string")
        by_date = (
            pd.DataFrame({"label": date_series.fillna("unknown")})
            .groupby("label", dropna=False)
            .size()
            .reset_index(name="duplicate_rows")
            .sort_values(["label"], ascending=[True])
            .reset_index(drop=True)
        )
    else:
        by_date = pd.DataFrame(columns=["label", "duplicate_rows"])

    return {
        "by_source_file": by_source,
        "by_date": by_date,
    }


def build_categorical_validation_summary(df: pd.DataFrame, allow_lists: Mapping[str, list[Any]]) -> pd.DataFrame:
    """Validate configured categorical allow-lists and report out-of-domain values."""

    records: list[dict[str, Any]] = []
    for column, allowed_values in allow_lists.items():
        if column not in df.columns:
            continue
        allowed_set = {str(value) for value in allowed_values or []}
        if not allowed_set:
            continue

        series = df[column].astype("string")
        non_null = series.dropna()
        invalid_mask = ~non_null.isin(allowed_set)
        invalid_count = int(invalid_mask.sum())
        sample_invalid = non_null[invalid_mask].drop_duplicates().head(5).tolist()
        records.append(
            {
                "column": column,
                "allowed_count": len(allowed_set),
                "invalid_count": invalid_count,
                "sample_invalid_values": " | ".join(str(value) for value in sample_invalid),
            }
        )

    return pd.DataFrame(records)


def build_key_diagnostics(df: pd.DataFrame, key_columns: list[str]) -> pd.DataFrame:
    """Summarize duplicate and non-null coverage for configured key columns."""

    records: list[dict[str, Any]] = []
    for column in key_columns:
        normalized_column = f"{column}_normalized" if f"{column}_normalized" in df.columns else column
        if normalized_column not in df.columns:
            continue

        series = df[normalized_column].astype("string").dropna()
        duplicate_mask = series.duplicated(keep=False)
        records.append(
            {
                "key_column": normalized_column,
                "non_null_count": int(series.shape[0]),
                "distinct_count": int(series.nunique(dropna=True)),
                "duplicate_row_count": int(duplicate_mask.sum()),
                "duplicate_key_count": int(series[duplicate_mask].nunique(dropna=True)),
            }
        )

    return pd.DataFrame(records)


def run_dataset_quality_checks(
    dataset_name: str,
    df: pd.DataFrame,
    schema: Mapping[str, Any],
    app_config: Mapping[str, Any],
    file_inventory: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Run the current suite of modular quality checks for one dataset."""

    quality_thresholds = _quality_thresholds(app_config)
    iqr_multiplier = float(quality_thresholds.get("outlier_iqr_multiplier", 1.5))
    max_future_days = int(quality_thresholds.get("temporal_max_future_days", 0))
    stale_warning_days = int(quality_thresholds.get("stale_data_warning_days", 90))
    categorical_allow_lists = schema.get("categorical_allow_lists", {})

    schema_report = validate_required_columns(df, list(schema.get("required_columns", [])))
    missing_summary = build_missing_value_summary(df)
    parse_summary = build_parse_summary(dataset_name, df)
    numeric_flags = build_numeric_flag_summary(df, list(schema.get("numeric_columns", [])))
    outlier_summary = build_outlier_summary(
        df,
        list(schema.get("numeric_columns", [])),
        iqr_multiplier=iqr_multiplier,
    )
    temporal_summary = build_temporal_sanity_summary(
        dataset_name=dataset_name,
        df=df,
        max_future_days=max_future_days,
        stale_data_warning_days=stale_warning_days,
    )
    duplicate_diagnostics = build_duplicate_diagnostics(dataset_name, df)
    categorical_summary = build_categorical_validation_summary(
        df,
        categorical_allow_lists if isinstance(categorical_allow_lists, Mapping) else {},
    )
    key_diagnostics = build_key_diagnostics(df, list(schema.get("candidate_keys", [])))
    column_profile = build_column_profile(df)
    duplicate_rows = int(df.duplicated().sum()) if not df.empty else 0

    dataset_inventory = pd.DataFrame()
    if file_inventory is not None and not file_inventory.empty and "dataset" in file_inventory.columns:
        dataset_inventory = file_inventory[file_inventory["dataset"] == dataset_name].copy()

    warnings = build_quality_warnings(dataset_name, df, app_config)
    if temporal_summary["future_row_count"] > 0:
        warnings.append(
            f"{temporal_summary['future_row_count']:,} rows have parsed dates beyond the allowed future window "
            f"({max_future_days} days)."
        )
    if temporal_summary["is_stale"]:
        warnings.append(
            f"Latest parsed date appears stale based on a {stale_warning_days}-day threshold."
        )
    if not categorical_summary.empty and "invalid_count" in categorical_summary.columns:
        invalid_total = int(categorical_summary["invalid_count"].sum())
        if invalid_total > 0:
            warnings.append(
                f"Categorical validation found {invalid_total:,} values outside configured allow-lists."
            )
    if not key_diagnostics.empty and "duplicate_key_count" in key_diagnostics.columns:
        duplicate_key_total = int(key_diagnostics["duplicate_key_count"].sum())
        if duplicate_key_total > 0:
            warnings.append(
                f"Key diagnostics found {duplicate_key_total:,} duplicate normalized business keys across configured join columns."
            )

    return {
        "schema_report": schema_report,
        "missing_summary": missing_summary,
        "parse_summary": parse_summary,
        "numeric_flags": numeric_flags,
        "outlier_summary": outlier_summary,
        "temporal_summary": temporal_summary,
        "duplicate_diagnostics": duplicate_diagnostics,
        "categorical_summary": categorical_summary,
        "key_diagnostics": key_diagnostics,
        "column_profile": column_profile,
        "duplicate_rows": duplicate_rows,
        "warnings": list(dict.fromkeys(warnings)),
        "file_diagnostics": build_file_level_diagnostics(dataset_inventory),
    }

