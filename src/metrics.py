"""Reusable metrics helpers for dashboard pages."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.constants import CONSUMPTION_DATE_COLUMN, VEND_DATE_COLUMN


def _safe_sum(df: pd.DataFrame, column: str) -> float:
    if column not in df.columns or df.empty:
        return 0.0
    return float(pd.to_numeric(df[column], errors="coerce").fillna(0).sum())


def _safe_mean(df: pd.DataFrame, column: str) -> float:
    if column not in df.columns or df.empty:
        return 0.0
    series = pd.to_numeric(df[column], errors="coerce").dropna()
    return float(series.mean()) if not series.empty else 0.0


def _safe_nunique(df: pd.DataFrame, column: str) -> int:
    if column not in df.columns or df.empty:
        return 0
    return int(df[column].nunique(dropna=True))


def date_span(df: pd.DataFrame, column: str) -> dict[str, Any]:
    """Return min and max dates when available."""

    if column not in df.columns or df.empty:
        return {"min": None, "max": None}

    series = pd.to_datetime(df[column], errors="coerce").dropna()
    if series.empty:
        return {"min": None, "max": None}
    return {"min": series.min(), "max": series.max()}


def consumption_metrics(df: pd.DataFrame) -> dict[str, Any]:
    """Compute safe metrics for the consumption dataset."""

    daily = (
        df.groupby("date", dropna=True)[["kwh_consumption", "kvah_consumption"]]
        .sum(numeric_only=True)
        .reset_index()
        if not df.empty and "date" in df.columns
        else pd.DataFrame()
    )

    return {
        "rows": int(len(df)),
        "unique_consumers": _safe_nunique(df, "consumernumber_normalized"),
        "unique_meters": _safe_nunique(df, "meterno_normalized"),
        "total_kwh_consumption": _safe_sum(df, "kwh_consumption"),
        "total_kvah_consumption": _safe_sum(df, "kvah_consumption"),
        "average_daily_kwh": float(daily["kwh_consumption"].mean()) if not daily.empty else 0.0,
        "min_daily_kwh": float(daily["kwh_consumption"].min()) if not daily.empty else 0.0,
        "max_daily_kwh": float(daily["kwh_consumption"].max()) if not daily.empty else 0.0,
        "date_span": date_span(df, CONSUMPTION_DATE_COLUMN),
    }


def vend_metrics(df: pd.DataFrame) -> dict[str, Any]:
    """Compute safe metrics for the vend dataset."""

    return {
        "rows": int(len(df)),
        "transactions": int(len(df)),
        "unique_consumers": _safe_nunique(df, "consumernumber_normalized"),
        "unique_meters": _safe_nunique(df, "meterno"),
        "total_transaction_amount": _safe_sum(df, "transactionamount"),
        "average_transaction_amount": _safe_mean(df, "transactionamount"),
        "date_span": date_span(df, VEND_DATE_COLUMN),
        "time_only_rows": int((df.get("issuedate_is_time_only", pd.Series(dtype="bool")) == True).sum()),
    }


def overview_metrics(consumption_df: pd.DataFrame, vend_df: pd.DataFrame, file_inventory: pd.DataFrame) -> dict[str, Any]:
    """Aggregate project-level metrics for the landing page."""

    dataset_series = file_inventory.get("dataset", pd.Series(dtype="string"))
    return {
        "file_count": int(len(file_inventory)),
        "consumption_files": int((dataset_series == "consumption").sum()),
        "vend_files": int((dataset_series == "vend").sum()),
        "consumption": consumption_metrics(consumption_df),
        "vend": vend_metrics(vend_df),
    }
