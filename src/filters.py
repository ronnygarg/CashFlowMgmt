"""Reusable Streamlit filters for dashboard pages."""

from __future__ import annotations

from typing import Any, Iterable, Mapping

import pandas as pd
import streamlit as st


def _unique_options(df: pd.DataFrame, column: str) -> list[str]:
    if column not in df.columns or df.empty:
        return []
    return sorted(df[column].dropna().astype("string").drop_duplicates().tolist())


def render_dimension_filters(
    df: pd.DataFrame,
    app_config: Mapping[str, Any],
    key_prefix: str,
    title: str,
    date_column: str | None = None,
    date_label: str = "Date range",
    extra_columns: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Render shared categorical and date filters for dashboard pages."""

    filters: dict[str, Any] = {}
    st.sidebar.subheader(title)

    if df.empty:
        st.sidebar.info("No rows are available for this view.")
        return filters

    if date_column and date_column in df.columns:
        date_series = pd.to_datetime(df[date_column], errors="coerce").dropna()
        if not date_series.empty:
            min_date = date_series.min().date()
            max_date = date_series.max().date()
            filters["date_column"] = date_column
            filters["date_range"] = st.sidebar.date_input(
                date_label,
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
                key=f"{key_prefix}_{date_column}_range",
            )

    configured_dimensions = list(app_config.get("filters", {}).get("dimension_columns", []))
    dimension_columns = configured_dimensions + list(extra_columns or [])
    for column in dict.fromkeys(dimension_columns):
        options = _unique_options(df, column)
        if not options:
            continue
        label = column.replace("_", " ").title()
        filters[column] = st.sidebar.multiselect(
            label,
            options=options,
            key=f"{key_prefix}_{column}",
        )

    return filters


def apply_dimension_filters(df: pd.DataFrame, filters: Mapping[str, Any]) -> pd.DataFrame:
    """Apply the shared filters returned by render_dimension_filters."""

    filtered = df.copy()
    if filtered.empty:
        return filtered

    date_column = filters.get("date_column")
    date_range = filters.get("date_range")
    if date_column and date_range and len(date_range) == 2 and date_column in filtered.columns:
        start_date = pd.Timestamp(date_range[0])
        end_date = pd.Timestamp(date_range[1])
        parsed_dates = pd.to_datetime(filtered[date_column], errors="coerce")
        filtered = filtered[parsed_dates.between(start_date, end_date, inclusive="both")]

    for column, values in filters.items():
        if column in {"date_column", "date_range"}:
            continue
        if column in filtered.columns and values:
            value_set = {str(value) for value in values}
            filtered = filtered[filtered[column].astype("string").isin(value_set)]

    return filtered
