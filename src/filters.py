"""Streamlit filter widgets and dataframe filter logic."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st


def render_consumption_filters(df: pd.DataFrame, key_prefix: str = "consumption") -> dict[str, Any]:
    """Render sidebar filters for the consumption page."""

    filters: dict[str, Any] = {}
    st.sidebar.subheader("Consumption Filters")

    if df.empty:
        st.sidebar.info("No consumption data available.")
        return filters

    date_series = pd.to_datetime(df.get("date"), errors="coerce").dropna()
    if not date_series.empty:
        min_date = date_series.min().date()
        max_date = date_series.max().date()
        filters["date_range"] = st.sidebar.date_input(
            "Date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            key=f"{key_prefix}_date_range",
        )

    meter_options = sorted(df.get("mtrid", pd.Series(dtype="string")).dropna().astype(str).unique().tolist())
    filters["mtrid"] = st.sidebar.multiselect(
        "Meters",
        options=meter_options,
        key=f"{key_prefix}_mtrid",
    )

    if "kwh_consumption" in df.columns:
        series = pd.to_numeric(df["kwh_consumption"], errors="coerce")
        min_value = float(series.min(skipna=True) if not series.dropna().empty else 0.0)
        max_value = float(series.max(skipna=True) if not series.dropna().empty else 0.0)
        slider_max = max_value if max_value > min_value else min_value + 1.0
        filters["kwh_range"] = st.sidebar.slider(
            "kWh consumption range",
            min_value=min_value,
            max_value=slider_max,
            value=(min_value, slider_max),
            key=f"{key_prefix}_kwh_range",
        )

    return filters


def apply_consumption_filters(df: pd.DataFrame, filters: dict[str, Any]) -> pd.DataFrame:
    """Apply sidebar filters to the consumption dataframe."""

    filtered = df.copy()

    date_range = filters.get("date_range")
    if date_range and len(date_range) == 2 and "date" in filtered.columns:
        start_date = pd.Timestamp(date_range[0])
        end_date = pd.Timestamp(date_range[1])
        filtered = filtered[filtered["date"].between(start_date, end_date)]

    if filters.get("mtrid"):
        meter_values = {str(value) for value in filters["mtrid"]}
        filtered = filtered[filtered["mtrid"].astype(str).isin(meter_values)]

    if filters.get("kwh_range") and "kwh_consumption" in filtered.columns:
        low, high = filters["kwh_range"]
        series = pd.to_numeric(filtered["kwh_consumption"], errors="coerce")
        filtered = filtered[series.between(low, high, inclusive="both")]

    return filtered


def render_vend_filters(df: pd.DataFrame, key_prefix: str = "vend") -> dict[str, Any]:
    """Render sidebar filters for the vend page."""

    filters: dict[str, Any] = {}
    st.sidebar.subheader("Vend Filters")

    if df.empty:
        st.sidebar.info("No vend data available.")
        return filters

    filters["meterno"] = st.sidebar.multiselect(
        "Meters",
        options=sorted(df.get("meterno", pd.Series(dtype="string")).dropna().astype(str).unique().tolist()),
        key=f"{key_prefix}_meterno",
    )
    filters["servicepointno"] = st.sidebar.multiselect(
        "Service points",
        options=sorted(df.get("servicepointno", pd.Series(dtype="string")).dropna().astype(str).unique().tolist()),
        key=f"{key_prefix}_servicepointno",
    )
    filters["categorycode"] = st.sidebar.multiselect(
        "Category codes",
        options=sorted(df.get("categorycode", pd.Series(dtype="string")).dropna().astype(str).unique().tolist()),
        key=f"{key_prefix}_categorycode",
    )
    filters["source_file"] = st.sidebar.multiselect(
        "Source files",
        options=sorted(df.get("source_file", pd.Series(dtype="string")).dropna().astype(str).unique().tolist()),
        key=f"{key_prefix}_source_file",
    )

    if "transactionamount" in df.columns:
        series = pd.to_numeric(df["transactionamount"], errors="coerce")
        min_amount = float(series.min(skipna=True) if not series.dropna().empty else 0.0)
        max_amount = float(series.max(skipna=True) if not series.dropna().empty else 0.0)
        slider_max = max_amount if max_amount > min_amount else min_amount + 1.0
        filters["transaction_range"] = st.sidebar.slider(
            "Transaction amount range",
            min_value=min_amount,
            max_value=slider_max,
            value=(min_amount, slider_max),
            key=f"{key_prefix}_transaction_range",
        )

    parsed_dates = pd.to_datetime(df.get("vend_date"), errors="coerce").dropna()
    if not parsed_dates.empty:
        min_date = parsed_dates.min().date()
        max_date = parsed_dates.max().date()
        filters["date_range"] = st.sidebar.date_input(
            "Vend date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            key=f"{key_prefix}_date_range",
        )

    analysis_hours = sorted(pd.to_numeric(df.get("analysis_hour"), errors="coerce").dropna().astype(int).unique().tolist())
    if analysis_hours:
        filters["analysis_hours"] = st.sidebar.multiselect(
            "Analysis hours",
            options=analysis_hours,
            key=f"{key_prefix}_analysis_hours",
        )

    return filters


def apply_vend_filters(df: pd.DataFrame, filters: dict[str, Any]) -> pd.DataFrame:
    """Apply sidebar filters to the vend dataframe."""

    filtered = df.copy()

    for column in ("meterno", "servicepointno", "categorycode", "source_file"):
        values = filters.get(column)
        if values and column in filtered.columns:
            value_set = {str(value) for value in values}
            filtered = filtered[filtered[column].astype(str).isin(value_set)]

    if filters.get("transaction_range") and "transactionamount" in filtered.columns:
        low, high = filters["transaction_range"]
        series = pd.to_numeric(filtered["transactionamount"], errors="coerce")
        filtered = filtered[series.between(low, high, inclusive="both")]

    date_range = filters.get("date_range")
    if date_range and len(date_range) == 2 and "vend_date" in filtered.columns:
        start_date = pd.Timestamp(date_range[0])
        end_date = pd.Timestamp(date_range[1])
        vend_dates = pd.to_datetime(filtered["vend_date"], errors="coerce")
        filtered = filtered[vend_dates.between(start_date, end_date)]

    if filters.get("analysis_hours") and "analysis_hour" in filtered.columns:
        hours = set(filters["analysis_hours"])
        analysis_hour = pd.to_numeric(filtered["analysis_hour"], errors="coerce").astype("Int64")
        filtered = filtered[analysis_hour.isin(hours)]

    return filtered

