"""Consumption analysis page."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import plotly.express as px
import streamlit as st

from src.charts import bar_chart, histogram, line_chart
from src.dashboard_data import load_dashboard_bundle
from src.filters import apply_dimension_filters, render_dimension_filters
from src.io_utils import dataframe_to_csv_bytes
from src.path_utils import extract_base_dir_arg, load_app_config

APP_CONFIG = load_app_config()
DISPLAY_CONFIG = APP_CONFIG.get("display", {})
MAX_TABLE_ROWS = APP_CONFIG.get("filters", {}).get("defaults", {}).get("max_table_rows", 1000)

st.set_page_config(
    page_title=f"{DISPLAY_CONFIG.get('app_title', 'CashFlowMgmt Dashboard')} - Consumption Analysis",
    layout=DISPLAY_CONFIG.get("layout", "wide"),
    initial_sidebar_state=DISPLAY_CONFIG.get("sidebar_state", "expanded"),
)


def _prepare_consumption_view(consumption_df: pd.DataFrame) -> pd.DataFrame:
    view = consumption_df.copy()
    for source, target in (
        ("master_tariffcode", "tariffcode"),
        ("master_area_type", "area_type"),
        ("master_connection_type", "connection_type"),
        ("master_accountingmode", "accountingmode"),
        ("master_feedercode", "feedercode_master"),
        ("master_dtcode", "dtcode_master"),
    ):
        if source in view.columns:
            view[target] = view[source]
    return view


def main() -> None:
    base_dir_arg = extract_base_dir_arg()
    bundle = load_dashboard_bundle(base_dir_override=base_dir_arg)

    consumption_enriched = _prepare_consumption_view(bundle["derived"]["datasets"]["consumption_enriched"])
    consumer_summary = bundle["derived"]["datasets"]["consumer_summary"]

    st.title("Consumption Analysis")
    st.caption("Daily consumption trends, customer usage distributions, suspicious cases, and network drill-downs.")

    filters = render_dimension_filters(
        consumption_enriched,
        app_config=APP_CONFIG,
        key_prefix="consumption_analysis",
        title="Consumption Filters",
        date_column="date",
        date_label="Consumption date range",
        extra_columns=["join_coverage_status", "resolution_status", "feedercode_master", "dtcode_master"],
    )
    filtered_consumption = apply_dimension_filters(consumption_enriched, filters)

    if filtered_consumption.empty:
        st.warning("No consumption rows match the current filters.")
        return

    kwh = pd.to_numeric(filtered_consumption.get("kwh_consumption"), errors="coerce")
    kvah = pd.to_numeric(filtered_consumption.get("kvah_consumption"), errors="coerce")

    cards = st.columns(6)
    cards[0].metric("Rows", f"{len(filtered_consumption):,}")
    cards[1].metric("Average daily kWh", f"{kwh.mean():,.2f}")
    cards[2].metric("Average daily kVAh", f"{kvah.mean():,.2f}")
    cards[3].metric("Distinct consumers", f"{filtered_consumption.get('consumernumber_normalized', pd.Series(dtype='string')).nunique(dropna=True):,}")
    cards[4].metric("Zero kWh rows", f"{int((kwh == 0).sum()):,}")
    cards[5].metric("Negative kWh rows", f"{int((kwh < 0).sum()):,}")

    daily = (
        filtered_consumption.groupby("date", dropna=True)[["kwh_consumption", "kvah_consumption"]]
        .sum(numeric_only=True)
        .reset_index()
        .sort_values("date")
    )
    if not daily.empty:
        daily["rolling_7d_kwh"] = daily["kwh_consumption"].rolling(7, min_periods=1).mean()

    trend_left, trend_right = st.columns(2)
    with trend_left:
        st.plotly_chart(
            line_chart(daily, x="date", y="kwh_consumption", title="Daily kWh consumption"),
            use_container_width=True,
        )
    with trend_right:
        if not daily.empty:
            rolling_fig = px.line(daily, x="date", y=["kwh_consumption", "rolling_7d_kwh"], markers=True)
            rolling_fig.update_layout(template="plotly_white", title="Daily kWh and 7-day rolling average")
            st.plotly_chart(rolling_fig, use_container_width=True)

    customer_usage = (
        filtered_consumption.dropna(subset=["resolved_master_id"])
        .groupby("resolved_master_id", dropna=True)["kwh_consumption"]
        .mean()
        .reset_index(name="average_daily_kwh")
        .merge(
            consumer_summary[["consumer_master_id", "consumername", "consumernumber", "meterbalance"]],
            how="left",
            left_on="resolved_master_id",
            right_on="consumer_master_id",
        )
    )

    usage_left, usage_right = st.columns(2)
    with usage_left:
        st.plotly_chart(
            histogram(customer_usage, x="average_daily_kwh", title="Customer average daily kWh distribution"),
            use_container_width=True,
        )
    with usage_right:
        top_consumers = customer_usage.sort_values("average_daily_kwh", ascending=False).head(10)
        st.plotly_chart(
            bar_chart(top_consumers, x="consumername", y="average_daily_kwh", title="Top customers by average daily kWh"),
            use_container_width=True,
        )

    network_left, network_right = st.columns(2)
    with network_left:
        by_substation = (
            filtered_consumption.dropna(subset=["substationname"])
            .groupby("substationname", dropna=True)["kwh_consumption"]
            .mean()
            .reset_index(name="average_daily_kwh")
            .sort_values("average_daily_kwh", ascending=False)
            .head(10)
        )
        st.plotly_chart(
            bar_chart(by_substation, x="substationname", y="average_daily_kwh", title="Average daily kWh by substation"),
            use_container_width=True,
        )
    with network_right:
        by_voltage = (
            filtered_consumption.dropna(subset=["supplyvoltage"])
            .groupby("supplyvoltage", dropna=True)["kwh_consumption"]
            .mean()
            .reset_index(name="average_daily_kwh")
            .sort_values("average_daily_kwh", ascending=False)
        )
        st.plotly_chart(
            bar_chart(by_voltage, x="supplyvoltage", y="average_daily_kwh", title="Average daily kWh by supply voltage"),
            use_container_width=True,
        )

    suspicious = filtered_consumption[(kwh <= 0) | kvah.lt(0) | kwh.isna()].copy()

    suspicious_tab, customer_tab, raw_tab = st.tabs(["Suspicious Cases", "Customer Usage", "Raw Rows"])

    with suspicious_tab:
        st.dataframe(suspicious.head(MAX_TABLE_ROWS), use_container_width=True, hide_index=True)

    with customer_tab:
        st.dataframe(
            customer_usage.sort_values("average_daily_kwh", ascending=False).head(MAX_TABLE_ROWS),
            use_container_width=True,
            hide_index=True,
        )

    with raw_tab:
        st.download_button(
            "Download filtered consumption rows",
            data=dataframe_to_csv_bytes(filtered_consumption),
            file_name="filtered_consumption_analysis.csv",
            mime="text/csv",
        )
        st.dataframe(filtered_consumption.head(MAX_TABLE_ROWS), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
