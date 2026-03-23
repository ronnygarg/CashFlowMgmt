"""Consumption analysis page."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import streamlit as st

from src.charts import bar_chart, box_plot, histogram, line_chart
from src.dashboard_data import load_dashboard_bundle
from src.filters import apply_consumption_filters, render_consumption_filters
from src.io_utils import dataframe_to_csv_bytes
from src.metrics import consumption_metrics
from src.path_utils import extract_base_dir_arg, load_app_config

APP_CONFIG = load_app_config()
DISPLAY_CONFIG = APP_CONFIG.get("display", {})

st.set_page_config(
    page_title=f"{DISPLAY_CONFIG.get('app_title', 'CashFlowMgmt Dashboard')} - Consumption",
    layout=DISPLAY_CONFIG.get("layout", "wide"),
    initial_sidebar_state=DISPLAY_CONFIG.get("sidebar_state", "expanded"),
)


def main() -> None:
    base_dir_arg = extract_base_dir_arg()
    bundle = load_dashboard_bundle(base_dir_override=base_dir_arg)
    consumption_df = bundle["datasets"]["consumption"]

    st.title("Consumption")
    st.caption("Standalone exploratory analysis of electricity consumption. No vend linkage is attempted on this page.")

    filters = render_consumption_filters(consumption_df, key_prefix="consumption_page")
    filtered_df = apply_consumption_filters(consumption_df, filters)
    metrics = consumption_metrics(filtered_df)

    if filters.get("parsed_dates_only"):
        st.info("Quality filter active: only rows with parsed consumption dates are included.")

    if filtered_df.empty:
        st.warning("No consumption rows match the current filters.")
        return

    tabs = st.tabs(["Summary", "Trends", "Meter Drill-down", "Raw Table"])

    with tabs[0]:
        cards = st.columns(4)
        cards[0].metric("Total kWh consumption", f"{metrics['total_kwh_consumption']:,.2f}")
        cards[1].metric("Total kVAh consumption", f"{metrics['total_kvah_consumption']:,.2f}")
        cards[2].metric("Average daily kWh", f"{metrics['average_daily_kwh']:,.2f}")
        cards[3].metric("Unique meters", f"{metrics['unique_meters']:,}")

        top_meters = pd.DataFrame(columns=["mtrid", "kwh_consumption"])
        if {"mtrid", "kwh_consumption"}.issubset(filtered_df.columns):
            top_meters = (
                filtered_df.groupby("mtrid", dropna=True)["kwh_consumption"]
                .sum()
                .sort_values(ascending=False)
                .head(10)
                .reset_index()
            )
        chart_left, chart_right = st.columns(2)
        with chart_left:
            if top_meters.empty:
                st.warning("Top-meter ranking is unavailable because `mtrid` or `kwh_consumption` is missing.")
            else:
                st.plotly_chart(
                    bar_chart(
                        top_meters,
                        x="kwh_consumption",
                        y="mtrid",
                        title="Top meters by total kWh consumption",
                        orientation="h",
                    ),
                    use_container_width=True,
                )
        with chart_right:
            if "kwh_consumption" not in filtered_df.columns:
                st.warning("kWh distribution is unavailable because `kwh_consumption` is missing.")
            else:
                st.plotly_chart(
                    histogram(filtered_df, x="kwh_consumption", title="Distribution of kWh consumption"),
                    use_container_width=True,
                )

    with tabs[1]:
        daily = pd.DataFrame(columns=["date", "kwh_consumption"])
        if {"date", "kwh_consumption"}.issubset(filtered_df.columns):
            daily = (
                filtered_df.groupby("date", dropna=True)[["kwh_consumption"]]
                .sum(numeric_only=True)
                .reset_index()
                .sort_values("date")
            )
        chart_left, chart_right = st.columns(2)
        with chart_left:
            if daily.empty:
                st.warning("Daily trend is unavailable because `date` or `kwh_consumption` is missing.")
            else:
                st.plotly_chart(
                    line_chart(daily, x="date", y="kwh_consumption", title="Daily kWh consumption trend"),
                    use_container_width=True,
                )
        with chart_right:
            if "kwh_consumption" not in filtered_df.columns:
                st.warning("Consumption spread is unavailable because `kwh_consumption` is missing.")
            else:
                st.plotly_chart(
                    box_plot(filtered_df, y="kwh_consumption", title="kWh consumption spread"),
                    use_container_width=True,
                )

    with tabs[2]:
        if "mtrid" not in filtered_df.columns:
            st.warning("Meter drill-down is unavailable because `mtrid` is missing.")
        else:
            meter_options = sorted(filtered_df["mtrid"].dropna().astype(str).unique().tolist())
            if not meter_options:
                st.warning("No meter values are available for drill-down.")
            else:
                selected_meter = st.selectbox("Choose a meter", options=meter_options)
                meter_df = filtered_df[filtered_df["mtrid"].astype(str) == str(selected_meter)].copy()
                kwh_series = pd.to_numeric(meter_df.get("kwh_consumption", pd.Series(dtype=float)), errors="coerce")

                meter_cards = st.columns(4)
                meter_cards[0].metric("Rows", f"{len(meter_df):,}")
                meter_cards[1].metric("Total kWh", f"{kwh_series.sum():,.2f}")
                meter_cards[2].metric("Min daily kWh", f"{kwh_series.min():,.2f}" if not kwh_series.dropna().empty else "Unavailable")
                meter_cards[3].metric("Max daily kWh", f"{kwh_series.max():,.2f}" if not kwh_series.dropna().empty else "Unavailable")

                meter_daily = pd.DataFrame(columns=["date", "kwh_consumption"])
                if {"date", "kwh_consumption"}.issubset(meter_df.columns):
                    meter_daily = (
                        meter_df.groupby("date", dropna=True)[["kwh_consumption"]]
                        .sum(numeric_only=True)
                        .reset_index()
                        .sort_values("date")
                    )
                if meter_daily.empty:
                    st.warning("Meter trend is unavailable because `date` or `kwh_consumption` is missing.")
                else:
                    st.plotly_chart(
                        line_chart(meter_daily, x="date", y="kwh_consumption", title=f"Daily kWh trend for meter {selected_meter}"),
                        use_container_width=True,
                    )
                st.dataframe(meter_df.head(APP_CONFIG.get("filters", {}).get("defaults", {}).get("max_table_rows", 1000)), use_container_width=True, hide_index=True)

    with tabs[3]:
        st.download_button(
            "Download filtered consumption CSV",
            data=dataframe_to_csv_bytes(filtered_df),
            file_name="filtered_consumption.csv",
            mime="text/csv",
        )
        st.dataframe(
            filtered_df.head(APP_CONFIG.get("filters", {}).get("defaults", {}).get("max_table_rows", 1000)),
            use_container_width=True,
            hide_index=True,
        )


if __name__ == "__main__":
    main()
