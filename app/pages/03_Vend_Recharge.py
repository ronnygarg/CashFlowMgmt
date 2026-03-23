"""Vend and recharge analysis page."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import streamlit as st

from src.charts import bar_chart, box_plot, histogram, line_chart
from src.constants import DATE_STATUS_PARSED, LIMITATION_VEND_DATETIME
from src.dashboard_data import load_dashboard_bundle
from src.filters import apply_vend_filters, render_vend_filters
from src.io_utils import dataframe_to_csv_bytes
from src.metrics import vend_metrics
from src.path_utils import extract_base_dir_arg, load_app_config

APP_CONFIG = load_app_config()
DISPLAY_CONFIG = APP_CONFIG.get("display", {})

st.set_page_config(
    page_title=f"{DISPLAY_CONFIG.get('app_title', 'CashFlowMgmt Dashboard')} - Vend",
    layout=DISPLAY_CONFIG.get("layout", "wide"),
    initial_sidebar_state=DISPLAY_CONFIG.get("sidebar_state", "expanded"),
)


def main() -> None:
    base_dir_arg = extract_base_dir_arg()
    bundle = load_dashboard_bundle(base_dir_override=base_dir_arg)
    vend_df = bundle["datasets"]["vend"]

    st.title("Vend / Recharge")
    st.caption("Standalone exploratory analysis of recharge transactions across combined vend source files.")
    st.warning(LIMITATION_VEND_DATETIME)

    filters = render_vend_filters(vend_df, key_prefix="vend_page")
    filtered_df = apply_vend_filters(vend_df, filters)
    metrics = vend_metrics(filtered_df)

    parse_status_series = filtered_df.get("issuedate_parse_status", pd.Series(dtype="string")).astype("string")
    parsed_rows = int((parse_status_series == DATE_STATUS_PARSED).sum())
    parse_coverage_pct = (parsed_rows / len(filtered_df) * 100) if len(filtered_df) else 0.0
    st.caption(f"Parsed full-datetime coverage in current selection: {parse_coverage_pct:.1f}% ({parsed_rows:,}/{len(filtered_df):,})")

    if filters.get("full_datetime_only"):
        st.info("Quality filter active: only rows with fully parsed vend datetime are included.")
    selected_parse_statuses = filters.get("issuedate_parse_status")
    if selected_parse_statuses and len(selected_parse_statuses) > 0:
        st.caption("Issuedate status filter applied: " + ", ".join(sorted(str(value) for value in selected_parse_statuses)))

    if filtered_df.empty:
        st.warning("No vend rows match the current filters.")
        return

    tabs = st.tabs(["Summary", "Transaction Analysis", "Category Analysis", "Meter / Service Point Drill-down", "Raw Table"])

    with tabs[0]:
        cards = st.columns(5)
        cards[0].metric("Total vend amount", f"{metrics['total_transaction_amount']:,.2f}")
        cards[1].metric("Transactions", f"{metrics['transactions']:,}")
        cards[2].metric("Average vend amount", f"{metrics['average_transaction_amount']:,.2f}")
        cards[3].metric("Unique meters", f"{metrics['unique_meters']:,}")
        cards[4].metric("Unique service points", f"{metrics['unique_service_points']:,}")

        summary_left, summary_right = st.columns(2)
        with summary_left:
            file_breakdown = pd.DataFrame(columns=["source_file", "transactionamount"])
            if {"source_file", "transactionamount"}.issubset(filtered_df.columns):
                file_breakdown = (
                    filtered_df.groupby("source_file", dropna=True)["transactionamount"]
                    .sum()
                    .sort_values(ascending=False)
                    .reset_index()
                )
            if file_breakdown.empty:
                st.warning("Source-file breakdown is unavailable because `source_file` or `transactionamount` is missing.")
            else:
                st.plotly_chart(
                    bar_chart(file_breakdown, x="source_file", y="transactionamount", title="Vend amount by source file"),
                    use_container_width=True,
                )
        with summary_right:
            if "transactionamount" not in filtered_df.columns:
                st.warning("Transaction distribution is unavailable because `transactionamount` is missing.")
            else:
                st.plotly_chart(
                    histogram(filtered_df, x="transactionamount", title="Distribution of transaction amounts"),
                    use_container_width=True,
                )

    with tabs[1]:
        transaction_left, transaction_right = st.columns(2)

        daily_amounts = pd.DataFrame(columns=["vend_date", "transactionamount"])
        hourly_amounts = pd.DataFrame(columns=["analysis_hour", "transactionamount"])
        if {"vend_date", "transactionamount"}.issubset(filtered_df.columns):
            daily_amounts = (
                filtered_df.dropna(subset=["vend_date"])
                .groupby("vend_date", dropna=True)["transactionamount"]
                .sum()
                .reset_index()
                .sort_values("vend_date")
            )
        if {"analysis_hour", "transactionamount"}.issubset(filtered_df.columns):
            hourly_amounts = (
                filtered_df.dropna(subset=["analysis_hour"])
                .groupby("analysis_hour", dropna=True)["transactionamount"]
                .sum()
                .reset_index()
                .sort_values("analysis_hour")
            )

        with transaction_left:
            if not daily_amounts.empty:
                st.plotly_chart(
                    line_chart(daily_amounts, x="vend_date", y="transactionamount", title="Daily vend amount trend"),
                    use_container_width=True,
                )
            elif not hourly_amounts.empty:
                st.plotly_chart(
                    bar_chart(hourly_amounts, x="analysis_hour", y="transactionamount", title="Time-of-day vend amount profile"),
                    use_container_width=True,
                )
            else:
                st.warning("No safe date or time fields are currently available for transaction trend analysis.")

        with transaction_right:
            if "transactionamount" not in filtered_df.columns:
                st.warning("Transaction spread is unavailable because `transactionamount` is missing.")
            else:
                st.plotly_chart(
                    box_plot(filtered_df, y="transactionamount", title="Transaction amount spread"),
                    use_container_width=True,
                )

    with tabs[2]:
        category_amounts = pd.DataFrame(columns=["categorycode", "sum", "count"])
        if {"categorycode", "transactionamount"}.issubset(filtered_df.columns):
            category_amounts = (
                filtered_df.groupby("categorycode", dropna=True)["transactionamount"]
                .agg(["sum", "count"])
                .reset_index()
                .sort_values("sum", ascending=False)
            )
        cat_left, cat_right = st.columns(2)
        with cat_left:
            if category_amounts.empty:
                st.warning("Category amount analysis is unavailable because `categorycode` or `transactionamount` is missing.")
            else:
                st.plotly_chart(
                    bar_chart(category_amounts, x="categorycode", y="sum", title="Total vend amount by category"),
                    use_container_width=True,
                )
        with cat_right:
            if category_amounts.empty:
                st.warning("Category count analysis is unavailable because `categorycode` or `transactionamount` is missing.")
            else:
                st.plotly_chart(
                    bar_chart(category_amounts, x="categorycode", y="count", title="Transaction count by category"),
                    use_container_width=True,
                )
        st.dataframe(category_amounts, use_container_width=True, hide_index=True)

    with tabs[3]:
        if "meterno" not in filtered_df.columns and "servicepointno" not in filtered_df.columns:
            st.warning("Drill-down is unavailable because both `meterno` and `servicepointno` are missing.")
        else:
            meter_options = sorted(filtered_df.get("meterno", pd.Series(dtype="string")).dropna().astype(str).unique().tolist())
            service_point_options = sorted(filtered_df.get("servicepointno", pd.Series(dtype="string")).dropna().astype(str).unique().tolist())
            drill_type = st.radio("Drill-down entity", options=["Meter", "Service Point"], horizontal=True)

            if drill_type == "Meter":
                if not meter_options:
                    st.warning("No meter values are available for vend drill-down.")
                    drill_df = pd.DataFrame()
                    label = "selected meter"
                else:
                    selected_value = st.selectbox("Choose a meter", options=meter_options)
                    drill_df = filtered_df[filtered_df["meterno"].astype(str) == str(selected_value)].copy()
                    label = f"meter {selected_value}"
            else:
                if not service_point_options:
                    st.warning("No service point values are available for vend drill-down.")
                    drill_df = pd.DataFrame()
                    label = "selected service point"
                else:
                    selected_value = st.selectbox("Choose a service point", options=service_point_options)
                    drill_df = filtered_df[filtered_df["servicepointno"].astype(str) == str(selected_value)].copy()
                    label = f"service point {selected_value}"

            if not drill_df.empty:
                amount_series = pd.to_numeric(drill_df.get("transactionamount", pd.Series(dtype=float)), errors="coerce")
                drill_cards = st.columns(4)
                drill_cards[0].metric("Rows", f"{len(drill_df):,}")
                drill_cards[1].metric("Total amount", f"{amount_series.sum():,.2f}")
                drill_cards[2].metric("Average amount", f"{amount_series.mean():,.2f}" if not amount_series.dropna().empty else "Unavailable")
                drill_cards[3].metric("Source files", f"{drill_df.get('source_file', pd.Series(dtype='string')).nunique(dropna=True):,}")

                source_breakdown = pd.DataFrame(columns=["source_file", "transactionamount"])
                if {"source_file", "transactionamount"}.issubset(drill_df.columns):
                    source_breakdown = (
                        drill_df.groupby("source_file", dropna=True)["transactionamount"]
                        .sum()
                        .reset_index()
                        .sort_values("transactionamount", ascending=False)
                    )
                if source_breakdown.empty:
                    st.warning("Source-file breakdown is unavailable for this drill-down selection.")
                else:
                    st.plotly_chart(
                        bar_chart(source_breakdown, x="source_file", y="transactionamount", title=f"Source-file vend amount for {label}"),
                        use_container_width=True,
                    )
                st.dataframe(drill_df.head(APP_CONFIG.get("filters", {}).get("defaults", {}).get("max_table_rows", 1000)), use_container_width=True, hide_index=True)

    with tabs[4]:
        st.download_button(
            "Download filtered vend CSV",
            data=dataframe_to_csv_bytes(filtered_df),
            file_name="filtered_vend.csv",
            mime="text/csv",
        )
        st.dataframe(
            filtered_df.head(APP_CONFIG.get("filters", {}).get("defaults", {}).get("max_table_rows", 1000)),
            use_container_width=True,
            hide_index=True,
        )


if __name__ == "__main__":
    main()
