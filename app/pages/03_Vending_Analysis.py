"""Vending analysis page."""

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
from src.filters import apply_dimension_filters, render_dimension_filters
from src.io_utils import dataframe_to_csv_bytes
from src.path_utils import extract_base_dir_arg, load_app_config

APP_CONFIG = load_app_config()
DISPLAY_CONFIG = APP_CONFIG.get("display", {})
MAX_TABLE_ROWS = APP_CONFIG.get("filters", {}).get("defaults", {}).get("max_table_rows", 1000)

st.set_page_config(
    page_title=f"{DISPLAY_CONFIG.get('app_title', 'CashFlowMgmt Dashboard')} - Vending Analysis",
    layout=DISPLAY_CONFIG.get("layout", "wide"),
    initial_sidebar_state=DISPLAY_CONFIG.get("sidebar_state", "expanded"),
)


def _prepare_vend_view(vend_df: pd.DataFrame) -> pd.DataFrame:
    view = vend_df.copy()
    view["transactionamount_numeric"] = pd.to_numeric(view.get("transactionamount"), errors="coerce")
    for source, target in (
        ("master_tariffcode", "tariffcode"),
        ("master_area_type", "area_type"),
        ("master_connection_type", "connection_type"),
        ("master_accountingmode", "accountingmode"),
        ("master_feedercode", "feedercode"),
        ("master_dtcode", "dtcode"),
        ("master_meterbalance", "meterbalance"),
    ):
        if source in view.columns:
            view[target] = view[source]
    return view


def main() -> None:
    base_dir_arg = extract_base_dir_arg()
    bundle = load_dashboard_bundle(base_dir_override=base_dir_arg)

    vend_enriched = _prepare_vend_view(bundle["derived"]["datasets"]["vend_enriched"])
    consumer_summary = bundle["derived"]["datasets"]["consumer_summary"]
    timestamp_quality = bundle["derived"]["vend_timestamp_quality"]

    st.title("Vending Analysis")
    st.caption("Recharge behaviour, transaction distributions, join coverage, unmatched records, and practical vend segmentation.")
    st.info(timestamp_quality["note"])

    filter_date_column = "vend_date" if timestamp_quality["supports_daily_analysis"] else None
    filters = render_dimension_filters(
        vend_enriched,
        app_config=APP_CONFIG,
        key_prefix="vend_analysis",
        title="Vending Filters",
        date_column=filter_date_column,
        date_label="Vend date range",
        extra_columns=["join_coverage_status", "resolution_status"],
    )
    filtered_vend = apply_dimension_filters(vend_enriched, filters)

    if filtered_vend.empty:
        st.warning("No vend rows match the current filters.")
        return

    balance_updated = pd.to_datetime(filtered_vend.get("master_balanceupdatedon_parsed"), errors="coerce")
    stale_threshold_days = int(APP_CONFIG.get("data", {}).get("risk_thresholds", {}).get("stale_balance_warning_days", 30))
    stale_or_missing_balance = balance_updated.isna() | ((pd.Timestamp.now().normalize() - balance_updated.dt.floor("D")).dt.days > stale_threshold_days)

    cards = st.columns(6)
    cards[0].metric("Transactions", f"{len(filtered_vend):,}")
    cards[1].metric("Total amount", f"{filtered_vend['transactionamount_numeric'].sum():,.2f}")
    cards[2].metric("Average amount", f"{filtered_vend['transactionamount_numeric'].mean():,.2f}")
    cards[3].metric("Median amount", f"{filtered_vend['transactionamount_numeric'].median():,.2f}")
    cards[4].metric(
        "Unmatched rows",
        f"{int((filtered_vend.get('join_coverage_status', pd.Series(dtype='string')) == 'unmatched').sum()):,}",
    )
    cards[5].metric(
        "Rows with stale or missing balance update",
        f"{int(stale_or_missing_balance.fillna(False).sum()):,}",
    )

    top_left, top_right = st.columns(2)
    with top_left:
        st.plotly_chart(
            histogram(filtered_vend, x="transactionamount_numeric", title="Vend amount distribution"),
            use_container_width=True,
        )
    with top_right:
        st.plotly_chart(
            box_plot(filtered_vend, y="transactionamount_numeric", title="Vend amount spread"),
            use_container_width=True,
        )

    customer_vend = (
        filtered_vend.dropna(subset=["resolved_master_id"])
        .groupby("resolved_master_id", dropna=True)["transactionamount_numeric"]
        .agg(["count", "sum"])
        .reset_index()
        .rename(columns={"count": "vend_count", "sum": "vend_amount"})
    )
    customer_vend = customer_vend.merge(
        consumer_summary[["consumer_master_id", "consumername", "consumernumber", "meterno"]],
        how="left",
        left_on="resolved_master_id",
        right_on="consumer_master_id",
    )

    segmentation = pd.DataFrame()
    if not customer_vend.empty:
        segmentation = customer_vend.copy()
        segmentation["segment"] = pd.cut(
            segmentation["vend_count"],
            bins=[0, 1, 3, 7, float("inf")],
            labels=["1 transaction", "2-3 transactions", "4-7 transactions", "8+ transactions"],
            include_lowest=True,
        )
        segmentation = (
            segmentation.groupby("segment", dropna=False)
            .size()
            .reset_index(name="customer_count")
            .sort_values("customer_count", ascending=False)
        )

    mid_left, mid_right = st.columns(2)
    with mid_left:
        top_customers_amount = customer_vend.sort_values("vend_amount", ascending=False).head(10)
        st.plotly_chart(
            bar_chart(top_customers_amount, x="consumername", y="vend_amount", title="Top customers by vend amount"),
            use_container_width=True,
        )
    with mid_right:
        top_customers_count = customer_vend.sort_values("vend_count", ascending=False).head(10)
        st.plotly_chart(
            bar_chart(top_customers_count, x="consumername", y="vend_count", title="Top customers by transaction count"),
            use_container_width=True,
        )

    trend_left, trend_right = st.columns(2)
    with trend_left:
        if timestamp_quality["supports_daily_analysis"] and {"vend_date", "transactionamount"}.issubset(filtered_vend.columns):
            daily_vend = (
                filtered_vend.dropna(subset=["vend_date"])
                .groupby("vend_date", dropna=True)["transactionamount_numeric"]
                .sum()
                .reset_index()
                .sort_values("vend_date")
            )
            st.plotly_chart(
                line_chart(daily_vend, x="vend_date", y="transactionamount_numeric", title="Daily vend amount"),
                use_container_width=True,
            )
        else:
            hourly = (
                filtered_vend.dropna(subset=["analysis_hour"])
                .groupby("analysis_hour", dropna=True)["transactionamount_numeric"]
                .sum()
                .reset_index()
                .sort_values("analysis_hour")
            )
            st.plotly_chart(
                bar_chart(hourly, x="analysis_hour", y="transactionamount_numeric", title="Time-of-day vend amount"),
                use_container_width=True,
            )
    with trend_right:
        st.plotly_chart(
            bar_chart(segmentation, x="segment", y="customer_count", title="Top-up behaviour segments"),
            use_container_width=True,
        )

    unmatched = filtered_vend[filtered_vend.get("join_coverage_status", pd.Series(dtype="string")) == "unmatched"].copy()

    unmatched_tab, customer_tab, raw_tab = st.tabs(["Unmatched Vend", "Customer Summary", "Raw Rows"])

    with unmatched_tab:
        st.dataframe(unmatched.head(MAX_TABLE_ROWS), use_container_width=True, hide_index=True)

    with customer_tab:
        st.dataframe(
            customer_vend.sort_values("vend_amount", ascending=False).head(MAX_TABLE_ROWS),
            use_container_width=True,
            hide_index=True,
        )

    with raw_tab:
        st.download_button(
            "Download filtered vend rows",
            data=dataframe_to_csv_bytes(filtered_vend),
            file_name="filtered_vend_analysis.csv",
            mime="text/csv",
        )
        st.dataframe(filtered_vend.head(MAX_TABLE_ROWS), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
