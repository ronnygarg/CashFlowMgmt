"""Executive overview page."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import streamlit as st

from src.charts import bar_chart, histogram, line_chart
from src.constants import LIMITATION_NO_LINKING_KEY
from src.dashboard_data import load_dashboard_bundle
from src.path_utils import extract_base_dir_arg, load_app_config

APP_CONFIG = load_app_config()
DISPLAY_CONFIG = APP_CONFIG.get("display", {})
MAX_TABLE_ROWS = APP_CONFIG.get("filters", {}).get("defaults", {}).get("max_table_rows", 1000)

st.set_page_config(
    page_title=f"{DISPLAY_CONFIG.get('app_title', 'CashFlowMgmt Dashboard')} - Executive Overview",
    layout=DISPLAY_CONFIG.get("layout", "wide"),
    initial_sidebar_state=DISPLAY_CONFIG.get("sidebar_state", "expanded"),
)


def _top_counts(df: pd.DataFrame, column: str, count_label: str = "consumer_count", limit: int = 10) -> pd.DataFrame:
    if df.empty or column not in df.columns:
        return pd.DataFrame(columns=[column, count_label])
    grouped = (
        df.dropna(subset=[column])
        .groupby(column, dropna=True)
        .size()
        .reset_index(name=count_label)
        .sort_values([count_label, column], ascending=[False, True])
        .head(limit)
    )
    return grouped


def main() -> None:
    base_dir_arg = extract_base_dir_arg()
    bundle = load_dashboard_bundle(base_dir_override=base_dir_arg)

    consumer_summary = bundle["derived"]["datasets"]["consumer_summary"]
    vend_enriched = bundle["derived"]["datasets"]["vend_enriched"]
    consumption_enriched = bundle["derived"]["datasets"]["consumption_enriched"]
    exception_summary = bundle["derived"]["exception_summary"]
    portfolio_metrics = bundle["derived"]["portfolio_metrics"]
    vend_join = bundle["derived"]["join_coverage"]["vend"]["coverage"]
    consumption_join = bundle["derived"]["join_coverage"]["consumption"]["coverage"]

    st.title("Executive Overview")
    st.caption(
        "Business-facing summary of portfolio coverage, balances, recharge behaviour, daily consumption, "
        "and the highest-signal operational exceptions."
    )
    st.warning(LIMITATION_NO_LINKING_KEY)

    metrics = st.columns(6)
    metrics[0].metric("Consumers", f"{portfolio_metrics['total_consumers_in_master']:,}")
    metrics[1].metric("Consumers with vend", f"{portfolio_metrics['consumers_with_vend_records']:,}")
    metrics[2].metric("Consumers with consumption", f"{portfolio_metrics['consumers_with_consumption_records']:,}")
    metrics[3].metric("Vend match rate", f"{portfolio_metrics['vend_match_rate_pct']:.1f}%")
    metrics[4].metric("Consumption match rate", f"{portfolio_metrics['consumption_match_rate_pct']:.1f}%")
    metrics[5].metric("Consumers with GIS", f"{portfolio_metrics['consumers_with_gis']:,}")

    balance_cards = st.columns(4)
    balance_cards[0].metric(
        "Critical balance",
        f"{int(consumer_summary.get('critical_balance_flag', pd.Series(dtype='bool')).fillna(False).sum()):,}",
    )
    balance_cards[1].metric(
        "Low balance with consumption",
        f"{int(consumer_summary.get('low_balance_with_consumption', pd.Series(dtype='bool')).fillna(False).sum()):,}",
    )
    balance_cards[2].metric(
        "Missing feeder or DT",
        f"{int(consumer_summary.get('missing_network_attributes', pd.Series(dtype='bool')).fillna(False).sum()):,}",
    )
    balance_cards[3].metric(
        "Master only, no activity",
        f"{int(consumer_summary.get('master_without_activity', pd.Series(dtype='bool')).fillna(False).sum()):,}",
    )

    join_left, join_right = st.columns(2)
    with join_left:
        st.subheader("Vend Join Coverage")
        st.dataframe(vend_join, use_container_width=True, hide_index=True)
    with join_right:
        st.subheader("Consumption Join Coverage")
        st.dataframe(consumption_join, use_container_width=True, hide_index=True)

    balance_left, balance_right = st.columns(2)
    with balance_left:
        st.subheader("Balance Distribution")
        balance_band_counts = _top_counts(consumer_summary, "balance_band", count_label="consumer_count")
        st.plotly_chart(
            bar_chart(balance_band_counts, x="balance_band", y="consumer_count", title="Consumers by balance band"),
            use_container_width=True,
        )
    with balance_right:
        st.subheader("Meter Balance Spread")
        st.plotly_chart(
            histogram(consumer_summary.dropna(subset=["meterbalance"]), x="meterbalance", title="Meter balance distribution"),
            use_container_width=True,
        )

    vend_left, vend_right = st.columns(2)
    with vend_left:
        st.subheader("Vend Summary")
        top_vend = (
            consumer_summary.dropna(subset=["vend_total_amount"])
            .sort_values("vend_total_amount", ascending=False)
            .head(10)[["consumername", "consumernumber", "vend_total_amount", "vend_transaction_count"]]
        )
        st.dataframe(top_vend, use_container_width=True, hide_index=True)
    with vend_right:
        st.subheader("Exception Counts")
        st.dataframe(exception_summary, use_container_width=True, hide_index=True)

    consumption_daily = pd.DataFrame()
    if {"date", "kwh_consumption", "kvah_consumption"}.issubset(consumption_enriched.columns):
        consumption_daily = (
            consumption_enriched.groupby("date", dropna=True)[["kwh_consumption", "kvah_consumption"]]
            .sum(numeric_only=True)
            .reset_index()
            .sort_values("date")
        )

    trend_left, trend_right = st.columns(2)
    with trend_left:
        st.subheader("Daily kWh Trend")
        st.plotly_chart(
            line_chart(consumption_daily, x="date", y="kwh_consumption", title="Daily kWh consumption"),
            use_container_width=True,
        )
    with trend_right:
        st.subheader("Top Average Daily Usage")
        top_consumers = (
            consumer_summary.dropna(subset=["average_daily_kwh"])
            .sort_values("average_daily_kwh", ascending=False)
            .head(10)[["consumername", "consumernumber", "average_daily_kwh", "meterbalance"]]
        )
        st.dataframe(top_consumers, use_container_width=True, hide_index=True)

    slice_cols = st.columns(3)
    top_tariff = _top_counts(consumer_summary, "tariffcode")
    top_area = _top_counts(consumer_summary, "area_type")
    top_feeder = _top_counts(consumer_summary, "feedercode")

    with slice_cols[0]:
        st.subheader("Top Tariff Slices")
        st.plotly_chart(bar_chart(top_tariff, x="tariffcode", y="consumer_count", title="Consumers by tariff"), use_container_width=True)
    with slice_cols[1]:
        st.subheader("Top Area Slices")
        st.plotly_chart(bar_chart(top_area, x="area_type", y="consumer_count", title="Consumers by area"), use_container_width=True)
    with slice_cols[2]:
        st.subheader("Top Feeder Slices")
        st.plotly_chart(
            bar_chart(top_feeder, x="feedercode", y="consumer_count", title="Consumers by feeder"),
            use_container_width=True,
        )

    st.subheader("Low Balance Watchlist")
    low_balance_watchlist = (
        consumer_summary[consumer_summary.get("watch_balance_flag", pd.Series(dtype="bool")).fillna(False)]
        .sort_values(["meterbalance", "average_daily_kwh"], ascending=[True, False])
        .head(MAX_TABLE_ROWS)
    )
    st.dataframe(
        low_balance_watchlist[
            [
                "consumername",
                "consumernumber",
                "meterno",
                "meterbalance",
                "days_since_balance_update",
                "average_daily_kwh",
                "vend_transaction_count",
                "feedercode",
                "dtcode",
                "substationname",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )


if __name__ == "__main__":
    main()
