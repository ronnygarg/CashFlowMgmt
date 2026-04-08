"""Network and spatial view page."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import streamlit as st

from src.charts import bar_chart, scatter_chart
from src.dashboard_data import load_dashboard_bundle
from src.filters import apply_dimension_filters, render_dimension_filters
from src.path_utils import extract_base_dir_arg, load_app_config

APP_CONFIG = load_app_config()
DISPLAY_CONFIG = APP_CONFIG.get("display", {})
MAX_TABLE_ROWS = APP_CONFIG.get("filters", {}).get("defaults", {}).get("max_table_rows", 1000)
MAX_MAP_POINTS = APP_CONFIG.get("data", {}).get("risk_thresholds", {}).get("max_map_points", 2000)

st.set_page_config(
    page_title=f"{DISPLAY_CONFIG.get('app_title', 'CashFlowMgmt Dashboard')} - Network Spatial",
    layout=DISPLAY_CONFIG.get("layout", "wide"),
    initial_sidebar_state=DISPLAY_CONFIG.get("sidebar_state", "expanded"),
)


def _group_network(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if df.empty or column not in df.columns:
        return pd.DataFrame(columns=[column, "consumer_count", "low_balance_count", "average_daily_kwh"])
    grouped = (
        df.dropna(subset=[column])
        .groupby(column, dropna=True)
        .agg(
            consumer_count=("consumer_master_id", "size"),
            low_balance_count=("low_balance_flag", lambda series: int(series.fillna(False).sum())),
            average_daily_kwh=("average_daily_kwh", "mean"),
        )
        .reset_index()
        .sort_values(["consumer_count", column], ascending=[False, True])
    )
    return grouped


def main() -> None:
    base_dir_arg = extract_base_dir_arg()
    bundle = load_dashboard_bundle(base_dir_override=base_dir_arg)
    consumer_summary = bundle["derived"]["datasets"]["consumer_summary"]

    st.title("Network / Spatial View")
    st.caption("Network hierarchy summaries by feeder, DT, substation, and area, with a GIS scatter when coordinates are sufficiently populated.")

    filters = render_dimension_filters(
        consumer_summary,
        app_config=APP_CONFIG,
        key_prefix="network_view",
        title="Network Filters",
        extra_columns=["balance_band"],
    )
    filtered_summary = apply_dimension_filters(consumer_summary, filters)

    if filtered_summary.empty:
        st.warning("No consumers match the current filters.")
        return

    metrics = st.columns(4)
    metrics[0].metric("Consumers in view", f"{len(filtered_summary):,}")
    metrics[1].metric(
        "Consumers with valid GIS",
        f"{int(filtered_summary.get('has_valid_gis', pd.Series(dtype='bool')).fillna(False).sum()):,}",
    )
    metrics[2].metric(
        "Low balance consumers",
        f"{int(filtered_summary.get('low_balance_flag', pd.Series(dtype='bool')).fillna(False).sum()):,}",
    )
    metrics[3].metric(
        "Missing feeder or DT",
        f"{int(filtered_summary.get('missing_network_attributes', pd.Series(dtype='bool')).fillna(False).sum()):,}",
    )

    by_substation = _group_network(filtered_summary, "substationname").head(10)
    by_feeder = _group_network(filtered_summary, "feedercode").head(10)
    by_dt = _group_network(filtered_summary, "dtcode").head(10)
    by_area = _group_network(filtered_summary, "area_type").head(10)

    top_left, top_right = st.columns(2)
    with top_left:
        st.plotly_chart(
            bar_chart(by_substation, x="substationname", y="consumer_count", title="Top substations by consumer count"),
            use_container_width=True,
        )
    with top_right:
        st.plotly_chart(
            bar_chart(by_feeder, x="feedercode", y="consumer_count", title="Top feeders by consumer count"),
            use_container_width=True,
        )

    mid_left, mid_right = st.columns(2)
    with mid_left:
        st.plotly_chart(
            bar_chart(by_dt, x="dtcode", y="consumer_count", title="Top DTs by consumer count"),
            use_container_width=True,
        )
    with mid_right:
        st.plotly_chart(
            bar_chart(by_area, x="area_type", y="consumer_count", title="Consumers by area type"),
            use_container_width=True,
        )

    st.subheader("Spatial Scatter")
    valid_gis = filtered_summary[filtered_summary.get("has_valid_gis", pd.Series(dtype="bool")).fillna(False)].copy()
    if len(valid_gis) >= 50:
        spatial_sample = valid_gis.head(MAX_MAP_POINTS)
        st.plotly_chart(
            scatter_chart(
                spatial_sample,
                x="gis_longitude",
                y="gis_latitude",
                color="area_type",
                hover_name="consumername",
                title="GIS latitude/longitude scatter",
            ),
            use_container_width=True,
        )
    else:
        st.info("GIS coverage is too weak in the current selection for a useful scatter. Use the network hierarchy tables below instead.")

    hierarchy_tab, detail_tab = st.tabs(["Hierarchy Tables", "Consumer Detail"])

    with hierarchy_tab:
        st.dataframe(by_substation, use_container_width=True, hide_index=True)
        st.dataframe(by_feeder, use_container_width=True, hide_index=True)
        st.dataframe(by_dt, use_container_width=True, hide_index=True)

    with detail_tab:
        st.dataframe(
            filtered_summary[
                [
                    "consumername",
                    "consumernumber",
                    "meterno",
                    "area_type",
                    "feedercode",
                    "dtcode",
                    "substationname",
                    "meterbalance",
                    "average_daily_kwh",
                    "low_balance_flag",
                    "has_valid_gis",
                ]
            ].head(MAX_TABLE_ROWS),
            use_container_width=True,
            hide_index=True,
        )


if __name__ == "__main__":
    main()
