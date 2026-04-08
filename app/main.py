"""Streamlit entrypoint for the Cash Flow Management dashboard."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import streamlit as st

from src.constants import LIMITATION_NO_LINKING_KEY, LIMITATION_VEND_DATETIME
from src.dashboard_data import clear_dashboard_cache, load_dashboard_bundle
from src.path_utils import extract_base_dir_arg, load_app_config

APP_CONFIG = load_app_config()
DISPLAY_CONFIG = APP_CONFIG.get("display", {})

st.set_page_config(
    page_title=DISPLAY_CONFIG.get("app_title", "CashFlowMgmt Dashboard"),
    layout=DISPLAY_CONFIG.get("layout", "wide"),
    initial_sidebar_state=DISPLAY_CONFIG.get("sidebar_state", "expanded"),
)


def render_sidebar_controls() -> str | None:
    """Render global sidebar controls and return any CLI base-dir override."""

    st.sidebar.title("Dashboard Controls")
    base_dir_arg = extract_base_dir_arg()

    if st.sidebar.button("Refresh Raw Ingestion", use_container_width=True):
        st.session_state["force_raw_refresh"] = True
        clear_dashboard_cache()
        st.rerun()

    st.sidebar.caption("Base directory precedence: CLI argument, environment variable, config file, fallback default.")
    if base_dir_arg:
        st.sidebar.info(f"CLI base dir override detected: {base_dir_arg}")

    return base_dir_arg


def main() -> None:
    """Render the dashboard landing page."""

    base_dir_arg = render_sidebar_controls()
    force_raw_refresh = bool(st.session_state.get("force_raw_refresh", False))
    bundle = load_dashboard_bundle(base_dir_override=base_dir_arg, force_raw_refresh=force_raw_refresh)
    if force_raw_refresh:
        st.session_state["force_raw_refresh"] = False
    portfolio_metrics = bundle["derived"]["portfolio_metrics"]
    exception_summary = bundle["derived"]["exception_summary"]
    vend_timestamp_quality = bundle["derived"]["vend_timestamp_quality"]

    st.title(DISPLAY_CONFIG.get("app_title", "CashFlowMgmt Dashboard"))
    st.caption(
        "Operational dashboard for consumer portfolio context, recharge behaviour, daily consumption, "
        "reconciliation quality, and practical exception monitoring."
    )

    st.warning(LIMITATION_NO_LINKING_KEY)
    st.info(LIMITATION_VEND_DATETIME)
    st.info(vend_timestamp_quality["note"])

    if hasattr(st, "page_link"):
        links = st.columns(4)
        with links[0]:
            st.page_link("pages/01_Executive_Overview.py", label="Executive Overview")
        with links[1]:
            st.page_link("pages/02_Consumer_Explorer.py", label="Consumer Explorer")
        with links[2]:
            st.page_link("pages/03_Vending_Analysis.py", label="Vending Analysis")
        with links[3]:
            st.page_link("pages/06_Data_Quality_Reconciliation.py", label="Data Quality")

    if bundle.get("missing_directories"):
        st.error("Some expected project directories are missing.")
        st.dataframe(
            pd.DataFrame({"missing_path": bundle["missing_directories"]}),
            use_container_width=True,
            hide_index=True,
        )

    for message in bundle.get("messages", []):
        if message.startswith("Failed"):
            st.error(message)
        elif message.startswith("No valid"):
            st.warning(message)

    top_metrics = st.columns(6)
    top_metrics[0].metric("Master consumers", f"{portfolio_metrics['total_consumers_in_master']:,}")
    top_metrics[1].metric("Master meters", f"{portfolio_metrics['total_distinct_meters_in_master']:,}")
    top_metrics[2].metric("Vend transactions", f"{portfolio_metrics['total_vend_transactions']:,}")
    top_metrics[3].metric("Consumption rows", f"{portfolio_metrics['total_consumption_rows']:,}")
    top_metrics[4].metric("Vend match rate", f"{portfolio_metrics['vend_match_rate_pct']:.1f}%")
    top_metrics[5].metric("Consumption match rate", f"{portfolio_metrics['consumption_match_rate_pct']:.1f}%")

    detail_metrics = st.columns(4)
    detail_metrics[0].metric("Consumers with vend", f"{portfolio_metrics['consumers_with_vend_records']:,}")
    detail_metrics[1].metric("Consumers with consumption", f"{portfolio_metrics['consumers_with_consumption_records']:,}")
    detail_metrics[2].metric("Consumers with GIS", f"{portfolio_metrics['consumers_with_gis']:,}")
    detail_metrics[3].metric(
        "Critical balance exceptions",
        f"{int(exception_summary.loc[exception_summary['exception_type'] == 'Critical balance customers', 'consumer_count'].sum()):,}",
    )

    current_run, next_steps = st.columns([1.1, 0.9])
    with current_run:
        st.subheader("Current Run")
        st.dataframe(
            pd.DataFrame(
                [
                    {"setting": "Resolved base directory", "value": str(bundle["paths"].base_dir)},
                    {"setting": "Base directory source", "value": bundle["paths"].base_dir_source},
                    {"setting": "Load mode", "value": bundle.get("load_mode", "unknown")},
                    {"setting": "Consumer Master output", "value": bundle["processed_outputs"]["consumer_master"]},
                    {"setting": "Vend output", "value": bundle["processed_outputs"]["vend"]},
                    {"setting": "Consumption output", "value": bundle["processed_outputs"]["consumption"]},
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )

    with next_steps:
        st.subheader("Recommended Flow")
        st.markdown(
            "\n".join(
                [
                    "- Start with Executive Overview for portfolio health, join coverage, and exception counts.",
                    "- Use Consumer Explorer for account-level diagnostics across master, vend, and consumption.",
                    "- Use Data Quality early if you need to validate missing keys, duplicate identifiers, or timestamp quality.",
                ]
            )
        )

    st.subheader("Source Files in Use")
    st.dataframe(bundle["file_inventory"], use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
