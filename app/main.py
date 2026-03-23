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
from src.metrics import overview_metrics
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
        clear_dashboard_cache()
        st.rerun()

    st.sidebar.caption("Base directory precedence: CLI argument, environment variable, config file, fallback default.")
    if base_dir_arg:
        st.sidebar.info(f"CLI base dir override detected: {base_dir_arg}")

    return base_dir_arg


def main() -> None:
    """Render the dashboard landing page."""

    base_dir_arg = render_sidebar_controls()
    bundle = load_dashboard_bundle(base_dir_override=base_dir_arg)
    metrics = overview_metrics(
        consumption_df=bundle["datasets"]["consumption"],
        vend_df=bundle["datasets"]["vend"],
        file_inventory=bundle["file_inventory"],
    )

    st.title(DISPLAY_CONFIG.get("app_title", "CashFlowMgmt Dashboard"))
    st.caption("Exploratory dashboard for electricity consumption and vend analysis with modular ingestion and first-class data quality diagnostics.")

    st.warning(LIMITATION_NO_LINKING_KEY)
    st.info(LIMITATION_VEND_DATETIME)

    if hasattr(st, "page_link"):
        links = st.columns(3)
        with links[0]:
            st.page_link("pages/01_Overview.py", label="Open Overview")
        with links[1]:
            st.page_link("pages/04_Data_Quality.py", label="Start with Data Quality")
        with links[2]:
            st.page_link("pages/05_Combined_Analysis_Future.py", label="See Combined Analysis Placeholder")

    if bundle.get("missing_directories"):
        st.error("Some expected project directories are missing.")
        st.dataframe(pd.DataFrame({"missing_path": bundle["missing_directories"]}), use_container_width=True, hide_index=True)

    for message in bundle.get("messages", []):
        if message.startswith("Failed"):
            st.error(message)
        elif message.startswith("No valid"):
            st.warning(message)
        else:
            st.success(message)

    top_metrics = st.columns(4)
    top_metrics[0].metric("Raw files discovered", f"{metrics['file_count']:,}")
    top_metrics[1].metric("Consumption rows", f"{metrics['consumption']['rows']:,}")
    top_metrics[2].metric("Vend rows", f"{metrics['vend']['rows']:,}")
    top_metrics[3].metric("Base dir source", bundle["paths"].base_dir_source.replace("_", " "))

    detail_metrics = st.columns(4)
    detail_metrics[0].metric("Consumption files", f"{metrics['consumption_files']:,}")
    detail_metrics[1].metric("Vend files", f"{metrics['vend_files']:,}")
    detail_metrics[2].metric("Unique consumption meters", f"{metrics['consumption']['unique_meters']:,}")
    detail_metrics[3].metric("Unique vend service points", f"{metrics['vend']['unique_service_points']:,}")

    status_col, path_col = st.columns([1, 1])
    with status_col:
        st.subheader("Current Run")
        st.dataframe(
            pd.DataFrame(
                [
                    {"setting": "Resolved base directory", "value": str(bundle["paths"].base_dir)},
                    {"setting": "Base directory source", "value": bundle["paths"].base_dir_source},
                    {"setting": "Consumption output", "value": bundle["processed_outputs"]["consumption"]},
                    {"setting": "Vend output", "value": bundle["processed_outputs"]["vend"]},
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )

    with path_col:
        st.subheader("Next Steps")
        st.markdown(
            "\n".join(
                [
                    "- Use the Overview page for project-level context and ingest summaries.",
                    "- Use the Data Quality page early. It is the best place to inspect schema, parse coverage, and current limitations.",
                    "- Keep combined analysis disabled until a validated bridge table or common key becomes available.",
                ]
            )
        )

    st.subheader("File Inventory")
    st.dataframe(bundle["file_inventory"], use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()

