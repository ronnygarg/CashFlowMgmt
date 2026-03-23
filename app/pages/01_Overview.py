"""Project-level overview page."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import streamlit as st

from src.constants import LIMITATION_NO_LINKING_KEY
from src.dashboard_data import load_dashboard_bundle
from src.metrics import overview_metrics
from src.path_utils import extract_base_dir_arg, load_app_config

APP_CONFIG = load_app_config()
DISPLAY_CONFIG = APP_CONFIG.get("display", {})

st.set_page_config(
    page_title=f"{DISPLAY_CONFIG.get('app_title', 'CashFlowMgmt Dashboard')} - Overview",
    layout=DISPLAY_CONFIG.get("layout", "wide"),
    initial_sidebar_state=DISPLAY_CONFIG.get("sidebar_state", "expanded"),
)


def _format_span(span: dict[str, object]) -> str:
    if not span.get("min") or not span.get("max"):
        return "Unavailable"
    return f"{pd.Timestamp(span['min']).date()} to {pd.Timestamp(span['max']).date()}"


def main() -> None:
    base_dir_arg = extract_base_dir_arg()
    bundle = load_dashboard_bundle(base_dir_override=base_dir_arg)
    metrics = overview_metrics(
        consumption_df=bundle["datasets"]["consumption"],
        vend_df=bundle["datasets"]["vend"],
        file_inventory=bundle["file_inventory"],
    )

    st.title("Overview")
    st.caption("Project-level landing page for file coverage, row counts, temporal coverage, and current limitations.")
    st.warning(LIMITATION_NO_LINKING_KEY)
    st.info("Data Quality is a first-class feature in this project. Review that page before drawing strong conclusions from the exploratory charts.")

    cards = st.columns(6)
    cards[0].metric("Raw files", f"{metrics['file_count']:,}")
    cards[1].metric("Consumption rows", f"{metrics['consumption']['rows']:,}")
    cards[2].metric("Vend rows", f"{metrics['vend']['rows']:,}")
    cards[3].metric("Consumption meters", f"{metrics['consumption']['unique_meters']:,}")
    cards[4].metric("Vend meters", f"{metrics['vend']['unique_meters']:,}")
    cards[5].metric("Vend service points", f"{metrics['vend']['unique_service_points']:,}")

    summary_left, summary_right = st.columns([1.1, 0.9])
    with summary_left:
        st.subheader("Dataset Coverage")
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "dataset": "Consumption",
                        "files": metrics["consumption_files"],
                        "rows": metrics["consumption"]["rows"],
                        "date_span": _format_span(metrics["consumption"]["date_span"]),
                    },
                    {
                        "dataset": "Vend / Recharge",
                        "files": metrics["vend_files"],
                        "rows": metrics["vend"]["rows"],
                        "date_span": _format_span(metrics["vend"]["date_span"]),
                    },
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )

        st.subheader("Known Limitations")
        for item in APP_CONFIG.get("data", {}).get("known_limitations", []):
            st.markdown(f"- {item}")

    with summary_right:
        st.subheader("Processed Outputs")
        st.dataframe(
            pd.DataFrame(
                [
                    {"artifact": "Consumption Parquet", "path": bundle["processed_outputs"]["consumption"]},
                    {"artifact": "Vend Parquet", "path": bundle["processed_outputs"]["vend"]},
                    {"artifact": "Resolved base directory", "path": str(bundle["paths"].base_dir)},
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Ingest Summary by File")
    st.dataframe(bundle["file_inventory"], use_container_width=True, hide_index=True)

    st.subheader("Quick Data Quality Snapshot")
    quality_snapshot = pd.DataFrame(
        [
            {
                "dataset": "Consumption",
                "duplicate_rows": bundle["quality"]["consumption"]["duplicate_rows"],
                "parse_warning_count": int(
                    bundle["quality"]["consumption"]["parse_summary"]["count"].iloc[-1]
                    if not bundle["quality"]["consumption"]["parse_summary"].empty
                    else 0
                ),
                "missing_columns": ", ".join(bundle["quality"]["consumption"]["schema_report"]["missing_columns"]) or "None",
            },
            {
                "dataset": "Vend",
                "duplicate_rows": bundle["quality"]["vend"]["duplicate_rows"],
                "parse_warning_count": int(
                    bundle["quality"]["vend"]["parse_summary"]
                    .loc[
                        bundle["quality"]["vend"]["parse_summary"]["status"] != "parsed_datetime",
                        "count",
                    ]
                    .sum()
                    if not bundle["quality"]["vend"]["parse_summary"].empty
                    else 0
                ),
                "missing_columns": ", ".join(bundle["quality"]["vend"]["schema_report"]["missing_columns"]) or "None",
            },
        ]
    )
    st.dataframe(quality_snapshot, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()

