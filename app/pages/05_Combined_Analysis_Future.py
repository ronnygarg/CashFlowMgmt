"""Future combined analysis placeholder page."""

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
from src.path_utils import extract_base_dir_arg, load_app_config

APP_CONFIG = load_app_config()
DISPLAY_CONFIG = APP_CONFIG.get("display", {})

st.set_page_config(
    page_title=f"{DISPLAY_CONFIG.get('app_title', 'CashFlowMgmt Dashboard')} - Combined Analysis Future",
    layout=DISPLAY_CONFIG.get("layout", "wide"),
    initial_sidebar_state=DISPLAY_CONFIG.get("sidebar_state", "expanded"),
)


def main() -> None:
    base_dir_arg = extract_base_dir_arg()
    bundle = load_dashboard_bundle(base_dir_override=base_dir_arg)
    schema_config = bundle["schema_config"]

    st.title("Combined Analysis, Future")
    st.caption("Architecture placeholder for future linked analysis once a validated bridge or common key becomes available.")
    st.error(LIMITATION_NO_LINKING_KEY)
    st.info("This page intentionally avoids any fake merge between consumption and vend data.")

    current_keys = st.columns(2)
    with current_keys[0]:
        st.subheader("Current Consumption Keys")
        st.dataframe(
            pd.DataFrame(
                {
                    "field": schema_config["datasets"]["consumption"].get("candidate_keys", []),
                    "dataset": "consumption",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    with current_keys[1]:
        st.subheader("Current Vend Keys")
        st.dataframe(
            pd.DataFrame(
                {
                    "field": schema_config["datasets"]["vend"].get("candidate_keys", []),
                    "dataset": "vend",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Blocked Reason")
    st.write(schema_config.get("future_combined_analysis", {}).get("blocked_reason", LIMITATION_NO_LINKING_KEY))

    st.subheader("Future Architecture Hooks")
    st.code(
        "\n".join(
            [
                "raw bridge tables -> standardised entity resolution layer",
                "consumption fact table -> validated meter or account bridge",
                "vend fact table -> validated meter or service-point bridge",
                "linked analytical mart -> cashflow vs consumption analysis",
                "risk models -> recharge exhaustion and self-disconnection analysis",
            ]
        ),
        language="text",
    )

    st.subheader("Disabled Prototype Controls")
    st.selectbox("Bridge table", options=["Not available yet"], disabled=True)
    st.selectbox("Join strategy", options=["Blocked until validated mapping"], disabled=True)
    st.slider("Recharge exhaustion lookback days", min_value=1, max_value=60, value=14, disabled=True)

    st.subheader("TODOs")
    for item in schema_config.get("future_combined_analysis", {}).get("desired_future_artifacts", []):
        st.markdown(f"- {item}")

    st.subheader("Current Dataset Field Snapshot")
    field_snapshot = pd.DataFrame(
        [
            {
                "dataset": "consumption",
                "available_columns": ", ".join(bundle["datasets"]["consumption"].columns[:20].tolist()),
            },
            {
                "dataset": "vend",
                "available_columns": ", ".join(bundle["datasets"]["vend"].columns[:20].tolist()),
            },
        ]
    )
    st.dataframe(field_snapshot, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
