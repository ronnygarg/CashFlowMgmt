"""Prominent data quality and reconciliation page."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import streamlit as st

from src.constants import LIMITATION_NO_LINKING_KEY, LIMITATION_VEND_DATETIME
from src.dashboard_data import load_dashboard_bundle
from src.io_utils import build_utc_timestamped_filename, object_to_json_bytes
from src.path_utils import extract_base_dir_arg, load_app_config

APP_CONFIG = load_app_config()
DISPLAY_CONFIG = APP_CONFIG.get("display", {})
QUALITY_EXPORT_SCHEMA_VERSION = "2.0.0"
MAX_TABLE_ROWS = APP_CONFIG.get("filters", {}).get("defaults", {}).get("max_table_rows", 1000)

st.set_page_config(
    page_title=f"{DISPLAY_CONFIG.get('app_title', 'CashFlowMgmt Dashboard')} - Data Quality Reconciliation",
    layout=DISPLAY_CONFIG.get("layout", "wide"),
    initial_sidebar_state=DISPLAY_CONFIG.get("sidebar_state", "expanded"),
)


def _to_serializable(value: Any) -> Any:
    if isinstance(value, pd.DataFrame):
        return value.to_dict(orient="records")
    if isinstance(value, pd.Series):
        return value.to_list()
    if isinstance(value, dict):
        return {str(key): _to_serializable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_serializable(item) for item in value]
    if pd.isna(value):
        return None
    return value


def _build_quality_export_payload(bundle: dict[str, Any]) -> dict[str, Any]:
    generated_at_utc = datetime.now(timezone.utc)
    return {
        "schema_version": QUALITY_EXPORT_SCHEMA_VERSION,
        "generated_at_utc": generated_at_utc.isoformat(),
        "quality": _to_serializable(bundle["quality"]),
        "join_coverage": _to_serializable(bundle["derived"]["join_coverage"]),
        "vend_timestamp_quality": _to_serializable(bundle["derived"]["vend_timestamp_quality"]),
    }


def _render_dataset_quality(label: str, quality: dict[str, Any]) -> None:
    st.subheader(f"{label} schema")
    st.json(quality["schema_report"])
    st.subheader(f"{label} key diagnostics")
    st.dataframe(quality["key_diagnostics"], use_container_width=True, hide_index=True)
    st.subheader(f"{label} missing values")
    st.dataframe(quality["missing_summary"].head(MAX_TABLE_ROWS), use_container_width=True, hide_index=True)
    st.subheader(f"{label} parse summary")
    st.dataframe(quality["parse_summary"], use_container_width=True, hide_index=True)
    st.subheader(f"{label} numeric flags")
    st.dataframe(quality["numeric_flags"], use_container_width=True, hide_index=True)
    st.subheader(f"{label} outlier indicators")
    st.dataframe(quality["outlier_summary"], use_container_width=True, hide_index=True)
    st.subheader(f"{label} column profile")
    st.dataframe(quality["column_profile"].head(MAX_TABLE_ROWS), use_container_width=True, hide_index=True)


def main() -> None:
    base_dir_arg = extract_base_dir_arg()
    bundle = load_dashboard_bundle(base_dir_override=base_dir_arg)

    master_quality = bundle["quality"]["consumer_master"]
    vend_quality = bundle["quality"]["vend"]
    consumption_quality = bundle["quality"]["consumption"]

    vend_join = bundle["derived"]["join_coverage"]["vend"]
    consumption_join = bundle["derived"]["join_coverage"]["consumption"]
    vend_enriched = bundle["derived"]["datasets"]["vend_enriched"]
    consumption_enriched = bundle["derived"]["datasets"]["consumption_enriched"]

    st.title("Data Quality & Reconciliation")
    st.caption(
        "Prominent diagnostics for schema conformance, blank rates, duplicate business keys, timestamp quality, "
        "and conservative join coverage against Consumer Master."
    )
    st.warning(LIMITATION_NO_LINKING_KEY)
    st.warning(LIMITATION_VEND_DATETIME)

    for warning in master_quality["warnings"] + vend_quality["warnings"] + consumption_quality["warnings"]:
        st.info(warning)

    overview_cards = st.columns(6)
    overview_cards[0].metric("Master duplicate rows", f"{master_quality['duplicate_rows']:,}")
    overview_cards[1].metric("Vend duplicate rows", f"{vend_quality['duplicate_rows']:,}")
    overview_cards[2].metric("Consumption duplicate rows", f"{consumption_quality['duplicate_rows']:,}")
    overview_cards[3].metric(
        "Vend unmatched rows",
        f"{int(vend_join['coverage'].loc[vend_join['coverage']['join_coverage_status'] == 'unmatched', 'row_count'].sum()):,}",
    )
    overview_cards[4].metric(
        "Consumption unmatched rows",
        f"{int(consumption_join['coverage'].loc[consumption_join['coverage']['join_coverage_status'] == 'unmatched', 'row_count'].sum()):,}",
    )
    overview_cards[5].metric(
        "Vend time-only rows",
        f"{int(vend_quality['parse_summary'].loc[vend_quality['parse_summary']['status'] == 'time_only', 'count'].sum()):,}",
    )

    quality_export_payload = _build_quality_export_payload(bundle)
    export_file_name = build_utc_timestamped_filename(
        prefix="quality_diagnostics",
        generated_at_utc_iso=str(quality_export_payload["generated_at_utc"]),
    )
    st.download_button(
        "Download quality and reconciliation JSON",
        data=object_to_json_bytes(quality_export_payload),
        file_name=export_file_name,
        mime="application/json",
    )

    tabs = st.tabs(
        [
            "Overview",
            "Consumer Master",
            "Vend",
            "Consumption",
            "Reconciliation",
            "File Diagnostics",
        ]
    )

    with tabs[0]:
        source_counts = pd.DataFrame(
            [
                {"dataset": "Consumer Master", "rows": len(bundle["datasets"]["consumer_master"])},
                {"dataset": "Vend", "rows": len(bundle["datasets"]["vend"])},
                {"dataset": "Consumption", "rows": len(bundle["datasets"]["consumption"])},
            ]
        )
        st.subheader("Row counts by source")
        st.dataframe(source_counts, use_container_width=True, hide_index=True)
        st.subheader("Vend join coverage")
        st.dataframe(vend_join["coverage"], use_container_width=True, hide_index=True)
        st.subheader("Consumption join coverage")
        st.dataframe(consumption_join["coverage"], use_container_width=True, hide_index=True)

    with tabs[1]:
        _render_dataset_quality("Consumer Master", master_quality)

    with tabs[2]:
        _render_dataset_quality("Vend", vend_quality)

    with tabs[3]:
        _render_dataset_quality("Consumption", consumption_quality)

    with tabs[4]:
        st.subheader("Vend resolution status")
        st.dataframe(vend_join["resolution"], use_container_width=True, hide_index=True)
        st.subheader("Consumption resolution status")
        st.dataframe(consumption_join["resolution"], use_container_width=True, hide_index=True)

        st.subheader("Unmatched vend rows")
        st.dataframe(
            vend_enriched[vend_enriched.get("join_coverage_status", pd.Series(dtype="string")) == "unmatched"].head(MAX_TABLE_ROWS),
            use_container_width=True,
            hide_index=True,
        )
        st.subheader("Unmatched consumption rows")
        st.dataframe(
            consumption_enriched[consumption_enriched.get("join_coverage_status", pd.Series(dtype="string")) == "unmatched"].head(MAX_TABLE_ROWS),
            use_container_width=True,
            hide_index=True,
        )

    with tabs[5]:
        st.subheader("File-level diagnostics")
        st.dataframe(bundle["file_inventory"], use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
