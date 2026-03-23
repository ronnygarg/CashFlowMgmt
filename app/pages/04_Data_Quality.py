"""Prominent data quality diagnostics page."""

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
QUALITY_EXPORT_SCHEMA_VERSION = "1.0.0"

st.set_page_config(
    page_title=f"{DISPLAY_CONFIG.get('app_title', 'CashFlowMgmt Dashboard')} - Data Quality",
    layout=DISPLAY_CONFIG.get("layout", "wide"),
    initial_sidebar_state=DISPLAY_CONFIG.get("sidebar_state", "expanded"),
)


def _schema_status_row(dataset_name: str, quality: dict[str, object]) -> dict[str, object]:
    schema_report = quality["schema_report"]
    parse_summary = quality["parse_summary"]
    parse_warning_count = 0
    if isinstance(parse_summary, pd.DataFrame) and not parse_summary.empty:
        if dataset_name == "Consumption":
            parse_warning_count = int(parse_summary.loc[parse_summary["status"] != "parsed_datetime", "count"].sum())
        else:
            parse_warning_count = int(parse_summary.loc[parse_summary["status"] != "parsed_datetime", "count"].sum())

    return {
        "dataset": dataset_name,
        "schema_valid": schema_report["is_valid"],
        "missing_columns": ", ".join(schema_report["missing_columns"]) or "None",
        "duplicate_rows": quality["duplicate_rows"],
        "parse_warning_count": parse_warning_count,
    }


def _render_duplicate_diagnostics(quality: dict[str, object], dataset_label: str) -> None:
    duplicate_details = quality.get("duplicate_diagnostics", {})
    if not isinstance(duplicate_details, dict):
        st.info(f"No duplicate diagnostics available for {dataset_label}.")
        return

    by_source = duplicate_details.get("by_source_file", pd.DataFrame())
    by_date = duplicate_details.get("by_date", pd.DataFrame())

    st.caption(f"Duplicate diagnostics for {dataset_label}. Rows are not dropped by default policy.")

    if isinstance(by_source, pd.DataFrame) and not by_source.empty:
        st.markdown("**Duplicate rows by source file**")
        st.bar_chart(by_source.set_index("label")["duplicate_rows"])
        st.dataframe(by_source, use_container_width=True, hide_index=True)
    else:
        st.info("No duplicated rows detected by source file.")

    if isinstance(by_date, pd.DataFrame) and not by_date.empty:
        st.markdown("**Duplicate rows by date**")
        st.line_chart(by_date.set_index("label")["duplicate_rows"])
        st.dataframe(by_date, use_container_width=True, hide_index=True)
    else:
        st.info("No duplicated rows detected by date.")


def _to_serializable(value: Any) -> Any:
    """Recursively convert pandas-heavy objects into JSON-serializable payloads."""

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
    """Build a schema-versioned payload for quality diagnostics export."""

    generated_at_utc = datetime.now(timezone.utc)
    return {
        "schema_version": QUALITY_EXPORT_SCHEMA_VERSION,
        "generated_at_utc": generated_at_utc.isoformat(),
        "project": {
            "name": APP_CONFIG.get("project", {}).get("name", "CashFlowMgmt Dashboard"),
            "base_dir": str(bundle.get("paths", {}).base_dir) if bundle.get("paths") else None,
            "base_dir_source": getattr(bundle.get("paths"), "base_dir_source", None),
        },
        "quality": {
            "consumption": _to_serializable(bundle["quality"]["consumption"]),
            "vend": _to_serializable(bundle["quality"]["vend"]),
        },
    }


def main() -> None:
    base_dir_arg = extract_base_dir_arg()
    bundle = load_dashboard_bundle(base_dir_override=base_dir_arg)
    consumption_quality = bundle["quality"]["consumption"]
    vend_quality = bundle["quality"]["vend"]

    st.title("Data Quality")
    st.caption("Critical diagnostics for schema conformance, missingness, parse coverage, suspicious values, outliers, and file-level ingest health.")
    st.warning(LIMITATION_NO_LINKING_KEY)
    st.warning(LIMITATION_VEND_DATETIME)

    overview_cards = st.columns(4)
    overview_cards[0].metric("Consumption duplicate rows", f"{consumption_quality['duplicate_rows']:,}")
    overview_cards[1].metric("Vend duplicate rows", f"{vend_quality['duplicate_rows']:,}")
    overview_cards[2].metric(
        "Consumption parse warnings",
        f"{int(consumption_quality['parse_summary'].loc[consumption_quality['parse_summary']['status'] != 'parsed_datetime', 'count'].sum()):,}",
    )
    overview_cards[3].metric(
        "Vend parse warnings",
        f"{int(vend_quality['parse_summary'].loc[vend_quality['parse_summary']['status'] != 'parsed_datetime', 'count'].sum()):,}",
    )

    for warning in vend_quality["warnings"]:
        st.info(warning)

    tabs = st.tabs(["Overview", "Consumption Diagnostics", "Vend Diagnostics", "File Diagnostics"])

    with tabs[0]:
        st.subheader("Quality Overview")
        overview_table = pd.DataFrame(
            [
                _schema_status_row("Consumption", consumption_quality),
                _schema_status_row("Vend", vend_quality),
            ]
        )
        st.dataframe(overview_table, use_container_width=True, hide_index=True)

        quality_export_payload = _build_quality_export_payload(bundle)
        export_file_name = build_utc_timestamped_filename(
            prefix="quality_diagnostics",
            generated_at_utc_iso=str(quality_export_payload["generated_at_utc"]),
        )
        st.download_button(
            "Download quality diagnostics JSON",
            data=object_to_json_bytes(quality_export_payload),
            file_name=export_file_name,
            mime="application/json",
        )

    with tabs[1]:
        st.subheader("Consumption Schema")
        st.json(consumption_quality["schema_report"])
        st.subheader("Duplicate Diagnostics")
        _render_duplicate_diagnostics(consumption_quality, "Consumption")
        st.subheader("Missing Values")
        st.dataframe(consumption_quality["missing_summary"], use_container_width=True, hide_index=True)
        st.subheader("Parse Summary")
        st.dataframe(consumption_quality["parse_summary"], use_container_width=True, hide_index=True)
        st.subheader("Suspicious Numeric Flags")
        st.dataframe(consumption_quality["numeric_flags"], use_container_width=True, hide_index=True)
        st.subheader("Outlier Indicators")
        st.dataframe(consumption_quality["outlier_summary"], use_container_width=True, hide_index=True)
        st.subheader("Column Profiling")
        st.dataframe(consumption_quality["column_profile"], use_container_width=True, hide_index=True)

    with tabs[2]:
        st.subheader("Vend Schema")
        st.json(vend_quality["schema_report"])
        st.subheader("Duplicate Diagnostics")
        _render_duplicate_diagnostics(vend_quality, "Vend")
        st.subheader("Missing Values")
        st.dataframe(vend_quality["missing_summary"], use_container_width=True, hide_index=True)
        st.subheader("Issuedate Parse Summary")
        st.dataframe(vend_quality["parse_summary"], use_container_width=True, hide_index=True)
        st.subheader("Suspicious Numeric Flags")
        st.dataframe(vend_quality["numeric_flags"], use_container_width=True, hide_index=True)
        st.subheader("Outlier Indicators")
        st.dataframe(vend_quality["outlier_summary"], use_container_width=True, hide_index=True)
        st.subheader("Column Profiling")
        st.dataframe(vend_quality["column_profile"], use_container_width=True, hide_index=True)

    with tabs[3]:
        st.subheader("File-Level Diagnostics")
        st.dataframe(bundle["file_inventory"], use_container_width=True, hide_index=True)
        errors_only = bundle["file_inventory"][bundle["file_inventory"]["read_status"] != "ok"].copy()
        if not errors_only.empty:
            st.error("Some files failed to read.")
            st.dataframe(errors_only, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()

