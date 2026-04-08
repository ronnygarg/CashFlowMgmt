"""Consumer explorer page."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import streamlit as st

from src.charts import bar_chart, line_chart
from src.dashboard_data import load_dashboard_bundle
from src.io_utils import dataframe_to_csv_bytes
from src.path_utils import extract_base_dir_arg, load_app_config

APP_CONFIG = load_app_config()
DISPLAY_CONFIG = APP_CONFIG.get("display", {})
MAX_TABLE_ROWS = APP_CONFIG.get("filters", {}).get("defaults", {}).get("max_table_rows", 1000)

st.set_page_config(
    page_title=f"{DISPLAY_CONFIG.get('app_title', 'CashFlowMgmt Dashboard')} - Consumer Explorer",
    layout=DISPLAY_CONFIG.get("layout", "wide"),
    initial_sidebar_state=DISPLAY_CONFIG.get("sidebar_state", "expanded"),
)


def _matching_activity_rows(df: pd.DataFrame, master_row: pd.Series) -> pd.DataFrame:
    """Return rows that resolve to or directly match the selected master record."""

    consumer_key = master_row.get("consumernumber_normalized")
    meter_key = master_row.get("meterno_normalized")
    master_id = master_row.get("consumer_master_id")

    masks = []
    if "resolved_master_id" in df.columns and pd.notna(master_id):
        masks.append(df["resolved_master_id"] == master_id)
    if "consumernumber_normalized" in df.columns and pd.notna(consumer_key):
        masks.append(df["consumernumber_normalized"] == consumer_key)
    if "meterno_normalized" in df.columns and pd.notna(meter_key):
        masks.append(df["meterno_normalized"] == meter_key)

    if not masks:
        return df.iloc[0:0].copy()

    combined = masks[0].copy()
    for mask in masks[1:]:
        combined = combined | mask
    return df[combined].copy()


def main() -> None:
    base_dir_arg = extract_base_dir_arg()
    bundle = load_dashboard_bundle(base_dir_override=base_dir_arg)

    consumer_summary = bundle["derived"]["datasets"]["consumer_summary"]
    vend_enriched = bundle["derived"]["datasets"]["vend_enriched"]
    consumption_enriched = bundle["derived"]["datasets"]["consumption_enriched"]

    st.title("Consumer Explorer")
    st.caption("Search for a consumer or meter to inspect the master profile, activity, joins, balances, and local data-quality gaps.")

    search_text = st.sidebar.text_input("Search consumer, consumer number, or meter", value="")
    filtered_consumers = consumer_summary.copy()
    if search_text.strip():
        needle = search_text.strip().lower()
        filtered_consumers = filtered_consumers[
            filtered_consumers.get("consumername", pd.Series(dtype="string")).astype("string").str.lower().str.contains(needle, na=False)
            | filtered_consumers.get("consumernumber", pd.Series(dtype="string")).astype("string").str.lower().str.contains(needle, na=False)
            | filtered_consumers.get("meterno", pd.Series(dtype="string")).astype("string").str.lower().str.contains(needle, na=False)
        ]

    if filtered_consumers.empty:
        st.warning("No consumers match the current search.")
        return

    filtered_consumers = filtered_consumers.sort_values(["consumername", "consumernumber"], na_position="last")
    option_labels = filtered_consumers.apply(
        lambda row: f"{row.get('consumername', 'Unknown')} | CN {row.get('consumernumber', 'NA')} | Meter {row.get('meterno', 'NA')}",
        axis=1,
    ).tolist()
    selected_label = st.selectbox("Select a consumer", options=option_labels)
    selected_row = filtered_consumers.iloc[option_labels.index(selected_label)]

    selected_vend = _matching_activity_rows(vend_enriched, selected_row)
    selected_consumption = _matching_activity_rows(consumption_enriched, selected_row)

    cards = st.columns(6)
    cards[0].metric("Meter balance", f"{selected_row.get('meterbalance', float('nan')):,.2f}" if pd.notna(selected_row.get("meterbalance")) else "Unavailable")
    cards[1].metric("Days since balance update", f"{int(selected_row.get('days_since_balance_update')):,}" if pd.notna(selected_row.get("days_since_balance_update")) else "Unavailable")
    cards[2].metric("Vend transactions", f"{int(selected_row.get('vend_transaction_count', 0)):,}")
    cards[3].metric("Vend total", f"{float(selected_row.get('vend_total_amount', 0) or 0):,.2f}")
    cards[4].metric("Avg daily kWh", f"{float(selected_row.get('average_daily_kwh', 0) or 0):,.2f}")
    cards[5].metric("Exception score", f"{int(selected_row.get('exception_score', 0)):,}")

    profile_col, activity_col = st.columns([1, 1.2])
    with profile_col:
        st.subheader("Master Profile")
        profile_rows = [
            {"field": "Consumer name", "value": selected_row.get("consumername")},
            {"field": "Consumer number", "value": selected_row.get("consumernumber")},
            {"field": "Meter number", "value": selected_row.get("meterno")},
            {"field": "Tariff code", "value": selected_row.get("tariffcode")},
            {"field": "Accounting mode", "value": selected_row.get("accountingmode")},
            {"field": "Connection status", "value": selected_row.get("connectionstatus")},
            {"field": "Connection type", "value": selected_row.get("connection_type")},
            {"field": "Area type", "value": selected_row.get("area_type")},
            {"field": "Feeder code", "value": selected_row.get("feedercode")},
            {"field": "DT code", "value": selected_row.get("dtcode")},
            {"field": "Substation", "value": selected_row.get("substationname")},
            {"field": "GIS latitude", "value": selected_row.get("gis_latitude")},
            {"field": "GIS longitude", "value": selected_row.get("gis_longitude")},
        ]
        st.dataframe(pd.DataFrame(profile_rows), use_container_width=True, hide_index=True)

        st.subheader("Missing or Weak Fields")
        missing_fields = [
            field["field"]
            for field in profile_rows
            if pd.isna(field["value"]) or (isinstance(field["value"], str) and field["value"].strip() == "")
        ]
        st.dataframe(
            pd.DataFrame({"missing_field": missing_fields or ["None in the displayed profile block"]}),
            use_container_width=True,
            hide_index=True,
        )

    with activity_col:
        st.subheader("Join and Activity Status")
        join_rows = [
            {"metric": "Vend rows linked to this consumer", "value": len(selected_vend)},
            {"metric": "Consumption rows linked to this consumer", "value": len(selected_consumption)},
            {"metric": "Vend present", "value": bool(selected_row.get("has_vend_records"))},
            {"metric": "Consumption present", "value": bool(selected_row.get("has_consumption_records"))},
            {"metric": "Low balance with consumption", "value": bool(selected_row.get("low_balance_with_consumption"))},
            {"metric": "Consumption without vend", "value": bool(selected_row.get("consumption_without_vend"))},
            {"metric": "Vend without consumption", "value": bool(selected_row.get("vend_without_consumption"))},
            {"metric": "Missing feeder or DT", "value": bool(selected_row.get("missing_network_attributes"))},
        ]
        st.dataframe(pd.DataFrame(join_rows), use_container_width=True, hide_index=True)

        if not selected_vend.empty and "join_coverage_status" in selected_vend.columns:
            join_status = (
                selected_vend.groupby("join_coverage_status", dropna=False)
                .size()
                .reset_index(name="row_count")
                .sort_values("row_count", ascending=False)
            )
            st.plotly_chart(
                bar_chart(join_status, x="join_coverage_status", y="row_count", title="Vend join status mix"),
                use_container_width=True,
            )

    st.subheader("Consumption Trend")
    if {"date", "kwh_consumption"}.issubset(selected_consumption.columns):
        consumer_daily = (
            selected_consumption.groupby("date", dropna=True)["kwh_consumption"]
            .sum(numeric_only=True)
            .reset_index()
            .sort_values("date")
        )
        st.plotly_chart(
            line_chart(consumer_daily, x="date", y="kwh_consumption", title="Daily kWh consumption for selected consumer"),
            use_container_width=True,
        )
    else:
        st.info("No consumption time series is available for the current selection.")

    vend_tab, consumption_tab = st.tabs(["Vend Detail", "Consumption Detail"])

    with vend_tab:
        st.download_button(
            "Download selected vend rows",
            data=dataframe_to_csv_bytes(selected_vend),
            file_name="consumer_explorer_vend.csv",
            mime="text/csv",
        )
        st.dataframe(selected_vend.head(MAX_TABLE_ROWS), use_container_width=True, hide_index=True)

    with consumption_tab:
        st.download_button(
            "Download selected consumption rows",
            data=dataframe_to_csv_bytes(selected_consumption),
            file_name="consumer_explorer_consumption.csv",
            mime="text/csv",
        )
        st.dataframe(selected_consumption.head(MAX_TABLE_ROWS), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
