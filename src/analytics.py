"""Analytical tables, reconciliation, and operational exception helpers."""

from __future__ import annotations

from typing import Any, Mapping

import numpy as np
import pandas as pd

from src.constants import (
    DATE_STATUS_DATE_ONLY,
    DATE_STATUS_FAILED,
    DATE_STATUS_MISSING,
    DATE_STATUS_PARSED,
    DATE_STATUS_TIME_ONLY,
    JOIN_STATUS_BOTH_KEYS,
    JOIN_STATUS_CONSUMER_ONLY,
    JOIN_STATUS_METER_ONLY,
    JOIN_STATUS_UNMATCHED,
    RESOLUTION_STATUS_CONFLICTING_KEYS,
    RESOLUTION_STATUS_DUPLICATE_MASTER_KEY,
    RESOLUTION_STATUS_RESOLVED_BOTH,
    RESOLUTION_STATUS_RESOLVED_CONSUMER,
    RESOLUTION_STATUS_RESOLVED_METER,
    RESOLUTION_STATUS_UNRESOLVED,
)

MASTER_ENRICHMENT_COLUMNS = [
    "consumer_master_id",
    "consumername",
    "tariffcode",
    "accountingmode",
    "connectionstatus",
    "connection_type",
    "area_type",
    "feedercode",
    "dtcode",
    "meterbalance",
    "balanceupdatedon_parsed",
    "has_valid_gis",
]


def _safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _match_status_from_flags(consumer_match: bool, meter_match: bool) -> str:
    if consumer_match and meter_match:
        return JOIN_STATUS_BOTH_KEYS
    if consumer_match:
        return JOIN_STATUS_CONSUMER_ONLY
    if meter_match:
        return JOIN_STATUS_METER_ONLY
    return JOIN_STATUS_UNMATCHED


def _series_from_map(df: pd.DataFrame, column: str, mapping: dict[str, Any]) -> pd.Series:
    if column not in df.columns:
        return pd.Series(pd.NA, index=df.index, dtype="object")
    return df[column].map(mapping)


def _build_unique_key_lookup(master_df: pd.DataFrame, key_column: str) -> tuple[dict[str, Any], set[str], set[str]]:
    """Return unique lookup, full membership set, and duplicate-key set for one master key."""

    if key_column not in master_df.columns:
        return {}, set(), set()

    candidate_rows = master_df[["consumer_master_id", key_column]].dropna().copy()
    if candidate_rows.empty:
        return {}, set(), set()

    grouped = (
        candidate_rows.groupby(key_column, dropna=True)["consumer_master_id"]
        .agg(list)
        .reset_index()
    )
    grouped["match_count"] = grouped["consumer_master_id"].map(len)
    unique_lookup = {
        str(row[key_column]): row["consumer_master_id"][0]
        for _, row in grouped[grouped["match_count"] == 1].iterrows()
    }
    all_keys = {str(value) for value in grouped[key_column].tolist()}
    duplicate_keys = {str(row[key_column]) for _, row in grouped[grouped["match_count"] > 1].iterrows()}
    return unique_lookup, all_keys, duplicate_keys


def summarize_vend_timestamp_quality(vend_df: pd.DataFrame) -> dict[str, Any]:
    """Summarize issuedate quality and what types of analysis are safe."""

    status_counts = (
        vend_df.get("issuedate_parse_status", pd.Series(dtype="string"))
        .fillna(DATE_STATUS_MISSING)
        .value_counts(dropna=False)
        .to_dict()
    )
    total_rows = int(len(vend_df))
    full_datetime = int(status_counts.get(DATE_STATUS_PARSED, 0))
    date_only = int(status_counts.get(DATE_STATUS_DATE_ONLY, 0))
    time_only = int(status_counts.get(DATE_STATUS_TIME_ONLY, 0))
    missing = int(status_counts.get(DATE_STATUS_MISSING, 0))
    failed = int(status_counts.get(DATE_STATUS_FAILED, 0))
    calendar_rows = full_datetime + date_only
    calendar_pct = round((calendar_rows / total_rows * 100), 2) if total_rows else 0.0

    if total_rows == 0:
        quality_label = "empty"
    elif full_datetime == total_rows:
        quality_label = "full_datetime"
    elif date_only == total_rows:
        quality_label = "date_only"
    elif time_only == total_rows:
        quality_label = "time_only"
    elif calendar_rows > 0 and time_only == 0 and failed == 0:
        quality_label = "calendar_mixed"
    else:
        quality_label = "mixed_or_low_quality"

    supports_daily = calendar_rows > 0 and time_only == 0 and failed == 0
    supports_intraday = full_datetime > 0 or time_only > 0

    note_parts: list[str] = []
    if quality_label == "full_datetime":
        note_parts.append("Issuedate quality supports daily and intraday analysis.")
    elif quality_label == "date_only":
        note_parts.append("Issuedate values are date-only, so daily analysis is safe but intraday timing is not.")
    elif quality_label == "time_only":
        note_parts.append("Issuedate values are time-only, so intraday distribution is available but calendar trends are not safe.")
    else:
        note_parts.append("Issuedate quality is mixed, so calendar charts should be treated cautiously and time-of-day analysis may be more reliable.")

    if missing or failed:
        note_parts.append(f"{missing + failed:,} rows remain missing or failed parsing.")

    return {
        "total_rows": total_rows,
        "full_datetime_rows": full_datetime,
        "date_only_rows": date_only,
        "time_only_rows": time_only,
        "missing_rows": missing,
        "failed_rows": failed,
        "calendar_coverage_pct": calendar_pct,
        "quality_label": quality_label,
        "supports_daily_analysis": supports_daily,
        "supports_intraday_analysis": supports_intraday,
        "note": " ".join(note_parts).strip(),
    }


def enrich_dataset_with_master(
    dataset_df: pd.DataFrame,
    master_df: pd.DataFrame,
) -> pd.DataFrame:
    """Classify join coverage and attach a conservative resolved master id when possible."""

    enriched = dataset_df.copy()
    if enriched.empty:
        for column in [
            "join_coverage_status",
            "resolution_status",
            "matched_on_consumernumber",
            "matched_on_meterno",
            "resolved_master_id",
        ]:
            enriched[column] = pd.Series(dtype="string")
        return enriched

    consumer_lookup, consumer_key_set, duplicate_consumer_keys = _build_unique_key_lookup(
        master_df,
        "consumernumber_normalized",
    )
    meter_lookup, meter_key_set, duplicate_meter_keys = _build_unique_key_lookup(
        master_df,
        "meterno_normalized",
    )

    consumer_key = enriched.get("consumernumber_normalized", pd.Series(pd.NA, index=enriched.index, dtype="string")).astype("string")
    meter_key = enriched.get("meterno_normalized", pd.Series(pd.NA, index=enriched.index, dtype="string")).astype("string")

    consumer_match = consumer_key.isin(list(consumer_key_set))
    meter_match = meter_key.isin(list(meter_key_set))
    enriched["matched_on_consumernumber"] = consumer_match
    enriched["matched_on_meterno"] = meter_match
    enriched["join_coverage_status"] = [
        _match_status_from_flags(bool(cons), bool(meter))
        for cons, meter in zip(consumer_match.tolist(), meter_match.tolist())
    ]

    consumer_resolved_id = _series_from_map(enriched, "consumernumber_normalized", consumer_lookup)
    meter_resolved_id = _series_from_map(enriched, "meterno_normalized", meter_lookup)
    consumer_duplicate = consumer_key.isin(list(duplicate_consumer_keys))
    meter_duplicate = meter_key.isin(list(duplicate_meter_keys))

    resolution_status = pd.Series(RESOLUTION_STATUS_UNRESOLVED, index=enriched.index, dtype="string")
    resolved_master_id = pd.Series(pd.NA, index=enriched.index, dtype="Int64")

    both_unique_same = consumer_resolved_id.notna() & meter_resolved_id.notna() & (consumer_resolved_id == meter_resolved_id)
    resolution_status.loc[both_unique_same] = RESOLUTION_STATUS_RESOLVED_BOTH
    resolved_master_id.loc[both_unique_same] = consumer_resolved_id.loc[both_unique_same].astype("Int64")

    consumer_only = consumer_resolved_id.notna() & (meter_resolved_id.isna() | ~meter_match)
    resolution_status.loc[consumer_only] = RESOLUTION_STATUS_RESOLVED_CONSUMER
    resolved_master_id.loc[consumer_only] = consumer_resolved_id.loc[consumer_only].astype("Int64")

    meter_only = meter_resolved_id.notna() & (consumer_resolved_id.isna() | ~consumer_match)
    resolution_status.loc[meter_only] = RESOLUTION_STATUS_RESOLVED_METER
    resolved_master_id.loc[meter_only] = meter_resolved_id.loc[meter_only].astype("Int64")

    conflicting = consumer_resolved_id.notna() & meter_resolved_id.notna() & (consumer_resolved_id != meter_resolved_id)
    resolution_status.loc[conflicting] = RESOLUTION_STATUS_CONFLICTING_KEYS

    duplicate_key_match = (consumer_duplicate & consumer_match) | (meter_duplicate & meter_match)
    resolution_status.loc[duplicate_key_match & ~conflicting & ~both_unique_same] = RESOLUTION_STATUS_DUPLICATE_MASTER_KEY

    enriched["resolution_status"] = resolution_status
    enriched["resolved_master_id"] = resolved_master_id

    enrichment_columns = [column for column in MASTER_ENRICHMENT_COLUMNS if column in master_df.columns]
    master_enrichment = master_df[enrichment_columns].copy()
    master_enrichment = master_enrichment.rename(
        columns={column: f"master_{column}" for column in enrichment_columns if column != "consumer_master_id"}
    )
    enriched = enriched.merge(
        master_enrichment,
        how="left",
        left_on="resolved_master_id",
        right_on="consumer_master_id",
    )
    if "consumer_master_id_y" in enriched.columns:
        enriched = enriched.drop(columns=["consumer_master_id_y"])
    if "consumer_master_id_x" in enriched.columns:
        enriched = enriched.rename(columns={"consumer_master_id_x": "consumer_master_id"})
    return enriched


def _mode_or_na(series: pd.Series) -> Any:
    cleaned = series.dropna()
    if cleaned.empty:
        return pd.NA
    modes = cleaned.mode(dropna=True)
    return modes.iloc[0] if not modes.empty else cleaned.iloc[0]


def _latest_per_group(
    df: pd.DataFrame,
    group_column: str,
    sort_column: str,
    value_columns: list[str],
) -> pd.DataFrame:
    if df.empty or group_column not in df.columns or sort_column not in df.columns:
        return pd.DataFrame(columns=[group_column] + value_columns)

    latest = (
        df.dropna(subset=[group_column, sort_column])
        .sort_values([group_column, sort_column])
        .groupby(group_column, dropna=True)
        .tail(1)
    )
    available_columns = [group_column] + [column for column in value_columns if column in latest.columns]
    return latest[available_columns].copy()


def _risk_thresholds(app_config: Mapping[str, Any]) -> Mapping[str, Any]:
    thresholds = app_config.get("data", {}).get("risk_thresholds", {})
    return thresholds if isinstance(thresholds, Mapping) else {}


def build_join_coverage_summary(dataset_df: pd.DataFrame, dataset_name: str) -> dict[str, pd.DataFrame]:
    """Build join coverage summary tables for reconciliation outputs."""

    total_rows = len(dataset_df)
    coverage = (
        dataset_df.get("join_coverage_status", pd.Series(dtype="string"))
        .fillna(JOIN_STATUS_UNMATCHED)
        .value_counts(dropna=False)
        .rename_axis("join_coverage_status")
        .reset_index(name="row_count")
    )
    if not coverage.empty:
        coverage["row_pct"] = (coverage["row_count"] / total_rows * 100).round(2)
        coverage["dataset"] = dataset_name

    resolution = (
        dataset_df.get("resolution_status", pd.Series(dtype="string"))
        .fillna(RESOLUTION_STATUS_UNRESOLVED)
        .value_counts(dropna=False)
        .rename_axis("resolution_status")
        .reset_index(name="row_count")
    )
    if not resolution.empty:
        resolution["row_pct"] = (resolution["row_count"] / total_rows * 100).round(2)
        resolution["dataset"] = dataset_name

    return {
        "coverage": coverage,
        "resolution": resolution,
    }


def build_consumer_summary(
    master_df: pd.DataFrame,
    vend_enriched: pd.DataFrame,
    consumption_enriched: pd.DataFrame,
    app_config: Mapping[str, Any],
) -> pd.DataFrame:
    """Build a master-anchored customer summary table."""

    summary = master_df.copy()

    vend_resolved = vend_enriched[vend_enriched["resolved_master_id"].notna()].copy()
    if not vend_resolved.empty:
        vend_resolved["resolved_master_id"] = vend_resolved["resolved_master_id"].astype("Int64")
    vend_resolved["transactionamount_numeric"] = _safe_numeric(vend_resolved.get("transactionamount", pd.Series(dtype="float64")))

    vend_agg = (
        vend_resolved.groupby("resolved_master_id", dropna=True)
        .agg(
            vend_transaction_count=("transactionamount_numeric", "size"),
            vend_total_amount=("transactionamount_numeric", "sum"),
            vend_average_amount=("transactionamount_numeric", "mean"),
            vend_median_amount=("transactionamount_numeric", "median"),
            vend_last_date=("issuedate_parsed", "max"),
            vend_first_date=("issuedate_parsed", "min"),
        )
        .reset_index()
        if not vend_resolved.empty
        else pd.DataFrame(
            columns=[
                "resolved_master_id",
                "vend_transaction_count",
                "vend_total_amount",
                "vend_average_amount",
                "vend_median_amount",
                "vend_last_date",
                "vend_first_date",
            ]
        )
    )

    consumption_resolved = consumption_enriched[consumption_enriched["resolved_master_id"].notna()].copy()
    if not consumption_resolved.empty:
        consumption_resolved["resolved_master_id"] = consumption_resolved["resolved_master_id"].astype("Int64")
    consumption_resolved["kwh_consumption_numeric"] = _safe_numeric(consumption_resolved.get("kwh_consumption", pd.Series(dtype="float64")))
    consumption_resolved["kvah_consumption_numeric"] = _safe_numeric(consumption_resolved.get("kvah_consumption", pd.Series(dtype="float64")))

    consumption_agg = (
        consumption_resolved.groupby("resolved_master_id", dropna=True)
        .agg(
            consumption_row_count=("kwh_consumption_numeric", "size"),
            total_kwh_consumption=("kwh_consumption_numeric", "sum"),
            total_kvah_consumption=("kvah_consumption_numeric", "sum"),
            average_daily_kwh=("kwh_consumption_numeric", "mean"),
            average_daily_kvah=("kvah_consumption_numeric", "mean"),
            latest_consumption_date=("midnightdate_parsed", "max"),
            zero_kwh_days=("kwh_consumption_numeric", lambda series: int((series == 0).sum())),
            negative_kwh_days=("kwh_consumption_numeric", lambda series: int((series < 0).sum())),
            suspicious_kwh_days=("kwh_consumption_numeric", lambda series: int(((series <= 0) | series.isna()).sum())),
            substationname=("substationname", _mode_or_na),
            supplyvoltage=("supplyvoltage", _mode_or_na),
        )
        .reset_index()
        if not consumption_resolved.empty
        else pd.DataFrame(
            columns=[
                "resolved_master_id",
                "consumption_row_count",
                "total_kwh_consumption",
                "total_kvah_consumption",
                "average_daily_kwh",
                "average_daily_kvah",
                "latest_consumption_date",
                "zero_kwh_days",
                "negative_kwh_days",
                "suspicious_kwh_days",
                "substationname",
                "supplyvoltage",
            ]
        )
    )

    latest_consumption = _latest_per_group(
        consumption_resolved,
        group_column="resolved_master_id",
        sort_column="midnightdate_parsed",
        value_columns=["midnightdate_parsed", "kwh_consumption_numeric", "kvah_consumption_numeric"],
    ).rename(
        columns={
            "midnightdate_parsed": "latest_daily_consumption_date",
            "kwh_consumption_numeric": "latest_daily_kwh",
            "kvah_consumption_numeric": "latest_daily_kvah",
        }
    )
    if latest_consumption.empty:
        latest_consumption = pd.DataFrame(
            columns=[
                "resolved_master_id",
                "latest_daily_consumption_date",
                "latest_daily_kwh",
                "latest_daily_kvah",
            ]
        )

    summary = summary.merge(vend_agg, how="left", left_on="consumer_master_id", right_on="resolved_master_id")
    summary = summary.drop(columns=["resolved_master_id"], errors="ignore")
    summary = summary.merge(consumption_agg, how="left", left_on="consumer_master_id", right_on="resolved_master_id")
    summary = summary.drop(columns=["resolved_master_id"], errors="ignore")
    summary = summary.merge(latest_consumption, how="left", left_on="consumer_master_id", right_on="resolved_master_id")
    summary = summary.drop(columns=["resolved_master_id"], errors="ignore")

    summary["vend_transaction_count"] = summary["vend_transaction_count"].fillna(0).astype("Int64")
    summary["consumption_row_count"] = summary["consumption_row_count"].fillna(0).astype("Int64")
    summary["zero_kwh_days"] = summary["zero_kwh_days"].fillna(0).astype("Int64")
    summary["negative_kwh_days"] = summary["negative_kwh_days"].fillna(0).astype("Int64")
    summary["suspicious_kwh_days"] = summary["suspicious_kwh_days"].fillna(0).astype("Int64")

    summary["has_vend_records"] = summary["vend_transaction_count"] > 0
    summary["has_consumption_records"] = summary["consumption_row_count"] > 0

    thresholds = _risk_thresholds(app_config)
    low_balance_threshold = float(thresholds.get("low_balance", 100))
    critical_balance_threshold = float(thresholds.get("critical_balance", 50))
    watch_balance_threshold = float(thresholds.get("watch_balance", 250))
    stale_balance_days = int(thresholds.get("stale_balance_warning_days", 30))
    high_avg_daily_kwh = float(thresholds.get("high_average_daily_kwh", 20))

    now = pd.Timestamp.now().normalize()
    balance_updated = pd.to_datetime(summary.get("balanceupdatedon_parsed"), errors="coerce")
    summary["days_since_balance_update"] = (now - balance_updated.dt.floor("D")).dt.days.astype("Float64")
    summary["balance_update_missing"] = balance_updated.isna()
    summary["stale_balance_update"] = summary["days_since_balance_update"].gt(stale_balance_days).fillna(False)

    meterbalance = summary.get("meterbalance", pd.Series(np.nan, index=summary.index, dtype="float64"))
    feedercode = summary.get("feedercode", pd.Series(pd.NA, index=summary.index, dtype="string"))
    dtcode = summary.get("dtcode", pd.Series(pd.NA, index=summary.index, dtype="string"))
    has_valid_gis = summary.get("has_valid_gis", pd.Series(False, index=summary.index, dtype="bool"))

    summary["low_balance_flag"] = meterbalance.le(low_balance_threshold).fillna(False)
    summary["critical_balance_flag"] = meterbalance.le(critical_balance_threshold).fillna(False)
    summary["watch_balance_flag"] = meterbalance.le(watch_balance_threshold).fillna(False)
    summary["high_average_consumption_flag"] = summary.get("average_daily_kwh", pd.Series(dtype="float64")).ge(high_avg_daily_kwh).fillna(False)

    summary["low_balance_with_consumption"] = summary["low_balance_flag"] & summary["has_consumption_records"]
    summary["consumption_without_vend"] = summary["has_consumption_records"] & ~summary["has_vend_records"]
    summary["vend_without_consumption"] = summary["has_vend_records"] & ~summary["has_consumption_records"]
    summary["master_without_activity"] = ~summary["has_vend_records"] & ~summary["has_consumption_records"]
    summary["missing_network_attributes"] = feedercode.isna() | dtcode.isna()
    summary["missing_gis_attributes"] = ~has_valid_gis.fillna(False)
    summary["high_consumption_low_balance"] = summary["high_average_consumption_flag"] & summary["low_balance_flag"]

    summary["exception_score"] = (
        summary[
            [
                "critical_balance_flag",
                "low_balance_with_consumption",
                "consumption_without_vend",
                "vend_without_consumption",
                "missing_network_attributes",
                "missing_gis_attributes",
                "high_consumption_low_balance",
                "master_without_activity",
            ]
        ]
        .fillna(False)
        .astype(int)
        .sum(axis=1)
    )

    balance_bins = [-np.inf, 0, critical_balance_threshold, low_balance_threshold, watch_balance_threshold, np.inf]
    balance_labels = [
        "Negative or zero",
        f"0 to {int(critical_balance_threshold)}",
        f"{int(critical_balance_threshold)} to {int(low_balance_threshold)}",
        f"{int(low_balance_threshold)} to {int(watch_balance_threshold)}",
        f"{int(watch_balance_threshold)}+",
    ]
    summary["balance_band"] = pd.cut(meterbalance, bins=balance_bins, labels=balance_labels)

    return summary


def build_exception_summary(consumer_summary: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return exception counts and a detailed exception population."""

    flag_columns = [
        "critical_balance_flag",
        "low_balance_with_consumption",
        "consumption_without_vend",
        "vend_without_consumption",
        "missing_network_attributes",
        "missing_gis_attributes",
        "high_consumption_low_balance",
        "master_without_activity",
    ]
    labels = {
        "critical_balance_flag": "Critical balance customers",
        "low_balance_with_consumption": "Low balance with consumption",
        "consumption_without_vend": "Consumption but no vend",
        "vend_without_consumption": "Vend but no consumption",
        "missing_network_attributes": "Missing feeder or DT",
        "missing_gis_attributes": "Missing GIS coordinates",
        "high_consumption_low_balance": "High average consumption with low balance",
        "master_without_activity": "Master records with no activity",
    }

    summary_records = []
    for column in flag_columns:
        summary_records.append(
            {
                "exception_type": labels[column],
                "consumer_count": int(consumer_summary.get(column, pd.Series(dtype="bool")).fillna(False).sum()),
            }
        )
    exception_summary = pd.DataFrame(summary_records).sort_values(
        ["consumer_count", "exception_type"],
        ascending=[False, True],
    )

    detailed = consumer_summary[consumer_summary.get("exception_score", pd.Series(dtype="int64")).fillna(0) > 0].copy()
    if not detailed.empty:
        detailed = detailed.sort_values(
            ["exception_score", "meterbalance", "average_daily_kwh"],
            ascending=[False, True, False],
        )
    return exception_summary.reset_index(drop=True), detailed.reset_index(drop=True)


def build_portfolio_metrics(
    master_df: pd.DataFrame,
    vend_df: pd.DataFrame,
    consumption_df: pd.DataFrame,
    consumer_summary: pd.DataFrame,
) -> dict[str, Any]:
    """Build high-level portfolio metrics for the app and overview page."""

    vend_match_rate = float((vend_df.get("join_coverage_status", pd.Series(dtype="string")) != JOIN_STATUS_UNMATCHED).mean() * 100) if len(vend_df) else 0.0
    consumption_match_rate = float((consumption_df.get("join_coverage_status", pd.Series(dtype="string")) != JOIN_STATUS_UNMATCHED).mean() * 100) if len(consumption_df) else 0.0
    feedercode = consumer_summary.get("feedercode", pd.Series(pd.NA, index=consumer_summary.index, dtype="string"))
    dtcode = consumer_summary.get("dtcode", pd.Series(pd.NA, index=consumer_summary.index, dtype="string"))

    return {
        "total_consumers_in_master": int(len(master_df)),
        "total_distinct_meters_in_master": int(master_df.get("meterno_normalized", pd.Series(dtype="string")).nunique(dropna=True)),
        "consumers_with_vend_records": int(consumer_summary.get("has_vend_records", pd.Series(dtype="bool")).fillna(False).sum()),
        "consumers_with_consumption_records": int(consumer_summary.get("has_consumption_records", pd.Series(dtype="bool")).fillna(False).sum()),
        "vend_match_rate_pct": round(vend_match_rate, 2),
        "consumption_match_rate_pct": round(consumption_match_rate, 2),
        "consumers_with_gis": int(consumer_summary.get("has_valid_gis", pd.Series(dtype="bool")).fillna(False).sum()),
        "consumers_with_feeder_dt": int((feedercode.notna() & dtcode.notna()).sum()),
        "total_vend_transactions": int(len(vend_df)),
        "total_consumption_rows": int(len(consumption_df)),
        "total_vend_amount": float(_safe_numeric(vend_df.get("transactionamount", pd.Series(dtype="float64"))).sum()),
        "average_vend_amount": float(_safe_numeric(vend_df.get("transactionamount", pd.Series(dtype="float64"))).mean()) if len(vend_df) else 0.0,
        "median_vend_amount": float(_safe_numeric(vend_df.get("transactionamount", pd.Series(dtype="float64"))).median()) if len(vend_df) else 0.0,
        "average_daily_kwh": float(_safe_numeric(consumption_df.get("kwh_consumption", pd.Series(dtype="float64"))).mean()) if len(consumption_df) else 0.0,
        "average_daily_kvah": float(_safe_numeric(consumption_df.get("kvah_consumption", pd.Series(dtype="float64"))).mean()) if len(consumption_df) else 0.0,
    }


def build_dashboard_analytics(
    master_df: pd.DataFrame,
    vend_df: pd.DataFrame,
    consumption_df: pd.DataFrame,
    app_config: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the derived analytical layer used by the Streamlit pages."""

    master = master_df.copy().reset_index(drop=True)
    if "consumer_master_id" not in master.columns:
        master["consumer_master_id"] = pd.Series(range(1, len(master) + 1), dtype="Int64")

    vend_enriched = enrich_dataset_with_master(vend_df, master)
    consumption_enriched = enrich_dataset_with_master(consumption_df, master)
    consumer_summary = build_consumer_summary(master, vend_enriched, consumption_enriched, app_config)
    exception_summary, exception_detail = build_exception_summary(consumer_summary)

    return {
        "datasets": {
            "consumer_master": master,
            "vend_enriched": vend_enriched,
            "consumption_enriched": consumption_enriched,
            "consumer_summary": consumer_summary,
            "exception_detail": exception_detail,
        },
        "join_coverage": {
            "vend": build_join_coverage_summary(vend_enriched, "vend"),
            "consumption": build_join_coverage_summary(consumption_enriched, "consumption"),
        },
        "exception_summary": exception_summary,
        "vend_timestamp_quality": summarize_vend_timestamp_quality(vend_enriched),
        "portfolio_metrics": build_portfolio_metrics(
            master,
            vend_enriched,
            consumption_enriched,
            consumer_summary,
        ),
    }
