"""Dataset transformations and safe derived-field logic."""

from __future__ import annotations

import re
from typing import Any, Mapping

import pandas as pd

from src.constants import (
    DATE_STATUS_DATE_ONLY,
    DATE_STATUS_FAILED,
    DATE_STATUS_MISSING,
    DATE_STATUS_PARSED,
    DATE_STATUS_TIME_ONLY,
)
from src.key_utils import add_normalized_key_columns
from src.schema_utils import trim_string_values

TIME_ONLY_PATTERN = re.compile(r"^\d{1,2}:\d{2}(:\d{2}(\.\d{1,6})?)?$")
DATE_ONLY_PATTERN = re.compile(
    r"^(?:\d{4}-\d{2}-\d{2}|\d{1,2}-[A-Za-z]{3}-\d{2,4}|\d{1,2}/\d{1,2}/\d{2,4})$"
)
FULL_DATETIME_PATTERN = re.compile(
    r"^(?:\d{4}-\d{2}-\d{2}|\d{1,2}-[A-Za-z]{3}-\d{2,4}|\d{1,2}/\d{1,2}/\d{2,4})[ T]\d{1,2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?$"
)


def _coerce_numeric_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Convert configured numeric columns to numeric dtype when present."""

    converted = df.copy()
    for column in columns:
        if column in converted.columns:
            converted[column] = pd.to_numeric(converted[column], errors="coerce")
    return converted


def _safe_week(series: pd.Series) -> pd.Series:
    """Return ISO week values as nullable integers."""

    if series.empty:
        return pd.Series(dtype="Int64")
    return series.dt.isocalendar().week.astype("Int64")


def _parse_mixed_datetime(series: pd.Series, dayfirst: bool) -> pd.Series:
    """Parse mixed datetime strings defensively."""

    try:
        return pd.to_datetime(series, errors="coerce", dayfirst=dayfirst, format="mixed")
    except TypeError:  # pragma: no cover
        return pd.to_datetime(series, errors="coerce", dayfirst=dayfirst)


def _classify_temporal_granularity(series: pd.Series) -> pd.Series:
    """Classify timestamp text into missing, time-only, date-only, datetime, or unknown."""

    text = series.astype("string").fillna("").str.strip()
    granularity = pd.Series("unknown", index=series.index, dtype="string")
    granularity.loc[text.eq("")] = DATE_STATUS_MISSING
    granularity.loc[text.str.match(TIME_ONLY_PATTERN, na=False)] = DATE_STATUS_TIME_ONLY
    granularity.loc[text.str.match(DATE_ONLY_PATTERN, na=False)] = DATE_STATUS_DATE_ONLY
    granularity.loc[text.str.match(FULL_DATETIME_PATTERN, na=False)] = DATE_STATUS_PARSED
    return granularity


def _build_parse_status(text: pd.Series, parsed: pd.Series, granularity: pd.Series) -> pd.Series:
    """Build a parse-status series from text, parsed values, and inferred granularity."""

    status = pd.Series(DATE_STATUS_FAILED, index=text.index, dtype="string")
    stripped = text.astype("string").fillna("").str.strip()
    status.loc[stripped.eq("")] = DATE_STATUS_MISSING
    status.loc[granularity == DATE_STATUS_TIME_ONLY] = DATE_STATUS_TIME_ONLY
    status.loc[parsed.notna() & (granularity == DATE_STATUS_DATE_ONLY)] = DATE_STATUS_DATE_ONLY
    status.loc[parsed.notna() & ((granularity == DATE_STATUS_PARSED) | stripped.str.contains(":", regex=False))] = DATE_STATUS_PARSED
    status.loc[parsed.notna() & (status == DATE_STATUS_FAILED)] = DATE_STATUS_DATE_ONLY
    return status


def _transform_date_column(
    transformed: pd.DataFrame,
    source_column: str,
    parsed_column: str,
    status_column: str,
    dayfirst: bool,
) -> pd.DataFrame:
    """Parse a date or datetime column and attach raw/status outputs."""

    raw_text = (
        transformed[source_column].astype("string")
        if source_column in transformed.columns
        else pd.Series(pd.NA, index=transformed.index, dtype="string")
    )
    granularity = _classify_temporal_granularity(raw_text)
    parsed = _parse_mixed_datetime(raw_text.where(granularity != DATE_STATUS_TIME_ONLY), dayfirst=dayfirst)

    transformed[f"{source_column}_raw"] = raw_text
    transformed[parsed_column] = parsed
    transformed[status_column] = _build_parse_status(raw_text, parsed, granularity)
    return transformed


def transform_consumer_master(
    df: pd.DataFrame,
    source_file: str,
    schema: Mapping[str, Any],
    app_config: Mapping[str, Any],
) -> pd.DataFrame:
    """Standardise and enrich the consumer master dataset."""

    transformed = trim_string_values(df)
    transformed = _coerce_numeric_columns(transformed, schema.get("numeric_columns", []))
    transformed = add_normalized_key_columns(transformed, ["consumernumber", "meterno"])
    transformed["source_file"] = source_file

    master_parsing = app_config.get("data", {}).get("date_parsing", {}).get("consumer_master", {})
    transformed = _transform_date_column(
        transformed,
        source_column="meterinstallationdate",
        parsed_column="meterinstallationdate_parsed",
        status_column="meterinstallationdate_parse_status",
        dayfirst=bool(master_parsing.get("meterinstallationdate", {}).get("dayfirst", True)),
    )
    transformed = _transform_date_column(
        transformed,
        source_column="balanceupdatedon",
        parsed_column="balanceupdatedon_parsed",
        status_column="balanceupdatedon_parse_status",
        dayfirst=bool(master_parsing.get("balanceupdatedon", {}).get("dayfirst", True)),
    )

    latitude = pd.to_numeric(transformed.get("gis_latitude"), errors="coerce")
    longitude = pd.to_numeric(transformed.get("gis_longitude"), errors="coerce")
    transformed["has_valid_gis"] = latitude.between(-90, 90, inclusive="both") & longitude.between(-180, 180, inclusive="both")
    transformed["has_feeder_dt"] = transformed.get("feedercode").notna() & transformed.get("dtcode").notna()
    return transformed


def transform_consumption(
    df: pd.DataFrame,
    source_file: str,
    schema: Mapping[str, Any],
    app_config: Mapping[str, Any],
) -> pd.DataFrame:
    """Standardise and enrich the consumption dataset."""

    transformed = trim_string_values(df)
    transformed = _coerce_numeric_columns(transformed, schema.get("numeric_columns", []))
    transformed = add_normalized_key_columns(transformed, ["consumernumber", "meterno"])
    transformed["source_file"] = source_file

    dayfirst = app_config.get("data", {}).get("date_parsing", {}).get("consumption", {}).get("dayfirst", True)
    transformed = _transform_date_column(
        transformed,
        source_column="midnightdate",
        parsed_column="midnightdate_parsed",
        status_column="midnightdate_parse_status",
        dayfirst=dayfirst,
    )
    transformed["midnightdate_parse_success"] = transformed["midnightdate_parsed"].notna()

    transformed["date"] = transformed["midnightdate_parsed"].dt.floor("D")
    transformed["month"] = transformed["midnightdate_parsed"].dt.to_period("M").astype("string")
    transformed["week"] = _safe_week(transformed["midnightdate_parsed"])
    transformed["day_of_week"] = transformed["midnightdate_parsed"].dt.day_name()

    return transformed


def _parse_time_only_series(series: pd.Series) -> pd.Series:
    """Parse time-only strings to extract hour-level insight without inventing dates."""

    parsed = pd.to_datetime(series, errors="coerce")
    return parsed.dt.hour.astype("Int64")


def transform_vend(
    df: pd.DataFrame,
    source_file: str,
    schema: Mapping[str, Any],
    app_config: Mapping[str, Any],
) -> pd.DataFrame:
    """Standardise and enrich the vend dataset without inventing missing date information."""

    transformed = trim_string_values(df)
    transformed = _coerce_numeric_columns(transformed, schema.get("numeric_columns", []))
    transformed = add_normalized_key_columns(transformed, ["consumernumber", "meterno"])
    transformed["source_file"] = source_file
    issuedate_text = (
        transformed["issuedate"].astype("string")
        if "issuedate" in transformed.columns
        else pd.Series(pd.NA, index=transformed.index, dtype="string")
    )
    transformed["issuedate_raw"] = issuedate_text
    granularity = _classify_temporal_granularity(issuedate_text)
    issuedate_text = issuedate_text.fillna("").str.strip()
    is_time_only = granularity == DATE_STATUS_TIME_ONLY

    dayfirst = app_config.get("data", {}).get("date_parsing", {}).get("vend", {}).get("dayfirst", True)
    parsed_datetime = _parse_mixed_datetime(issuedate_text.where(~is_time_only), dayfirst=dayfirst)
    transformed["issuedate_parse_status"] = _build_parse_status(issuedate_text, parsed_datetime, granularity)
    transformed["issuedate_is_time_only"] = is_time_only
    transformed["issuedate_parsed"] = parsed_datetime
    transformed["issuedate_time_only_text"] = issuedate_text.where(is_time_only, pd.NA)
    transformed["issuedate_time_hour"] = _parse_time_only_series(transformed["issuedate_time_only_text"])

    transformed["vend_date"] = transformed["issuedate_parsed"].dt.floor("D")
    transformed["vend_month"] = transformed["issuedate_parsed"].dt.to_period("M").astype("string")
    transformed["vend_week"] = _safe_week(transformed["issuedate_parsed"])
    transformed["vend_day_of_week"] = transformed["issuedate_parsed"].dt.day_name()
    transformed["vend_hour"] = transformed["issuedate_parsed"].dt.hour.astype("Int64")
    transformed["analysis_hour"] = transformed["vend_hour"].fillna(transformed["issuedate_time_hour"]).astype("Int64")

    # TODO: If a future source includes a reliable full timestamp plus timezone context,
    # add richer intraday vend window analysis here.
    return transformed


def empty_dataset_frame(dataset_name: str, schema: Mapping[str, Any]) -> pd.DataFrame:
    """Create an empty frame seeded with required source columns."""

    columns = list(schema.get("required_columns", []))
    if dataset_name == "consumer_master":
        columns.extend(
            [
                "source_file",
                "consumernumber_normalized",
                "meterno_normalized",
                "meterinstallationdate_raw",
                "meterinstallationdate_parsed",
                "meterinstallationdate_parse_status",
                "balanceupdatedon_raw",
                "balanceupdatedon_parsed",
                "balanceupdatedon_parse_status",
                "has_valid_gis",
                "has_feeder_dt",
            ]
        )
    if dataset_name == "consumption":
        columns.extend(
            [
                "source_file",
                "consumernumber_normalized",
                "meterno_normalized",
                "midnightdate_raw",
                "midnightdate_parsed",
                "midnightdate_parse_status",
                "midnightdate_parse_success",
                "date",
                "month",
                "week",
                "day_of_week",
            ]
        )
    if dataset_name == "vend":
        columns.extend(
            [
                "source_file",
                "consumernumber_normalized",
                "meterno_normalized",
                "issuedate_raw",
                "issuedate_parse_status",
                "issuedate_is_time_only",
                "issuedate_parsed",
                "issuedate_time_only_text",
                "issuedate_time_hour",
                "vend_date",
                "vend_month",
                "vend_week",
                "vend_day_of_week",
                "vend_hour",
                "analysis_hour",
            ]
        )
    return pd.DataFrame(columns=list(dict.fromkeys(columns)))
