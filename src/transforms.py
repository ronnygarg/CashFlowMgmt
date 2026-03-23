"""Dataset transformations and safe derived-field logic."""

from __future__ import annotations

import re
from typing import Any, Mapping

import pandas as pd

from src.constants import (
    DATE_STATUS_FAILED,
    DATE_STATUS_MISSING,
    DATE_STATUS_PARSED,
    DATE_STATUS_TIME_ONLY,
)
from src.schema_utils import trim_string_values

TIME_ONLY_PATTERN = re.compile(r"^\d{1,2}:\d{2}(:\d{2}(\.\d{1,6})?)?$")


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


def transform_consumption(
    df: pd.DataFrame,
    source_file: str,
    schema: Mapping[str, Any],
    app_config: Mapping[str, Any],
) -> pd.DataFrame:
    """Standardise and enrich the consumption dataset."""

    transformed = trim_string_values(df)
    transformed = _coerce_numeric_columns(transformed, schema.get("numeric_columns", []))
    transformed["source_file"] = source_file
    midnightdate_series = (
        transformed["midnightdate"].astype("string")
        if "midnightdate" in transformed.columns
        else pd.Series(pd.NA, index=transformed.index, dtype="string")
    )
    transformed["midnightdate_raw"] = midnightdate_series

    dayfirst = app_config.get("data", {}).get("date_parsing", {}).get("consumption", {}).get("dayfirst", True)
    transformed["midnightdate_parsed"] = pd.to_datetime(
        midnightdate_series,
        errors="coerce",
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
    transformed["source_file"] = source_file
    issuedate_text = (
        transformed["issuedate"].astype("string")
        if "issuedate" in transformed.columns
        else pd.Series(pd.NA, index=transformed.index, dtype="string")
    )
    transformed["issuedate_raw"] = issuedate_text
    issuedate_text = issuedate_text.fillna("").str.strip()
    is_missing = issuedate_text.eq("")
    is_time_only = issuedate_text.str.match(TIME_ONLY_PATTERN, na=False)

    dayfirst = app_config.get("data", {}).get("date_parsing", {}).get("vend", {}).get("dayfirst", True)
    parsed_datetime = pd.to_datetime(
        issuedate_text.where(~is_missing & ~is_time_only),
        errors="coerce",
        dayfirst=dayfirst,
    )

    parse_status = pd.Series(DATE_STATUS_FAILED, index=transformed.index, dtype="string")
    parse_status.loc[is_missing] = DATE_STATUS_MISSING
    parse_status.loc[is_time_only] = DATE_STATUS_TIME_ONLY
    parse_status.loc[parsed_datetime.notna()] = DATE_STATUS_PARSED

    transformed["issuedate_parse_status"] = parse_status
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
    if dataset_name == "consumption":
        columns.extend(
            [
                "source_file",
                "midnightdate_raw",
                "midnightdate_parsed",
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
