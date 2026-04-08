"""Safe business-key normalization helpers."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import re
from typing import Any

import pandas as pd

NULL_LIKE_TEXT = {"", "na", "n/a", "nan", "null", "none", "<na>"}
NUMERIC_TEXT_PATTERN = re.compile(r"^[+-]?(?:\d+|\d+\.\d+|\.\d+)(?:[Ee][+-]?\d+)?$")


def is_null_like_text(value: Any) -> bool:
    """Return True when a value should be treated as missing text."""

    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except TypeError:
        pass

    text = str(value).strip()
    return text.lower() in NULL_LIKE_TEXT


def normalize_business_key(value: Any) -> str | pd.NA:
    """Normalize a business key without relying on float conversion."""

    if is_null_like_text(value):
        return pd.NA

    text = str(value).strip()
    if not NUMERIC_TEXT_PATTERN.match(text):
        return text

    if "." not in text and "e" not in text.lower():
        return text

    try:
        numeric = Decimal(text)
    except InvalidOperation:
        return text

    if numeric == numeric.to_integral_value():
        return format(numeric.quantize(Decimal("1")), "f")

    normalized = format(numeric.normalize(), "f")
    return normalized.rstrip("0").rstrip(".") if "." in normalized else normalized


def normalize_business_key_series(series: pd.Series) -> pd.Series:
    """Normalize an identifier series while preserving nullable string dtype."""

    return series.map(normalize_business_key).astype("string")


def add_normalized_key_columns(df: pd.DataFrame, key_columns: list[str]) -> pd.DataFrame:
    """Append normalized key columns for the configured business keys."""

    normalized = df.copy()
    for column in key_columns:
        output_column = f"{column}_normalized"
        if column in normalized.columns:
            normalized[output_column] = normalize_business_key_series(normalized[column].astype("string"))
        else:
            normalized[output_column] = pd.Series(pd.NA, index=normalized.index, dtype="string")
    return normalized
