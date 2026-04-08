import pandas as pd

from src.key_utils import normalize_business_key, normalize_business_key_series


def test_normalize_business_key_handles_scientific_notation_and_whole_decimals() -> None:
    assert normalize_business_key("1.231E+03") == "1231"
    assert normalize_business_key("1231.0") == "1231"


def test_normalize_business_key_preserves_non_numeric_text_and_missing() -> None:
    assert normalize_business_key(" NB1001 ") == "NB1001"
    assert pd.isna(normalize_business_key("   "))


def test_normalize_business_key_series_preserves_nullable_string_dtype() -> None:
    series = pd.Series(["1001.0", "M1", None], dtype="string")

    normalized = normalize_business_key_series(series)

    assert normalized.iloc[0] == "1001"
    assert normalized.iloc[1] == "M1"
    assert pd.isna(normalized.iloc[2])
