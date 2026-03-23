import pandas as pd
import pytest

from src.schema_utils import (
    get_dataset_schema,
    standardize_column_names,
    summarize_schema_report,
    to_snake_case,
    trim_string_values,
    validate_required_columns,
)


def test_to_snake_case_handles_symbols_and_camel_case() -> None:
    assert to_snake_case(" Meter ID ") == "meter_id"
    assert to_snake_case("transactionAmount") == "transaction_amount"
    assert to_snake_case("A-B/C") == "a_b_c"


def test_standardize_column_names_returns_mapping() -> None:
    raw = pd.DataFrame({"Meter ID": ["M1"], "Txn Amount": ["12"]})

    standardized, mapping = standardize_column_names(raw)

    assert list(standardized.columns) == ["meter_id", "txn_amount"]
    assert mapping == {"Meter ID": "meter_id", "Txn Amount": "txn_amount"}


def test_trim_string_values_only_trims_text_columns() -> None:
    frame = pd.DataFrame({"name": ["  A  ", " B"], "value": [1, 2]})

    trimmed = trim_string_values(frame)

    assert trimmed["name"].tolist() == ["A", "B"]
    assert trimmed["value"].tolist() == [1, 2]


def test_validate_required_columns_reports_missing_and_extra() -> None:
    frame = pd.DataFrame({"a": [1], "b": [2]})

    report = validate_required_columns(frame, ["a", "c"])

    assert report["is_valid"] is False
    assert report["missing_columns"] == ["c"]
    assert report["extra_columns"] == ["b"]


def test_get_dataset_schema_raises_when_not_mapping() -> None:
    schema_config = {"datasets": {"consumption": "invalid"}}

    with pytest.raises(ValueError):
        get_dataset_schema(schema_config, "consumption")


def test_summarize_schema_report_message() -> None:
    ok = summarize_schema_report({"missing_columns": []})
    bad = summarize_schema_report({"missing_columns": ["x", "y"]})

    assert ok == "All required columns present"
    assert bad == "Missing required columns: x, y"
