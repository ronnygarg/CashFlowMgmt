import pandas as pd

from src.constants import DATE_STATUS_FAILED, DATE_STATUS_MISSING, DATE_STATUS_PARSED, DATE_STATUS_TIME_ONLY
from src.transforms import empty_dataset_frame, transform_consumption, transform_vend


def _app_config() -> dict:
    return {
        "data": {
            "date_parsing": {
                "consumption": {"dayfirst": False},
                "vend": {"dayfirst": False},
            }
        }
    }


def test_transform_consumption_enriches_date_fields() -> None:
    df = pd.DataFrame(
        {
            "mtrid": ["M1", "M2"],
            "midnightdate": ["2026-01-31", "not-a-date"],
            "kwh_abs": ["1.5", "2.5"],
        }
    )

    transformed = transform_consumption(
        df=df,
        source_file="Jan_consumption.csv",
        schema={"numeric_columns": ["kwh_abs"]},
        app_config=_app_config(),
    )

    assert transformed["source_file"].nunique() == 1
    assert transformed["source_file"].iloc[0] == "Jan_consumption.csv"
    assert transformed["midnightdate_parse_success"].tolist() == [True, False]
    assert transformed["date"].notna().sum() == 1
    assert transformed["month"].iloc[0] == "2026-01"
    assert transformed["week"].iloc[0] == 5


def test_transform_vend_handles_parsed_missing_time_only_and_failed() -> None:
    df = pd.DataFrame(
        {
            "servicepointno": ["S1", "S2", "S3", "S4"],
            "meterno": ["M1", "M2", "M3", "M4"],
            "categorycode": ["A", "A", "B", "B"],
            "transactionamount": ["10", "20", "30", "40"],
            "issuedate": ["2026-01-10 08:30", "", "09:45", "bad-date"],
        }
    )

    transformed = transform_vend(
        df=df,
        source_file="vend-01Jan-15Jan.csv",
        schema={"numeric_columns": ["transactionamount"]},
        app_config=_app_config(),
    )

    assert transformed["issuedate_parse_status"].tolist() == [
        DATE_STATUS_PARSED,
        DATE_STATUS_MISSING,
        DATE_STATUS_TIME_ONLY,
        DATE_STATUS_FAILED,
    ]
    issuedate_hours = transformed["issuedate_time_hour"]
    analysis_hours = transformed["analysis_hour"]

    assert pd.isna(issuedate_hours.iloc[0])
    assert pd.isna(issuedate_hours.iloc[1])
    assert issuedate_hours.iloc[2] == 9
    assert pd.isna(issuedate_hours.iloc[3])

    assert analysis_hours.iloc[0] == 8
    assert pd.isna(analysis_hours.iloc[1])
    assert analysis_hours.iloc[2] == 9
    assert pd.isna(analysis_hours.iloc[3])


def test_empty_dataset_frame_includes_derived_columns() -> None:
    consumption = empty_dataset_frame("consumption", {"required_columns": ["mtrid"]})
    vend = empty_dataset_frame("vend", {"required_columns": ["servicepointno"]})

    assert "midnightdate_parsed" in consumption.columns
    assert "analysis_hour" in vend.columns
