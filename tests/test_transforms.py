import pandas as pd

from src.constants import (
    DATE_STATUS_DATE_ONLY,
    DATE_STATUS_FAILED,
    DATE_STATUS_MISSING,
    DATE_STATUS_PARSED,
    DATE_STATUS_TIME_ONLY,
)
from src.transforms import (
    empty_dataset_frame,
    transform_consumer_master,
    transform_consumption,
    transform_vend,
)


def _app_config() -> dict:
    return {
        "data": {
            "date_parsing": {
                "consumer_master": {
                    "meterinstallationdate": {"dayfirst": True},
                    "balanceupdatedon": {"dayfirst": True},
                },
                "consumption": {"dayfirst": False},
                "vend": {"dayfirst": False},
            }
        }
    }


def test_transform_consumer_master_builds_normalized_keys_and_parse_columns() -> None:
    df = pd.DataFrame(
        {
            "consumernumber": ["1.231E+03"],
            "meterno": [" NB1001 "],
            "meterinstallationdate": ["11-Mar-2025"],
            "balanceupdatedon": ["25-Mar-26 00:00"],
            "meterbalance": ["199.5"],
            "gis_latitude": ["26.1"],
            "gis_longitude": ["85.1"],
            "feedercode": ["FD01"],
            "dtcode": ["DT01"],
        }
    )

    transformed = transform_consumer_master(
        df=df,
        source_file="ConsumerMaster20260401.csv",
        schema={"numeric_columns": ["meterbalance", "gis_latitude", "gis_longitude"]},
        app_config=_app_config(),
    )

    assert transformed["consumernumber_normalized"].iloc[0] == "1231"
    assert transformed["meterno_normalized"].iloc[0] == "NB1001"
    assert transformed["meterinstallationdate_parse_status"].iloc[0] == DATE_STATUS_DATE_ONLY
    assert transformed["balanceupdatedon_parse_status"].iloc[0] == DATE_STATUS_PARSED
    assert bool(transformed["has_valid_gis"].iloc[0]) is True
    assert bool(transformed["has_feeder_dt"].iloc[0]) is True


def test_transform_consumption_enriches_date_fields() -> None:
    df = pd.DataFrame(
        {
            "consumernumber": ["1001", "1002"],
            "meterno": ["M1", "M2"],
            "midnightdate": ["2026-01-31 00:00:00", "not-a-date"],
            "kwh_abs": ["1.5", "2.5"],
        }
    )

    transformed = transform_consumption(
        df=df,
        source_file="ConsumptionData20260401.csv",
        schema={"numeric_columns": ["kwh_abs"]},
        app_config=_app_config(),
    )

    assert transformed["source_file"].nunique() == 1
    assert transformed["source_file"].iloc[0] == "ConsumptionData20260401.csv"
    assert transformed["midnightdate_parse_success"].tolist() == [True, False]
    assert transformed["midnightdate_parse_status"].tolist() == [DATE_STATUS_PARSED, DATE_STATUS_FAILED]
    assert transformed["date"].notna().sum() == 1
    assert transformed["month"].iloc[0] == "2026-01"


def test_transform_vend_handles_parsed_missing_time_only_date_only_and_failed() -> None:
    df = pd.DataFrame(
        {
            "consumernumber": ["1001", "1002", "1003", "1004", "1005"],
            "meterno": ["M1", "M2", "M3", "M4", "M5"],
            "transactionamount": ["10", "20", "30", "40", "50"],
            "issuedate": ["2026-01-10 08:30", "", "09:45", "2026-01-10", "bad-date"],
        }
    )

    transformed = transform_vend(
        df=df,
        source_file="VendData20260401.csv",
        schema={"numeric_columns": ["transactionamount"]},
        app_config=_app_config(),
    )

    assert transformed["issuedate_parse_status"].tolist() == [
        DATE_STATUS_PARSED,
        DATE_STATUS_MISSING,
        DATE_STATUS_TIME_ONLY,
        DATE_STATUS_DATE_ONLY,
        DATE_STATUS_FAILED,
    ]
    assert transformed["analysis_hour"].iloc[0] == 8
    assert pd.isna(transformed["analysis_hour"].iloc[1])
    assert transformed["analysis_hour"].iloc[2] == 9


def test_empty_dataset_frame_includes_derived_columns() -> None:
    consumer_master = empty_dataset_frame("consumer_master", {"required_columns": ["consumernumber"]})
    consumption = empty_dataset_frame("consumption", {"required_columns": ["consumernumber"]})
    vend = empty_dataset_frame("vend", {"required_columns": ["consumernumber"]})

    assert "balanceupdatedon_parsed" in consumer_master.columns
    assert "midnightdate_parsed" in consumption.columns
    assert "analysis_hour" in vend.columns
