import pandas as pd

from src.constants import (
    DATE_STATUS_DATE_ONLY,
    DATE_STATUS_MISSING,
    DATE_STATUS_PARSED,
    DATE_STATUS_TIME_ONLY,
    LIMITATION_VEND_DATETIME,
)
from src.quality_checks import (
    build_outlier_summary,
    build_parse_summary,
    run_dataset_quality_checks,
)


def test_build_parse_summary_for_consumer_master() -> None:
    df = pd.DataFrame(
        {
            "meterinstallationdate_parse_status": [DATE_STATUS_DATE_ONLY, DATE_STATUS_DATE_ONLY],
            "balanceupdatedon_parse_status": [DATE_STATUS_PARSED, DATE_STATUS_MISSING],
        }
    )

    summary = build_parse_summary("consumer_master", df)

    assert set(summary["field"]) == {"meterinstallationdate", "balanceupdatedon"}


def test_build_parse_summary_for_vend_uses_status_counts() -> None:
    df = pd.DataFrame(
        {
            "issuedate_parse_status": [
                DATE_STATUS_PARSED,
                DATE_STATUS_MISSING,
                DATE_STATUS_MISSING,
                DATE_STATUS_TIME_ONLY,
                DATE_STATUS_DATE_ONLY,
            ]
        }
    )

    summary = build_parse_summary("vend", df)
    counts = dict(zip(summary["status"], summary["count"]))

    assert counts[DATE_STATUS_MISSING] == 2
    assert counts[DATE_STATUS_PARSED] == 1
    assert counts[DATE_STATUS_TIME_ONLY] == 1
    assert counts[DATE_STATUS_DATE_ONLY] == 1


def test_build_outlier_summary_detects_extreme_values() -> None:
    df = pd.DataFrame({"transactionamount": [1, 2, 3, 1000, 4, 5]})

    summary = build_outlier_summary(df, ["transactionamount"])

    assert int(summary["outlier_count"].iloc[0]) == 1
    assert float(summary["iqr_multiplier"].iloc[0]) == 1.5


def test_run_dataset_quality_checks_reports_duplicates_key_diagnostics_and_warnings() -> None:
    df = pd.DataFrame(
        {
            "consumernumber": ["1001", "1001"],
            "consumernumber_normalized": ["1001", "1001"],
            "meterno": ["M1", "M1"],
            "meterno_normalized": ["M1", "M1"],
            "transactionamount": ["10", "10"],
            "issuedate_parse_status": [DATE_STATUS_TIME_ONLY, DATE_STATUS_TIME_ONLY],
        }
    )
    file_inventory = pd.DataFrame(
        {
            "dataset": ["vend"],
            "file_name": ["VendData20260401.csv"],
            "rows_read": [2],
            "column_count": [5],
            "schema_valid": [True],
            "missing_required_columns": [""],
            "read_status": ["ok"],
        }
    )

    checks = run_dataset_quality_checks(
        dataset_name="vend",
        df=df,
        schema={"required_columns": ["consumernumber", "meterno", "transactionamount"], "candidate_keys": ["consumernumber", "meterno"]},
        app_config={
            "data": {"known_limitations": ["Known limitation"]},
            "quality_checks": {
                "thresholds": {
                    "outlier_iqr_multiplier": 2.0,
                    "temporal_max_future_days": 0,
                    "stale_data_warning_days": 30,
                    "vend_full_datetime_warning_pct": 70,
                }
            },
        },
        file_inventory=file_inventory,
    )

    assert checks["duplicate_rows"] == 1
    assert LIMITATION_VEND_DATETIME in checks["warnings"]
    assert any("time-only" in warning for warning in checks["warnings"])
    assert not checks["file_diagnostics"].empty
    assert not checks["key_diagnostics"].empty
