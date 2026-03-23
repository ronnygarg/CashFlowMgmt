import pandas as pd

from src.constants import DATE_STATUS_FAILED, DATE_STATUS_PARSED, DATE_STATUS_TIME_ONLY
from src.filters import apply_consumption_filters, apply_vend_filters


def test_apply_consumption_filters_parsed_dates_only() -> None:
    df = pd.DataFrame(
        {
            "mtrid": ["M1", "M2"],
            "midnightdate_parse_success": [True, False],
            "kwh_consumption": [1.0, 2.0],
        }
    )

    filtered = apply_consumption_filters(df, {"parsed_dates_only": True})

    assert len(filtered) == 1
    assert filtered["mtrid"].iloc[0] == "M1"


def test_apply_vend_filters_parse_status_and_full_datetime() -> None:
    df = pd.DataFrame(
        {
            "meterno": ["M1", "M2", "M3"],
            "issuedate_parse_status": [DATE_STATUS_PARSED, DATE_STATUS_TIME_ONLY, DATE_STATUS_FAILED],
            "transactionamount": [10, 20, 30],
        }
    )

    filtered_status = apply_vend_filters(df, {"issuedate_parse_status": [DATE_STATUS_PARSED, DATE_STATUS_TIME_ONLY]})
    filtered_full = apply_vend_filters(df, {"full_datetime_only": True})

    assert len(filtered_status) == 2
    assert set(filtered_status["issuedate_parse_status"].tolist()) == {DATE_STATUS_PARSED, DATE_STATUS_TIME_ONLY}
    assert len(filtered_full) == 1
    assert filtered_full["issuedate_parse_status"].iloc[0] == DATE_STATUS_PARSED
