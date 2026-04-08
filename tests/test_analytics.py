import pandas as pd

from src.analytics import enrich_dataset_with_master, summarize_vend_timestamp_quality


def test_enrich_dataset_with_master_classifies_join_coverage_and_resolution() -> None:
    master = pd.DataFrame(
        {
            "consumer_master_id": [1, 2],
            "consumernumber_normalized": ["1001", "1002"],
            "meterno_normalized": ["M1", "M2"],
        }
    )
    dataset = pd.DataFrame(
        {
            "consumernumber_normalized": ["1001", "9999", "1002"],
            "meterno_normalized": ["M1", "M2", "X"],
        }
    )

    enriched = enrich_dataset_with_master(dataset, master)

    assert enriched["join_coverage_status"].tolist() == [
        "both_keys_matched",
        "meter_only_matched",
        "consumer_only_matched",
    ]
    assert enriched["resolution_status"].tolist() == [
        "resolved_both_keys",
        "resolved_meter_key",
        "resolved_consumer_key",
    ]


def test_summarize_vend_timestamp_quality_flags_mixed_quality() -> None:
    vend = pd.DataFrame(
        {
            "issuedate_parse_status": [
                "parsed_datetime",
                "date_only",
                "time_only",
                "missing",
                "failed",
            ]
        }
    )

    summary = summarize_vend_timestamp_quality(vend)

    assert summary["quality_label"] == "mixed_or_low_quality"
    assert summary["supports_intraday_analysis"] is True
    assert summary["supports_daily_analysis"] is False
