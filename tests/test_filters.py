import pandas as pd

from src.filters import apply_dimension_filters


def test_apply_dimension_filters_categorical_and_date() -> None:
    df = pd.DataFrame(
        {
            "tariffcode": ["A", "B", "A"],
            "date": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"]),
        }
    )

    filtered = apply_dimension_filters(
        df,
        {
            "tariffcode": ["A"],
            "date_column": "date",
            "date_range": (pd.Timestamp("2026-01-01").date(), pd.Timestamp("2026-01-02").date()),
        },
    )

    assert len(filtered) == 1
    assert filtered["tariffcode"].iloc[0] == "A"
