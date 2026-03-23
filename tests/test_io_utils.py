import json

import pandas as pd

from src.io_utils import apply_duplicate_policy, build_utc_timestamped_filename, object_to_json_bytes


def test_apply_duplicate_policy_keep_all_preserves_duplicates() -> None:
    df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})

    result, before_count, after_count, messages = apply_duplicate_policy(df, "keep_all")

    assert len(result) == 3
    assert before_count == 1
    assert after_count == 1
    assert messages == []


def test_apply_duplicate_policy_drop_first_removes_duplicates() -> None:
    df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})

    result, before_count, after_count, _ = apply_duplicate_policy(df, "drop_first")

    assert len(result) == 2
    assert before_count == 1
    assert after_count == 0


def test_apply_duplicate_policy_error_mode_warns_but_keeps_rows() -> None:
    df = pd.DataFrame({"a": [1, 1], "b": ["x", "x"]})

    result, before_count, after_count, messages = apply_duplicate_policy(df, "error")

    assert len(result) == 2
    assert before_count == 1
    assert after_count == 1
    assert len(messages) == 1


def test_object_to_json_bytes_serializes_payload() -> None:
    payload = {"schema_version": "1.0.0", "quality": {"vend": {"duplicate_rows": 2}}}

    result = object_to_json_bytes(payload)
    decoded = json.loads(result.decode("utf-8"))

    assert decoded["schema_version"] == "1.0.0"
    assert decoded["quality"]["vend"]["duplicate_rows"] == 2


def test_build_utc_timestamped_filename_handles_microseconds_and_offset() -> None:
    file_name = build_utc_timestamped_filename(
        prefix="quality_diagnostics",
        generated_at_utc_iso="2026-03-23T15:34:12.123456+00:00",
    )

    assert file_name == "quality_diagnostics_20260323T153412Z.json"


def test_build_utc_timestamped_filename_handles_z_suffix() -> None:
    file_name = build_utc_timestamped_filename(
        prefix="quality_diagnostics",
        generated_at_utc_iso="2026-03-23T15:34:12Z",
    )

    assert file_name == "quality_diagnostics_20260323T153412Z.json"
