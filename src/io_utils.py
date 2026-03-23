"""Data discovery, ingestion, parquet persistence, and reusable IO helpers."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable, Mapping

import pandas as pd

from src.schema_utils import standardize_column_names, summarize_schema_report, validate_required_columns
from src.transforms import empty_dataset_frame, transform_consumption, transform_vend


TransformFunction = Callable[[pd.DataFrame, str, Mapping[str, Any], Mapping[str, Any]], pd.DataFrame]
FILE_INVENTORY_COLUMNS = [
    "dataset",
    "file_name",
    "file_path",
    "file_size_mb",
    "read_status",
    "rows_read",
    "column_count",
    "raw_columns",
    "standardized_columns",
    "schema_valid",
    "missing_required_columns",
    "schema_status",
    "column_mapping",
    "parse_warning_count",
]


def _resolve_duplicate_policy(app_config: Mapping[str, Any]) -> str:
    """Resolve duplicate handling mode from app config."""

    mode = (
        app_config.get("quality_checks", {})
        .get("duplicate_policy", {})
        .get("mode", "keep_all")
    )
    resolved = str(mode).strip().lower()
    if resolved not in {"keep_all", "drop_first", "drop_last", "error"}:
        return "keep_all"
    return resolved


def apply_duplicate_policy(df: pd.DataFrame, policy_mode: str) -> tuple[pd.DataFrame, int, int, list[str]]:
    """Apply duplicate policy and return transformed frame plus diagnostics."""

    if df.empty:
        return df.copy(), 0, 0, []

    duplicate_rows_before = int(df.duplicated().sum())
    messages: list[str] = []

    if policy_mode == "drop_first":
        result = df.drop_duplicates(keep="first").copy()
    elif policy_mode == "drop_last":
        result = df.drop_duplicates(keep="last").copy()
    elif policy_mode == "error":
        result = df.copy()
        if duplicate_rows_before > 0:
            messages.append(
                "Duplicate policy is set to error and duplicates were detected. "
                "Rows were preserved; review Data Quality diagnostics."
            )
    else:
        result = df.copy()

    duplicate_rows_after = int(result.duplicated().sum())
    return result, duplicate_rows_before, duplicate_rows_after, messages


def discover_dataset_files(raw_data_dir: Path, app_config: Mapping[str, Any]) -> dict[str, list[Path]]:
    """Discover raw CSV files for each supported dataset."""

    patterns = app_config.get("supported_file_patterns", {})
    discovered: dict[str, list[Path]] = {}

    for dataset_name, file_patterns in patterns.items():
        matched_files: set[Path] = set()
        for pattern in file_patterns or []:
            matched_files.update(raw_data_dir.glob(pattern))
        discovered[dataset_name] = sorted(matched_files)

    return discovered


def read_csv_file(path: Path) -> pd.DataFrame:
    """Read a raw CSV as strings so identifiers are preserved safely."""

    return pd.read_csv(path, dtype=str)


def _dataset_transformer(dataset_name: str) -> TransformFunction:
    if dataset_name == "consumption":
        return transform_consumption
    if dataset_name == "vend":
        return transform_vend
    raise ValueError(f"Unsupported dataset: {dataset_name}")


def ingest_dataset_files(
    dataset_name: str,
    file_paths: list[Path],
    dataset_schema: Mapping[str, Any],
    app_config: Mapping[str, Any],
) -> tuple[pd.DataFrame, list[dict[str, Any]], list[str]]:
    """Read, standardise, transform, and diagnose a dataset across multiple source files."""

    transformer = _dataset_transformer(dataset_name)
    frames: list[pd.DataFrame] = []
    file_records: list[dict[str, Any]] = []
    messages: list[str] = []

    for file_path in file_paths:
        record: dict[str, Any] = {
            "dataset": dataset_name,
            "file_name": file_path.name,
            "file_path": str(file_path),
            "file_size_mb": round(file_path.stat().st_size / (1024 * 1024), 2),
            "read_status": "ok",
        }

        try:
            raw_df = read_csv_file(file_path)
            standardized_df, column_mapping = standardize_column_names(raw_df)
            schema_report = validate_required_columns(standardized_df, list(dataset_schema.get("required_columns", [])))
            transformed_df = transformer(standardized_df, file_path.name, dataset_schema, app_config)

            record.update(
                {
                    "rows_read": int(len(raw_df)),
                    "column_count": int(len(raw_df.columns)),
                    "raw_columns": ", ".join(raw_df.columns.astype(str).tolist()),
                    "standardized_columns": ", ".join(standardized_df.columns.astype(str).tolist()),
                    "schema_valid": schema_report["is_valid"],
                    "missing_required_columns": ", ".join(schema_report["missing_columns"]),
                    "schema_status": summarize_schema_report(schema_report),
                    "column_mapping": ", ".join(f"{key}->{value}" for key, value in column_mapping.items()),
                }
            )

            if dataset_name == "consumption":
                parse_warning_count = int((~transformed_df["midnightdate_parse_success"].fillna(False)).sum())
            else:
                parse_warning_count = int((transformed_df["issuedate_parse_status"] != "parsed_datetime").sum())

            record["parse_warning_count"] = parse_warning_count
            frames.append(transformed_df)

        except Exception as exc:  # pragma: no cover
            record.update(
                {
                    "rows_read": 0,
                    "column_count": 0,
                    "raw_columns": "",
                    "standardized_columns": "",
                    "schema_valid": False,
                    "missing_required_columns": "",
                    "schema_status": f"Failed to read file: {exc}",
                    "column_mapping": "",
                    "parse_warning_count": 0,
                    "read_status": "error",
                }
            )
            messages.append(f"Failed to read {file_path.name}: {exc}")

        file_records.append(record)

    if not frames:
        messages.append(f"No valid {dataset_name} files were ingested.")
        return empty_dataset_frame(dataset_name, dataset_schema), file_records, messages

    combined = pd.concat(frames, ignore_index=True, sort=False)
    duplicate_policy_mode = _resolve_duplicate_policy(app_config)
    combined, duplicate_rows_before, duplicate_rows_after, duplicate_messages = apply_duplicate_policy(
        combined,
        duplicate_policy_mode,
    )
    messages.extend(duplicate_messages)
    messages.append(
        f"{dataset_name.title()} duplicate policy '{duplicate_policy_mode}': "
        f"{duplicate_rows_before:,} duplicate rows before policy, {duplicate_rows_after:,} after policy."
    )

    for record in file_records:
        record["duplicate_policy_mode"] = duplicate_policy_mode

    return combined, file_records, messages


def write_parquet(df: pd.DataFrame, output_path: Path) -> tuple[bool, str]:
    """Write a dataframe to Parquet with a friendly status message."""

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        return True, f"Wrote {output_path.name} with {len(df):,} rows."
    except Exception as exc:  # pragma: no cover
        return False, f"Failed to write {output_path.name}: {exc}"


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Convert a dataframe to UTF-8 CSV bytes for download buttons."""

    return df.to_csv(index=False).encode("utf-8")


def object_to_json_bytes(payload: Any) -> bytes:
    """Convert a python object into pretty UTF-8 JSON bytes for downloads."""

    return json.dumps(payload, indent=2, default=str).encode("utf-8")


def build_utc_timestamped_filename(prefix: str, generated_at_utc_iso: str, suffix: str = "json") -> str:
    """Build a UTC-stamped filename from an ISO timestamp.

    The output format is `<prefix>_YYYYMMDDTHHMMSSZ.<suffix>`.
    """

    iso_text = str(generated_at_utc_iso).strip()
    if iso_text.endswith("Z"):
        iso_text = f"{iso_text[:-1]}+00:00"

    parsed = datetime.fromisoformat(iso_text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)

    stamp = parsed.strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}_{stamp}.{suffix}"


def ingest_and_persist(
    paths: Any,
    app_config: Mapping[str, Any],
    schema_config: Mapping[str, Any],
) -> dict[str, Any]:
    """Run the end-to-end ingestion flow and persist processed Parquet outputs."""

    dataset_schemas = schema_config.get("datasets", {})
    discovered_files = discover_dataset_files(paths.raw_data_dir, app_config)
    messages: list[str] = []

    consumption_df, consumption_records, consumption_messages = ingest_dataset_files(
        dataset_name="consumption",
        file_paths=discovered_files.get("consumption", []),
        dataset_schema=dataset_schemas.get("consumption", {}),
        app_config=app_config,
    )
    vend_df, vend_records, vend_messages = ingest_dataset_files(
        dataset_name="vend",
        file_paths=discovered_files.get("vend", []),
        dataset_schema=dataset_schemas.get("vend", {}),
        app_config=app_config,
    )

    messages.extend(consumption_messages)
    messages.extend(vend_messages)

    output_config = app_config.get("processed_outputs", {})
    consumption_output = paths.processed_data_dir / output_config.get("consumption_parquet", "consumption.parquet")
    vend_output = paths.processed_data_dir / output_config.get("vend_parquet", "vend.parquet")

    consumption_write_ok, consumption_write_message = write_parquet(consumption_df, consumption_output)
    vend_write_ok, vend_write_message = write_parquet(vend_df, vend_output)
    messages.extend([consumption_write_message, vend_write_message])

    file_inventory = pd.DataFrame(consumption_records + vend_records)
    if file_inventory.empty:
        file_inventory = pd.DataFrame(columns=FILE_INVENTORY_COLUMNS)

    return {
        "paths": paths,
        "datasets": {
            "consumption": consumption_df,
            "vend": vend_df,
        },
        "file_inventory": file_inventory,
        "messages": messages,
        "processed_outputs": {
            "consumption": str(consumption_output),
            "vend": str(vend_output),
            "consumption_write_ok": consumption_write_ok,
            "vend_write_ok": vend_write_ok,
        },
        "discovered_files": {key: [str(path) for path in value] for key, value in discovered_files.items()},
    }
