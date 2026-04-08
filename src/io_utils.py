"""Data discovery, ingestion, parquet persistence, and reusable IO helpers."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable, Mapping

import pandas as pd

from src.schema_utils import standardize_column_names, summarize_schema_report, validate_required_columns
from src.transforms import empty_dataset_frame, transform_consumer_master, transform_consumption, transform_vend


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


def processed_output_path(dataset_name: str, paths: Any, app_config: Mapping[str, Any]) -> Path:
    """Resolve the processed Parquet path for a dataset."""

    output_config = app_config.get("processed_outputs", {})
    output_name = output_config.get(f"{dataset_name}_parquet", f"{dataset_name}.parquet")
    return paths.processed_data_dir / output_name


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
    configured_sources = app_config.get("source_files", {})
    discovered: dict[str, list[Path]] = {}

    dataset_names = set(patterns) | set(configured_sources)
    for dataset_name in dataset_names:
        source_settings = configured_sources.get(dataset_name, {})
        preferred_files = source_settings.get("preferred_filenames", [])
        existing_preferred = [raw_data_dir / file_name for file_name in preferred_files if (raw_data_dir / file_name).exists()]
        if existing_preferred:
            discovered[dataset_name] = existing_preferred
            continue

        matched_files: set[Path] = set()
        file_patterns = source_settings.get("patterns", patterns.get(dataset_name, []))
        for pattern in file_patterns or []:
            matched_files.update(raw_data_dir.glob(pattern))
        discovered[dataset_name] = sorted(matched_files)

    return discovered


def read_csv_file(path: Path) -> pd.DataFrame:
    """Read a raw CSV as strings so identifiers are preserved safely."""

    return pd.read_csv(path, dtype=str)


def read_parquet_file(path: Path) -> pd.DataFrame:
    """Read a processed Parquet dataset."""

    return pd.read_parquet(path)


def _dataset_transformer(dataset_name: str) -> TransformFunction:
    if dataset_name == "consumer_master":
        return transform_consumer_master
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

            if dataset_name == "consumer_master":
                parse_warning_count = int(
                    (
                        (transformed_df["meterinstallationdate_parse_status"] == "failed")
                        | (transformed_df["balanceupdatedon_parse_status"] == "failed")
                    ).sum()
                )
            elif dataset_name == "consumption":
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


def load_processed_datasets(
    paths: Any,
    app_config: Mapping[str, Any],
    schema_config: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Load processed Parquet datasets when all expected outputs are available."""

    dataset_schemas = schema_config.get("datasets", {})
    dataset_names = app_config.get("data", {}).get("expected_datasets", list(dataset_schemas))
    processed_paths = {
        dataset_name: processed_output_path(dataset_name, paths, app_config)
        for dataset_name in dataset_names
    }

    if not all(path.exists() for path in processed_paths.values()):
        return None

    datasets: dict[str, pd.DataFrame] = {}
    file_records: list[dict[str, Any]] = []
    messages = [
        "Loaded datasets from processed Parquet outputs. Raw CSV files were not required for this session."
    ]

    for dataset_name, parquet_path in processed_paths.items():
        dataset_df = read_parquet_file(parquet_path)
        datasets[dataset_name] = dataset_df
        file_records.append(
            {
                "dataset": dataset_name,
                "file_name": parquet_path.name,
                "file_path": str(parquet_path),
                "file_size_mb": round(parquet_path.stat().st_size / (1024 * 1024), 2),
                "read_status": "parquet_cache",
                "rows_read": int(len(dataset_df)),
                "column_count": int(len(dataset_df.columns)),
                "raw_columns": "",
                "standardized_columns": ", ".join(dataset_df.columns.astype(str).tolist()),
                "schema_valid": True,
                "missing_required_columns": "",
                "schema_status": "Loaded from processed Parquet output",
                "column_mapping": "",
                "parse_warning_count": 0,
                "duplicate_policy_mode": "from_cached_parquet",
            }
        )

    file_inventory = pd.DataFrame(file_records)
    return {
        "paths": paths,
        "datasets": datasets,
        "file_inventory": file_inventory if not file_inventory.empty else pd.DataFrame(columns=FILE_INVENTORY_COLUMNS),
        "messages": messages,
        "processed_outputs": {dataset_name: str(path) for dataset_name, path in processed_paths.items()},
        "discovered_files": {dataset_name: [] for dataset_name in dataset_names},
        "load_mode": "processed_parquet",
    }


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
    force_raw_refresh: bool = False,
) -> dict[str, Any]:
    """Run the end-to-end ingestion flow and persist processed Parquet outputs."""

    if not force_raw_refresh:
        processed_bundle = load_processed_datasets(paths=paths, app_config=app_config, schema_config=schema_config)
        if processed_bundle is not None:
            return processed_bundle

    dataset_schemas = schema_config.get("datasets", {})
    discovered_files = discover_dataset_files(paths.raw_data_dir, app_config)
    messages: list[str] = []
    if force_raw_refresh:
        messages.append("Forced raw ingestion requested. Rebuilding processed Parquet outputs from raw CSV files.")
    else:
        messages.append("Processed Parquet outputs were missing, so the app rebuilt them from raw CSV files.")

    dataset_names = app_config.get("data", {}).get("expected_datasets", list(dataset_schemas))
    datasets: dict[str, pd.DataFrame] = {}
    file_records: list[dict[str, Any]] = []
    processed_outputs: dict[str, Any] = {}
    output_config = app_config.get("processed_outputs", {})

    for dataset_name in dataset_names:
        dataset_df, dataset_records, dataset_messages = ingest_dataset_files(
            dataset_name=dataset_name,
            file_paths=discovered_files.get(dataset_name, []),
            dataset_schema=dataset_schemas.get(dataset_name, {}),
            app_config=app_config,
        )
        datasets[dataset_name] = dataset_df
        file_records.extend(dataset_records)
        messages.extend(dataset_messages)

        output_path = processed_output_path(dataset_name, paths, app_config)
        write_ok, write_message = write_parquet(dataset_df, output_path)
        processed_outputs[dataset_name] = str(output_path)
        processed_outputs[f"{dataset_name}_write_ok"] = write_ok
        messages.append(write_message)

    file_inventory = pd.DataFrame(file_records)
    if file_inventory.empty:
        file_inventory = pd.DataFrame(columns=FILE_INVENTORY_COLUMNS)

    return {
        "paths": paths,
        "datasets": datasets,
        "file_inventory": file_inventory,
        "messages": messages,
        "processed_outputs": processed_outputs,
        "discovered_files": {key: [str(path) for path in value] for key, value in discovered_files.items()},
        "load_mode": "raw_ingestion",
    }
