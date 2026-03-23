"""Cached dashboard bundle assembly for Streamlit pages."""

from __future__ import annotations

from typing import Any

import streamlit as st

from src.io_utils import ingest_and_persist
from src.path_utils import (
    ensure_project_directories,
    load_app_config,
    load_schema_config,
    resolve_project_paths,
    validate_required_directories,
)
from src.quality_checks import run_dataset_quality_checks
from src.schema_utils import get_dataset_schema


@st.cache_data(show_spinner="Loading raw files, profiling datasets, and refreshing Parquet outputs...")
def load_dashboard_bundle(base_dir_override: str | None = None) -> dict[str, Any]:
    """Load the shared dashboard bundle for all pages."""

    app_config = load_app_config()
    schema_config = load_schema_config()
    paths = resolve_project_paths(cli_base_dir=base_dir_override, app_config=app_config)
    ensure_project_directories(paths)

    bundle = ingest_and_persist(paths=paths, app_config=app_config, schema_config=schema_config)
    file_inventory = bundle["file_inventory"]

    quality = {
        "consumption": run_dataset_quality_checks(
            dataset_name="consumption",
            df=bundle["datasets"]["consumption"],
            schema=get_dataset_schema(schema_config, "consumption"),
            app_config=app_config,
            file_inventory=file_inventory,
        ),
        "vend": run_dataset_quality_checks(
            dataset_name="vend",
            df=bundle["datasets"]["vend"],
            schema=get_dataset_schema(schema_config, "vend"),
            app_config=app_config,
            file_inventory=file_inventory,
        ),
    }

    bundle["app_config"] = app_config
    bundle["schema_config"] = schema_config
    bundle["quality"] = quality
    bundle["missing_directories"] = validate_required_directories(paths)
    return bundle


def clear_dashboard_cache() -> None:
    """Clear cached dashboard data so the next rerun refreshes raw inputs."""

    st.cache_data.clear()
