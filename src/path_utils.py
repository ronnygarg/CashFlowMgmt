"""Helpers for resolving configurable project paths."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import yaml

from src.constants import BASE_DIR_ENV_VAR, DEFAULT_BASE_DIR


@dataclass(frozen=True)
class ProjectPaths:
    """Resolved project paths derived from the selected base directory."""

    base_dir: Path
    raw_data_dir: Path
    processed_data_dir: Path
    config_dir: Path
    src_dir: Path
    app_dir: Path
    base_dir_source: str

    def as_display_dict(self) -> dict[str, str]:
        """Return stringified paths for UI display and lightweight serialization."""

        return {
            "base_dir": str(self.base_dir),
            "raw_data_dir": str(self.raw_data_dir),
            "processed_data_dir": str(self.processed_data_dir),
            "config_dir": str(self.config_dir),
            "src_dir": str(self.src_dir),
            "app_dir": str(self.app_dir),
            "base_dir_source": self.base_dir_source,
        }


def project_root() -> Path:
    """Return the repository root based on the current module location."""

    return Path(__file__).resolve().parents[1]


def load_yaml_file(path: Path) -> dict[str, Any]:
    """Load a YAML file into a dictionary."""

    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}

    if not isinstance(loaded, dict):
        raise ValueError(f"Expected a mapping in YAML file: {path}")

    return loaded


def load_app_config(config_path: Path | None = None) -> dict[str, Any]:
    """Load the main application configuration file."""

    path = config_path or project_root() / "config" / "app_config.yaml"
    return load_yaml_file(path)


def load_schema_config(config_path: Path | None = None) -> dict[str, Any]:
    """Load the schema configuration file."""

    path = config_path or project_root() / "config" / "schema_config.yaml"
    return load_yaml_file(path)


def extract_base_dir_arg(argv: Sequence[str] | None = None) -> str | None:
    """Extract a --base-dir CLI value without interfering with Streamlit arguments."""

    args = list(argv or sys.argv[1:])
    for index, token in enumerate(args):
        if token == "--base-dir" and index + 1 < len(args):
            return args[index + 1]
        if token.startswith("--base-dir="):
            return token.split("=", 1)[1]
    return None


def resolve_base_dir(
    cli_base_dir: str | Path | None = None,
    app_config: Mapping[str, Any] | None = None,
    argv: Sequence[str] | None = None,
) -> tuple[Path, str]:
    """Resolve the base directory using the required precedence order."""

    config = app_config or load_app_config()
    config_paths = config.get("paths", {})
    root = project_root()

    def _normalize_base_dir(value: str | Path) -> Path:
        candidate = Path(str(value)).expanduser()
        if not candidate.is_absolute():
            candidate = (root / candidate).resolve()
        return candidate

    cli_value = str(cli_base_dir) if cli_base_dir else extract_base_dir_arg(argv)
    env_value = os.getenv(BASE_DIR_ENV_VAR)
    config_value = config_paths.get("default_base_dir")

    if cli_value:
        return _normalize_base_dir(cli_value), "cli_argument"
    if env_value:
        return _normalize_base_dir(env_value), f"environment_variable:{BASE_DIR_ENV_VAR}"
    if config_value:
        configured = _normalize_base_dir(config_value)
        if configured.exists():
            return configured, "config_file"
        return root, "config_file_missing_fallback_project_root"
    return DEFAULT_BASE_DIR, "hardcoded_fallback"


def build_project_paths(
    base_dir: Path,
    base_dir_source: str,
    app_config: Mapping[str, Any] | None = None,
) -> ProjectPaths:
    """Build all commonly used project paths from the selected base directory."""

    config = app_config or load_app_config()
    path_config = config.get("paths", {})

    return ProjectPaths(
        base_dir=base_dir,
        raw_data_dir=base_dir / path_config.get("raw_data_subdir", "raw_data"),
        processed_data_dir=base_dir / path_config.get("processed_data_subdir", "data/processed"),
        config_dir=base_dir / path_config.get("config_subdir", "config"),
        src_dir=base_dir / path_config.get("src_subdir", "src"),
        app_dir=base_dir / path_config.get("app_subdir", "app"),
        base_dir_source=base_dir_source,
    )


def resolve_project_paths(
    cli_base_dir: str | Path | None = None,
    app_config: Mapping[str, Any] | None = None,
    argv: Sequence[str] | None = None,
) -> ProjectPaths:
    """Resolve the effective project paths for the current run."""

    config = app_config or load_app_config()
    base_dir, source = resolve_base_dir(cli_base_dir=cli_base_dir, app_config=config, argv=argv)
    return build_project_paths(base_dir=base_dir, base_dir_source=source, app_config=config)


def ensure_project_directories(paths: ProjectPaths) -> None:
    """Create required directories that are safe to materialize automatically."""

    paths.processed_data_dir.mkdir(parents=True, exist_ok=True)


def validate_required_directories(paths: ProjectPaths) -> list[str]:
    """Return any missing required directories that should already exist."""

    missing: list[str] = []
    for label, path in (
        ("base_dir", paths.base_dir),
        ("raw_data_dir", paths.raw_data_dir),
        ("config_dir", paths.config_dir),
        ("src_dir", paths.src_dir),
        ("app_dir", paths.app_dir),
    ):
        if not path.exists():
            missing.append(f"{label}: {path}")
    return missing

