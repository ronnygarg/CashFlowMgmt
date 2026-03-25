from pathlib import Path

from src.path_utils import project_root, resolve_base_dir


def test_resolve_base_dir_uses_repo_root_when_config_relative_dot() -> None:
    base_dir, source = resolve_base_dir(app_config={"paths": {"default_base_dir": "."}}, argv=[])

    assert base_dir == project_root()
    assert source == "config_file"


def test_resolve_base_dir_falls_back_to_repo_root_when_config_missing_path() -> None:
    base_dir, source = resolve_base_dir(
        app_config={"paths": {"default_base_dir": "does_not_exist_anywhere_123"}},
        argv=[],
    )

    assert base_dir == project_root()
    assert source == "config_file_missing_fallback_project_root"


def test_resolve_base_dir_keeps_cli_precedence_and_resolves_relative() -> None:
    base_dir, source = resolve_base_dir(cli_base_dir="raw_data", app_config={"paths": {}}, argv=[])

    assert base_dir == (project_root() / "raw_data").resolve()
    assert source == "cli_argument"
