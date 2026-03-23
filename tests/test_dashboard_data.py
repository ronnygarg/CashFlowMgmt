from types import SimpleNamespace

import pandas as pd

import src.dashboard_data as dashboard_data


def test_load_dashboard_bundle_smoke(monkeypatch) -> None:
    call_log: list[tuple[str, str]] = []

    app_config = {"data": {"known_limitations": []}}
    schema_config = {
        "datasets": {
            "consumption": {"required_columns": ["mtrid"]},
            "vend": {"required_columns": ["meterno"]},
        }
    }
    paths = SimpleNamespace(raw_data_dir="raw", processed_data_dir="processed")
    file_inventory = pd.DataFrame({"dataset": ["consumption", "vend"]})
    datasets = {
        "consumption": pd.DataFrame({"mtrid": ["M1"]}),
        "vend": pd.DataFrame({"meterno": ["V1"]}),
    }

    def fake_run_quality(dataset_name, df, schema, app_config, file_inventory):
        call_log.append((dataset_name, schema["required_columns"][0]))
        return {"duplicate_rows": 0, "warnings": [], "file_diagnostics": pd.DataFrame()}

    monkeypatch.setattr(dashboard_data, "load_app_config", lambda: app_config)
    monkeypatch.setattr(dashboard_data, "load_schema_config", lambda: schema_config)
    monkeypatch.setattr(dashboard_data, "resolve_project_paths", lambda cli_base_dir, app_config: paths)
    monkeypatch.setattr(dashboard_data, "ensure_project_directories", lambda *_: None)
    monkeypatch.setattr(
        dashboard_data,
        "ingest_and_persist",
        lambda paths, app_config, schema_config: {
            "datasets": datasets,
            "file_inventory": file_inventory,
            "messages": [],
            "paths": paths,
            "processed_outputs": {},
            "discovered_files": {},
        },
    )
    monkeypatch.setattr(dashboard_data, "run_dataset_quality_checks", fake_run_quality)
    monkeypatch.setattr(dashboard_data, "validate_required_directories", lambda _: [])

    dashboard_data.clear_dashboard_cache()
    bundle = dashboard_data.load_dashboard_bundle(base_dir_override="F:/Secure/CashFlowMgmt")
    dashboard_data.clear_dashboard_cache()

    assert bundle["app_config"] == app_config
    assert bundle["schema_config"] == schema_config
    assert bundle["missing_directories"] == []
    assert set(bundle["quality"].keys()) == {"consumption", "vend"}
    assert call_log == [("consumption", "mtrid"), ("vend", "meterno")]
