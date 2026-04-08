from types import SimpleNamespace

import pandas as pd

import src.dashboard_data as dashboard_data


def test_load_dashboard_bundle_smoke(monkeypatch) -> None:
    call_log: list[tuple[str, str]] = []

    app_config = {"data": {"known_limitations": [], "expected_datasets": ["consumer_master", "consumption", "vend"]}}
    schema_config = {
        "datasets": {
            "consumer_master": {"required_columns": ["consumernumber"]},
            "consumption": {"required_columns": ["consumernumber"]},
            "vend": {"required_columns": ["meterno"]},
        }
    }
    paths = SimpleNamespace(raw_data_dir="raw", processed_data_dir="processed")
    file_inventory = pd.DataFrame({"dataset": ["consumer_master", "consumption", "vend"]})
    datasets = {
        "consumer_master": pd.DataFrame({"consumernumber": ["C1"]}),
        "consumption": pd.DataFrame({"consumernumber": ["C1"]}),
        "vend": pd.DataFrame({"meterno": ["M1"]}),
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
        lambda paths, app_config, schema_config, force_raw_refresh=False: {
            "datasets": datasets,
            "file_inventory": file_inventory,
            "messages": [],
            "paths": paths,
            "processed_outputs": {},
            "discovered_files": {},
            "load_mode": "processed_parquet",
        },
    )
    monkeypatch.setattr(dashboard_data, "run_dataset_quality_checks", fake_run_quality)
    monkeypatch.setattr(
        dashboard_data,
        "build_dashboard_analytics",
        lambda master_df, vend_df, consumption_df, app_config: {
            "datasets": {
                "consumer_master": master_df.assign(consumer_master_id=[1]),
                "vend_enriched": vend_df,
                "consumption_enriched": consumption_df,
                "consumer_summary": pd.DataFrame(),
                "exception_detail": pd.DataFrame(),
            },
            "join_coverage": {},
            "exception_summary": pd.DataFrame(),
            "vend_timestamp_quality": {},
            "portfolio_metrics": {},
        },
    )
    monkeypatch.setattr(dashboard_data, "validate_required_directories", lambda _: [])

    dashboard_data.clear_dashboard_cache()
    bundle = dashboard_data.load_dashboard_bundle(base_dir_override="F:/Secure/CashFlowMgmt")
    dashboard_data.clear_dashboard_cache()

    assert bundle["app_config"] == app_config
    assert bundle["schema_config"] == schema_config
    assert bundle["missing_directories"] == []
    assert set(bundle["quality"].keys()) == {"consumer_master", "consumption", "vend"}
    assert "derived" in bundle
    assert call_log == [
        ("consumer_master", "consumernumber"),
        ("consumption", "consumernumber"),
        ("vend", "meterno"),
    ]
