"""Build assets for the integrated Bihar generation, demand, and vend report."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.path_utils import ensure_project_directories, load_app_config, resolve_project_paths


WEEKDAY_ORDER = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]


@dataclass(frozen=True)
class OutputPaths:
    """Resolved output locations for processed assets and charts."""

    processed_dir: Path
    overlap_parquet: Path
    weekday_profile_parquet: Path
    summary_parquet: Path
    choice_components_chart: Path
    indexed_overlap_chart: Path
    indexed_weekday_chart: Path


def index_to_mean_100(series: pd.Series) -> pd.Series:
    """Return a mean-100 index while preserving nulls."""

    mean_value = series.mean()
    if pd.isna(mean_value) or mean_value == 0:
        return pd.Series(np.nan, index=series.index, dtype="float64")
    return (series / mean_value) * 100.0


def build_output_paths(base_data_dir: Path, processed_data_dir: Path) -> OutputPaths:
    """Return all output paths for the integrated report assets."""

    processed_dir = processed_data_dir / "generation_demand_vend_report"
    processed_dir.mkdir(parents=True, exist_ok=True)

    return OutputPaths(
        processed_dir=processed_dir,
        overlap_parquet=processed_dir / "generation_demand_vend_overlap.parquet",
        weekday_profile_parquet=processed_dir / "generation_demand_vend_weekday_profile.parquet",
        summary_parquet=processed_dir / "generation_demand_vend_summary.parquet",
        choice_components_chart=base_data_dir / "bihar_generation_choice_components.png",
        indexed_overlap_chart=base_data_dir / "bihar_generation_demand_vend_indexed_overlap.png",
        indexed_weekday_chart=base_data_dir / "bihar_generation_demand_vend_indexed_weekday_profile.png",
    )


def load_optional_metric(metric_path: Path, value_name: str) -> pd.DataFrame:
    """Load a daily metric CSV if available, otherwise return an empty frame."""

    if not metric_path.exists():
        return pd.DataFrame(columns=["date", value_name])

    frame = pd.read_csv(metric_path)
    if frame.empty:
        return pd.DataFrame(columns=["date", value_name])

    frame["date"] = pd.to_datetime(frame["sch_date"], errors="coerce")
    frame = frame.dropna(subset=["date"])
    return frame[["date", "scheduled_energy_mwh"]].rename(columns={"scheduled_energy_mwh": value_name})


def prepare_supply_choice_overlap(
    overlap_daily: pd.DataFrame,
    import_daily: pd.DataFrame,
) -> pd.DataFrame:
    """Build the overlap dataset with the requested generation decision rule."""

    frame = overlap_daily.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.dropna(subset=["date"])

    if "bihar_export_sched_mwh" not in frame.columns:
        frame["bihar_export_sched_mwh"] = 0.0

    frame = frame.merge(import_daily, on="date", how="left")
    frame["bihar_import_sched_mwh"] = pd.to_numeric(
        frame["bihar_import_sched_mwh"],
        errors="coerce",
    ).fillna(0.0)
    frame["bihar_export_sched_mwh"] = pd.to_numeric(
        frame["bihar_export_sched_mwh"],
        errors="coerce",
    ).fillna(0.0)

    frame["gross_plus_imports_mwh"] = (
        frame["bihar_generation_sched_mwh"] + frame["bihar_import_sched_mwh"]
    )
    frame["net_supply_candidate_mwh"] = (
        frame["gross_plus_imports_mwh"] - frame["bihar_export_sched_mwh"]
    )
    frame["chosen_generation_mwh"] = np.where(
        frame["net_supply_candidate_mwh"] > 0,
        frame["net_supply_candidate_mwh"],
        frame["gross_plus_imports_mwh"],
    )
    frame["chosen_generation_rule"] = np.where(
        frame["net_supply_candidate_mwh"] > 0,
        "net_of_exports",
        "gross_plus_imports_fallback",
    )

    frame["chosen_generation_index"] = index_to_mean_100(frame["chosen_generation_mwh"])
    frame["feeder_kwh_index"] = index_to_mean_100(frame["kWh"])
    frame["vend_amount_index"] = index_to_mean_100(frame["vend_amount"])

    return frame.sort_values("date").reset_index(drop=True)


def build_weekday_profile(overlap_frame: pd.DataFrame) -> pd.DataFrame:
    """Aggregate overlap data into ordered weekday means and indexed profiles."""

    weekday_profile = (
        overlap_frame.groupby("DayOfWeek", dropna=False)
        .agg(
            chosen_generation_mwh=("chosen_generation_mwh", "mean"),
            feeder_kwh=("kWh", "mean"),
            vend_amount=("vend_amount", "mean"),
            day_count=("date", "count"),
        )
        .reset_index()
    )

    weekday_profile["DayOfWeek"] = pd.Categorical(
        weekday_profile["DayOfWeek"],
        categories=WEEKDAY_ORDER,
        ordered=True,
    )
    weekday_profile = weekday_profile.sort_values("DayOfWeek").reset_index(drop=True)
    weekday_profile["chosen_generation_index"] = index_to_mean_100(
        weekday_profile["chosen_generation_mwh"]
    )
    weekday_profile["feeder_kwh_index"] = index_to_mean_100(weekday_profile["feeder_kwh"])
    weekday_profile["vend_amount_index"] = index_to_mean_100(weekday_profile["vend_amount"])
    return weekday_profile


def build_summary(overlap_frame: pd.DataFrame, import_daily: pd.DataFrame) -> pd.DataFrame:
    """Create a one-row summary table for report writing and QA."""

    days_positive_net = int((overlap_frame["net_supply_candidate_mwh"] > 0).sum())
    days_fallback = int((overlap_frame["chosen_generation_rule"] == "gross_plus_imports_fallback").sum())

    if days_positive_net == len(overlap_frame):
        report_choice = "net_of_exports"
    elif days_fallback == len(overlap_frame):
        report_choice = "gross_plus_imports_fallback"
    else:
        report_choice = "mixed_by_day"

    summary = pd.DataFrame(
        [
            {
                "overlap_start": overlap_frame["date"].min().date().isoformat(),
                "overlap_end": overlap_frame["date"].max().date().isoformat(),
                "days_in_overlap": int(len(overlap_frame)),
                "reported_import_rows": int(len(import_daily)),
                "reported_import_days": int(import_daily["date"].nunique()) if not import_daily.empty else 0,
                "days_positive_net_candidate": days_positive_net,
                "days_fallback_to_gross_plus_imports": days_fallback,
                "generation_total_mwh": float(overlap_frame["bihar_generation_sched_mwh"].sum()),
                "imports_total_mwh": float(overlap_frame["bihar_import_sched_mwh"].sum()),
                "exports_total_mwh": float(overlap_frame["bihar_export_sched_mwh"].sum()),
                "gross_plus_imports_total_mwh": float(overlap_frame["gross_plus_imports_mwh"].sum()),
                "net_supply_candidate_total_mwh": float(overlap_frame["net_supply_candidate_mwh"].sum()),
                "chosen_generation_total_mwh": float(overlap_frame["chosen_generation_mwh"].sum()),
                "report_level_choice": report_choice,
            }
        ]
    )
    return summary


def save_choice_components_chart(overlap_frame: pd.DataFrame, output_path: Path) -> None:
    """Save the section-one chart showing the decision-rule components."""

    plt.figure(figsize=(12, 5))
    plt.plot(
        overlap_frame["date"],
        overlap_frame["gross_plus_imports_mwh"],
        marker="o",
        linewidth=1.8,
        label="Generation + reported imports",
    )
    plt.plot(
        overlap_frame["date"],
        overlap_frame["bihar_export_sched_mwh"],
        marker="o",
        linewidth=1.4,
        label="Reported exports",
    )
    plt.plot(
        overlap_frame["date"],
        overlap_frame["net_supply_candidate_mwh"],
        linewidth=1.6,
        linestyle="--",
        label="Net candidate after exports",
    )
    plt.plot(
        overlap_frame["date"],
        overlap_frame["chosen_generation_mwh"],
        linewidth=2.4,
        label="Chosen generation series",
    )
    plt.axhline(0, color="black", linewidth=0.8, alpha=0.6)
    plt.title("Bihar generation decision-rule components during feeder overlap")
    plt.ylabel("MWh-equivalent")
    plt.xlabel("Date")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()


def save_indexed_overlap_chart(overlap_frame: pd.DataFrame, output_path: Path) -> None:
    """Save the daily indexed comparison chart for section three."""

    plt.figure(figsize=(12, 5))
    plt.plot(
        overlap_frame["date"],
        overlap_frame["chosen_generation_index"],
        linewidth=1.9,
        label="Chosen generation index",
    )
    plt.plot(
        overlap_frame["date"],
        overlap_frame["feeder_kwh_index"],
        linewidth=1.7,
        label="Demand index (feeder kWh)",
    )
    plt.plot(
        overlap_frame["date"],
        overlap_frame["vend_amount_index"],
        linewidth=1.7,
        label="Vend index",
    )
    plt.title("Indexed overlap comparison: chosen generation, demand, and vend")
    plt.ylabel("Mean = 100")
    plt.xlabel("Date")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()


def save_indexed_weekday_chart(weekday_profile: pd.DataFrame, output_path: Path) -> None:
    """Save the weekday indexed comparison chart for section three."""

    plt.figure(figsize=(10, 4.5))
    x_values = weekday_profile["DayOfWeek"].astype(str)
    plt.plot(
        x_values,
        weekday_profile["chosen_generation_index"],
        marker="o",
        linewidth=1.8,
        label="Chosen generation index",
    )
    plt.plot(
        x_values,
        weekday_profile["feeder_kwh_index"],
        marker="o",
        linewidth=1.6,
        label="Demand index (feeder kWh)",
    )
    plt.plot(
        x_values,
        weekday_profile["vend_amount_index"],
        marker="o",
        linewidth=1.6,
        label="Vend index",
    )
    plt.title("Indexed weekday profile: chosen generation, demand, and vend")
    plt.ylabel("Mean = 100")
    plt.xlabel("Day of week")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()


def main() -> None:
    """Build the processed assets and charts for the integrated report."""

    config = load_app_config()
    paths = resolve_project_paths(app_config=config)
    ensure_project_directories(paths)

    data_dir = paths.processed_data_dir.parent
    output_paths = build_output_paths(base_data_dir=data_dir, processed_data_dir=paths.processed_data_dir)

    overlap_csv_path = paths.processed_data_dir / "33kv_generation_extension" / "33kv_generation_overlap_daily.csv"
    import_csv_path = paths.processed_data_dir / "bihar_energy_flow" / "daily_interstate_to_bihar.csv"

    overlap_daily = pd.read_csv(overlap_csv_path)
    import_daily = load_optional_metric(import_csv_path, "bihar_import_sched_mwh")

    overlap_frame = prepare_supply_choice_overlap(overlap_daily=overlap_daily, import_daily=import_daily)
    weekday_profile = build_weekday_profile(overlap_frame)
    summary = build_summary(overlap_frame=overlap_frame, import_daily=import_daily)

    # TODO: If Bihar import routing becomes reliable, consider adding a net-supply-only comparison branch.
    overlap_frame.to_parquet(output_paths.overlap_parquet, index=False)
    weekday_profile.to_parquet(output_paths.weekday_profile_parquet, index=False)
    summary.to_parquet(output_paths.summary_parquet, index=False)

    save_choice_components_chart(overlap_frame, output_paths.choice_components_chart)
    save_indexed_overlap_chart(overlap_frame, output_paths.indexed_overlap_chart)
    save_indexed_weekday_chart(weekday_profile, output_paths.indexed_weekday_chart)

    print(f"Saved: {output_paths.overlap_parquet}")
    print(f"Saved: {output_paths.weekday_profile_parquet}")
    print(f"Saved: {output_paths.summary_parquet}")
    print(f"Saved: {output_paths.choice_components_chart}")
    print(f"Saved: {output_paths.indexed_overlap_chart}")
    print(f"Saved: {output_paths.indexed_weekday_chart}")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
