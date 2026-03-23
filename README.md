# Electricity Consumption and Prepayment Exploration Dashboard

## Purpose

This project is a modular Streamlit dashboard for early-stage exploratory analysis of:

- electricity consumption data
- electricity prepayment vend / recharge transactions
- source-data quality and schema diagnostics

The dashboard is designed for change. It prioritises configurable paths, modular source code, reusable functions, and explicit handling of current data limitations.

## Core constraints

- Do not fabricate or force a merge key between consumption and vend datasets.
- Do not hide data limitations.
- Keep the base directory configurable.
- Keep the Data Quality page prominent.
- Treat vend `issuedate` parsing as provisional when source values are incomplete or time-only.

## Project structure

```text
F:\Secure\CashFlowMgmt\
|-- app\
|   |-- main.py
|   `-- pages\
|       |-- 01_Overview.py
|       |-- 02_Consumption.py
|       |-- 03_Vend_Recharge.py
|       |-- 04_Data_Quality.py
|       `-- 05_Combined_Analysis_Future.py
|-- config\
|   |-- app_config.yaml
|   `-- schema_config.yaml
|-- data\
|   `-- processed\
|-- raw_data\
|-- src\
|   |-- __init__.py
|   |-- charts.py
|   |-- constants.py
|   |-- dashboard_data.py
|   |-- filters.py
|   |-- io_utils.py
|   |-- metrics.py
|   |-- path_utils.py
|   |-- profiling.py
|   |-- quality_checks.py
|   |-- schema_utils.py
|   `-- transforms.py
|-- requirements.txt
`-- README.md
```

## Raw data expectations

The dashboard expects CSV files under `<base_dir>/raw_data/`.

Current sample files:

- `Jan_consumption.csv`
- `vend-01Jan-15Jan.csv`
- `vend-15Jan-23Jan.csv`
- `vend-23Jan-31Jan.csv`

The vend files are automatically discovered and combined into one unified vend dataset.

The ingestion flow is also designed to tolerate future monthly file additions with minimal code change, especially for new CSVs that continue to match the configured filename patterns.

## Base directory precedence

The application resolves the project base directory in this order:

1. CLI argument: `--base-dir`
2. Environment variable: `CFM_BASE_DIR`
3. Config file value in `config/app_config.yaml`
4. Hard-coded fallback: `F:/Secure/CashFlowMgmt`

This path is centralised in [`src/path_utils.py`](/f:/Secure/CashFlowMgmt/src/path_utils.py) and used to resolve:

- `raw_data/`
- `data/processed/`
- `config/`
- `src/`
- `app/`

## How to run

Install dependencies:

```powershell
pip install -r requirements.txt
```

Run the app from the project root:

```powershell
streamlit run app/main.py
```

Optional base directory override:

```powershell
streamlit run app/main.py -- --base-dir F:/Secure/CashFlowMgmt
```

Optional environment variable override:

```powershell
$env:CFM_BASE_DIR = "F:/Secure/CashFlowMgmt"
streamlit run app/main.py
```

## What the app does

On load, the app:

1. discovers raw CSV files from the configured base directory
2. standardises column names to lowercase snake case
3. validates required columns using `config/schema_config.yaml`
4. parses safe date fields and derives lightweight time features
5. combines all vend files into one dataframe
6. writes processed Parquet outputs to:
   - `<base_dir>/data/processed/consumption.parquet`
   - `<base_dir>/data/processed/vend.parquet`
7. exposes exploratory pages for overview, consumption, vend, data quality, and future combined analysis

Date parsing is currently configured to interpret ambiguous values with month-first ordering, so `mm` and `dd` are effectively swapped relative to the earlier day-first setup.

## Dashboard pages

### Overview

- file counts
- row counts
- date coverage where available
- processed output paths
- known limitations
- ingest summary table

### Consumption

- summary KPIs
- daily trends
- distribution views
- meter drill-down
- raw filtered table with download button

### Vend / Recharge

- vend amount and transaction KPIs
- transaction analysis
- category breakdown
- meter and service-point drill-down
- source-file inspection
- raw filtered table with download button

### Data Quality

This page is intentionally prominent and should be reviewed early.

It includes:

- schema comparison
- missing-value summary
- duplicate counts
- parse success and failure counts
- numeric flags
- outlier indicators
- vend datetime warnings
- column profiling
- file-level diagnostics

### Combined Analysis, Future

This page is a placeholder only.

It does not attempt a fake merge. Instead it documents:

- why linked analysis is blocked today
- what keys currently exist in each dataset
- what future bridge artifacts would be needed
- TODO items for future entity resolution and linked analysis

## Assumptions

- Consumption data currently contains fields such as `mtrid`, `midnightdate`, `kwh_abs`, `kvah_abs`, `kwh_consumption`, and `kvah_consumption`.
- Vend data currently contains fields such as `servicepointno`, `meterno`, `categorycode`, `transactionamount`, and `issuedate`.
- Vend file schemas are expected to remain aligned enough to concatenate after standardisation.
- Future monthly CSV additions should continue to follow the configured filename patterns unless `config/app_config.yaml` is updated.

## Known data limitations

- There is currently no validated common key between consumption and vend data.
- Consumption appears keyed by `mtrid`.
- Vend data currently exposes `servicepointno` and `meterno`.
- Combined analysis is intentionally blocked until a mapping table or validated bridge becomes available.
- Vend `issuedate` may be incomplete or time-only in future extracts, so datetime handling remains provisional by design.
- The dashboard currently avoids aggressive cleaning rules so stakeholders can see source-data limitations clearly.

## Extending the project

### Add future monthly files

- Drop new CSV files into `<base_dir>/raw_data/`.
- Keep filenames aligned with the configured patterns in `config/app_config.yaml`.
- Refresh the app from the sidebar to re-run ingestion and regenerate Parquet outputs.

### Extend schema handling

- Update required or expected fields in [`config/schema_config.yaml`](/f:/Secure/CashFlowMgmt/config/schema_config.yaml).
- Add any new parsing or derivation logic in [`src/transforms.py`](/f:/Secure/CashFlowMgmt/src/transforms.py).
- Add new diagnostics in [`src/quality_checks.py`](/f:/Secure/CashFlowMgmt/src/quality_checks.py).

### Change the base folder later

- Prefer passing `--base-dir` when launching the app, or
- set `CFM_BASE_DIR`, or
- update `paths.default_base_dir` in [`config/app_config.yaml`](/f:/Secure/CashFlowMgmt/config/app_config.yaml)

### Add future combined analysis

When a validated linking asset becomes available, likely next steps are:

- add a bridge table between `mtrid`, `meterno`, and `servicepointno`
- create entity-resolution rules with documented confidence levels
- define temporal alignment rules for vend and consumption events
- build a linked analytical layer rather than merging raw source files directly
- add targeted pages for recharge exhaustion, cashflow versus usage, and self-disconnection risk

## Implementation notes

- Path resolution is centralised in [`src/path_utils.py`](/f:/Secure/CashFlowMgmt/src/path_utils.py).
- Ingestion and Parquet persistence live in [`src/io_utils.py`](/f:/Secure/CashFlowMgmt/src/io_utils.py).
- Schema handling is centralised in [`src/schema_utils.py`](/f:/Secure/CashFlowMgmt/src/schema_utils.py).
- Safe dataset transforms live in [`src/transforms.py`](/f:/Secure/CashFlowMgmt/src/transforms.py).
- Data quality checks live in [`src/quality_checks.py`](/f:/Secure/CashFlowMgmt/src/quality_checks.py).
- Streamlit pages stay intentionally thin by using the cached bundle in [`src/dashboard_data.py`](/f:/Secure/CashFlowMgmt/src/dashboard_data.py).

## TODOs

- Add richer anomaly rules once business thresholds are available.
- Add support for bridge tables and validated entity resolution.
- Add combined cashflow versus consumption analysis when linking is reliable.
- Add recharge exhaustion and self-disconnection risk workflows after the data model evolves.
