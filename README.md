# Electricity Consumption and Prepayment Exploration Dashboard

## Purpose

This project is a modular Streamlit dashboard for stakeholder demos and operational analysis across:

- consumer master / account context
- recharge / vend behaviour
- daily consumption behaviour
- join coverage and reconciliation quality
- practical risk and exception monitoring
- network and location drill-downs where the source data supports them

The app remains deliberately honest about source quality. It does not force a direct vend-to-consumption merge. Instead, both datasets are reconciled independently against `ConsumerMaster20260401.csv` using normalized `consumernumber` and `meterno` keys.

The dashboard can now start from processed Parquet outputs in [`data/processed`](/f:/Secure/CashFlowMgmt/data/processed) when raw CSV files are not present locally. Raw ingestion is only required to rebuild or refresh those processed outputs.

## Current source files

The dashboard is configured to look under `<base_dir>/raw_data/` and prefer these exact files when present:

These source files are intentionally local-only and are not committed to the repository. Add the required extracts under `raw_data/` when you want to ingest or refresh the dashboard inputs.

- `ConsumerMaster20260401.csv`
- `VendData20260401.csv`
- `ConsumptionData20260401.csv`

Fallback filename patterns are configured in [`config/app_config.yaml`](/f:/Secure/CashFlowMgmt/config/app_config.yaml) so the data layer can still evolve without hard-coding paths in Python.

## What changed in `dashboardv2`

- Added a third core dataset: Consumer Master.
- Reworked ingestion to support `consumer_master`, `vend`, and `consumption`.
- Added safe normalized join keys for `consumernumber` and `meterno`.
- Persisted transformed datasets to Parquet under [`data/processed`](/f:/Secure/CashFlowMgmt/data/processed).
- Replaced the old standalone-only dashboard flow with pages for:
  - Executive Overview
  - Consumer Explorer
  - Vending Analysis
  - Consumption Analysis
  - Network / Spatial View
  - Data Quality & Reconciliation
- Added conservative join coverage and resolution statuses instead of silently forcing matches.
- Added exception logic for low balance, missing activity, missing network/GIS fields, and consumption-versus-vend gaps.

## Join logic

Join handling is intentionally conservative.

1. Raw key columns are read as strings.
2. Internal normalized keys are built for:
   - `consumernumber`
   - `meterno`
3. Normalization rules:
   - trim whitespace
   - treat blank and null-like text as missing
   - safely convert scientific notation using `Decimal`
   - remove trailing `.0` when the value is a whole number
   - preserve non-empty non-numeric text after trimming
4. Vend and consumption are each reconciled to Consumer Master using normalized keys.
5. Coverage categories are surfaced as:
   - `both_keys_matched`
   - `consumer_only_matched`
   - `meter_only_matched`
   - `unmatched`
6. Resolution is stricter than coverage and may still be:
   - `resolved_both_keys`
   - `resolved_consumer_key`
   - `resolved_meter_key`
   - `conflicting_keys`
   - `duplicate_master_key`
   - `unresolved`

This means a row can show some key coverage without being silently assigned a high-confidence master record.

Generated Parquet outputs under `<base_dir>/data/processed/` are also intentionally excluded from Git. The app can create or refresh those files locally during ingestion.

## Base directory precedence

## Timestamp handling

The app inspects and preserves timestamp quality rather than inventing precision.

- `meterinstallationdate`: parsed defensively from observed formats such as `11-Mar-2025`
- `balanceupdatedon`: parsed defensively from observed formats such as `25-Mar-26 00:00`
- `midnightdate`: parsed from daily datetime values such as `2026-01-01 00:00:00`
- `issuedate`: classified as full datetime, date only, time only, missing, or failed

Vend analysis is quality-gated:

- full datetime: daily and intraday analysis are allowed
- date only: daily analysis is allowed, intraday is not
- time only: time-of-day analysis is allowed, calendar trends are not
- mixed or weak quality: the UI surfaces warnings and falls back to safer views

## Dashboard pages

### Executive Overview

- KPI tiles
- join coverage summary
- balance distribution
- vend and consumption headline summaries
- top tariff, area, feeder slices
- quick exception counts

### Consumer Explorer

- search by consumer number or meter number
- master profile
- vend and consumption activity for the selected consumer
- join and exception status
- missing profile fields

### Vending Analysis

- vend amount distribution
- top customers by vend amount and transaction count
- daily or time-of-day analysis depending on `issuedate` quality
- unmatched vend rows
- top-up behaviour segments

### Consumption Analysis

- daily kWh trend
- 7-day rolling average
- customer usage distribution
- top customers by average daily kWh
- substation and voltage splits
- suspicious zero / negative / missing consumption rows

### Network / Spatial View

- summaries by substation, feeder, DT, and area
- GIS scatter when coordinate coverage is usable
- fallback hierarchy tables when GIS quality is weak

### Data Quality & Reconciliation

This page remains prominent by design.

It includes:

- row counts by source
- schema diagnostics
- key duplication diagnostics
- missing-value summaries
- date parse summaries
- numeric flags and outlier indicators
- join coverage and resolution tables
- unmatched vend and consumption populations
- file-level ingest diagnostics
- downloadable JSON export of quality and reconciliation outputs

## How to run

Install dependencies:

```powershell
pip install -r requirements.txt
```

Run the app from the project root:

```powershell
streamlit run app/main.py
```

Default load behavior:

- prefer processed Parquet outputs when available
- fall back to raw CSV ingestion when processed outputs are missing
- use the sidebar `Refresh Raw Ingestion` button to rebuild Parquet outputs from raw files when those raw files are available locally

Optional base directory override:

```powershell
streamlit run app/main.py -- --base-dir F:/Secure/CashFlowMgmt
```

Optional environment variable override:

```powershell
$env:CFM_BASE_DIR = "F:/Secure/CashFlowMgmt"
streamlit run app/main.py
```

## Tests

Install dependencies first, then run:

```powershell
pytest
```

## Important assumptions

- `ConsumerMaster20260401.csv` is the primary reference table.
- Normalized `consumernumber` and normalized `meterno` are the only internal join keys used for reconciliation.
- Raw source columns are preserved alongside normalized keys for diagnostics.
- Business-friendly summaries are preferred over fragile visual complexity.

## Data-quality caveats

- Unmatched rows are surfaced rather than hidden.
- Duplicate or conflicting master keys reduce resolution confidence.
- Vend calendar charts are only shown when `issuedate` quality is safe enough.
- Weak GIS, feeder, DT, tariff, or area coverage is treated as a real limitation and shown in the UI.
- The exception layer is operational and descriptive. It is not a predictive risk model.

## Key implementation files

- [`config/app_config.yaml`](/f:/Secure/CashFlowMgmt/config/app_config.yaml)
- [`config/schema_config.yaml`](/f:/Secure/CashFlowMgmt/config/schema_config.yaml)
- [`src/key_utils.py`](/f:/Secure/CashFlowMgmt/src/key_utils.py)
- [`src/transforms.py`](/f:/Secure/CashFlowMgmt/src/transforms.py)
- [`src/io_utils.py`](/f:/Secure/CashFlowMgmt/src/io_utils.py)
- [`src/analytics.py`](/f:/Secure/CashFlowMgmt/src/analytics.py)
- [`src/quality_checks.py`](/f:/Secure/CashFlowMgmt/src/quality_checks.py)
- [`src/dashboard_data.py`](/f:/Secure/CashFlowMgmt/src/dashboard_data.py)
