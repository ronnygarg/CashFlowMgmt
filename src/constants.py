"""Project-wide constants."""

from __future__ import annotations

from pathlib import Path

DEFAULT_BASE_DIR = Path("F:/Secure/CashFlowMgmt")
BASE_DIR_ENV_VAR = "CFM_BASE_DIR"

DATASET_CONSUMPTION = "consumption"
DATASET_VEND = "vend"
SUPPORTED_DATASETS = (DATASET_CONSUMPTION, DATASET_VEND)

DATE_STATUS_PARSED = "parsed_datetime"
DATE_STATUS_TIME_ONLY = "time_only"
DATE_STATUS_FAILED = "failed"
DATE_STATUS_MISSING = "missing"

CONSUMPTION_DATE_COLUMN = "midnightdate_parsed"
VEND_DATE_COLUMN = "issuedate_parsed"

LIMITATION_NO_LINKING_KEY = (
    "Combined analysis is intentionally blocked because no validated linking key "
    "currently exists between the consumption and vend datasets."
)

LIMITATION_VEND_DATETIME = (
    "Vend issuedate parsing is provisional. If source values are incomplete or "
    "time-only, the app preserves that limitation instead of inventing a full datetime."
)

