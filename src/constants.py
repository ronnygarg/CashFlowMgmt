"""Project-wide constants."""

from __future__ import annotations

from pathlib import Path

DEFAULT_BASE_DIR = Path("F:/Secure/CashFlowMgmt")
BASE_DIR_ENV_VAR = "CFM_BASE_DIR"

DATASET_CONSUMER_MASTER = "consumer_master"
DATASET_CONSUMPTION = "consumption"
DATASET_VEND = "vend"
SUPPORTED_DATASETS = (DATASET_CONSUMER_MASTER, DATASET_CONSUMPTION, DATASET_VEND)

DATE_STATUS_PARSED = "parsed_datetime"
DATE_STATUS_DATE_ONLY = "date_only"
DATE_STATUS_TIME_ONLY = "time_only"
DATE_STATUS_FAILED = "failed"
DATE_STATUS_MISSING = "missing"

JOIN_STATUS_BOTH_KEYS = "both_keys_matched"
JOIN_STATUS_CONSUMER_ONLY = "consumer_only_matched"
JOIN_STATUS_METER_ONLY = "meter_only_matched"
JOIN_STATUS_UNMATCHED = "unmatched"

RESOLUTION_STATUS_RESOLVED_BOTH = "resolved_both_keys"
RESOLUTION_STATUS_RESOLVED_CONSUMER = "resolved_consumer_key"
RESOLUTION_STATUS_RESOLVED_METER = "resolved_meter_key"
RESOLUTION_STATUS_CONFLICTING_KEYS = "conflicting_keys"
RESOLUTION_STATUS_DUPLICATE_MASTER_KEY = "duplicate_master_key"
RESOLUTION_STATUS_UNRESOLVED = "unresolved"

CONSUMPTION_DATE_COLUMN = "midnightdate_parsed"
VEND_DATE_COLUMN = "issuedate_parsed"

LIMITATION_NO_LINKING_KEY = (
    "Vend and consumption are not directly joined to each other. "
    "Both datasets are reconciled against Consumer Master using normalized "
    "consumer number and meter number keys with match-confidence diagnostics."
)

LIMITATION_VEND_DATETIME = (
    "Vend issuedate parsing is provisional. If source values are incomplete or "
    "time-only, the app preserves that limitation instead of inventing a full datetime."
)

