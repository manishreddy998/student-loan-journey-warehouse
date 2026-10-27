"""Paths and configuration settings."""
from __future__ import annotations

import os
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CDC_DIR = DATA_DIR / "cdc_events"
TRUTH_DIR = DATA_DIR / "ground_truth"
REAL_DIR = DATA_DIR / "real_cache"
RESULTS = ROOT / "results"
CATALOG_DIR = ROOT / "catalog"
HASURA_DIR = ROOT / "hasura"


def duckdb_path() -> Path:
    env_p = os.environ.get("DUCKDB_PATH")
    if env_p:
        p = Path(env_p)
        return p if p.is_absolute() else ROOT / p
    return DATA_DIR / "student_journey.duckdb"


def load_settings() -> dict:
    with open(ROOT / "configs" / "settings.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
