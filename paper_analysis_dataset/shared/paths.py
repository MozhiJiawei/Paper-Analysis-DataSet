from __future__ import annotations

from pathlib import Path


DATASET_ROOT_DIR = Path(__file__).resolve().parents[2]
SUPERPROJECT_ROOT_DIR = DATASET_ROOT_DIR.parents[1]
ARTIFACTS_DIR = DATASET_ROOT_DIR / "artifacts"
DATA_BENCHMARKS_DIR = DATASET_ROOT_DIR / "data" / "benchmarks"

