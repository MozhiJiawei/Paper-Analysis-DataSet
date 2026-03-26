from __future__ import annotations

import sys
from pathlib import Path


DATASET_ROOT_DIR = Path(__file__).resolve().parent
SUPERPROJECT_ROOT_DIR = DATASET_ROOT_DIR.parents[1]

path = str(SUPERPROJECT_ROOT_DIR)
if path not in sys.path:
    sys.path.insert(0, path)
