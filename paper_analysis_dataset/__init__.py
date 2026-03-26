from __future__ import annotations

import sys
from pathlib import Path


SUPERPROJECT_ROOT_DIR = Path(__file__).resolve().parents[3]

path = str(SUPERPROJECT_ROOT_DIR)
if path not in sys.path:
    sys.path.insert(0, path)
