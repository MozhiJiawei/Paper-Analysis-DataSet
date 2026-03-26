from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from paper_analysis_dataset.shared.paths import DATASET_ROOT_DIR


def run_local_ci() -> dict[str, object]:
    env = dict(os.environ)
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    command = [
        sys.executable,
        "-m",
        "unittest",
        "discover",
        "-s",
        str(DATASET_ROOT_DIR / "tests" / "unit"),
        "-t",
        str(DATASET_ROOT_DIR),
    ]
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=DATASET_ROOT_DIR,
        env=env,
        check=False,
    )
    return {
        "ok": result.returncode == 0,
        "command": command,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    summary = run_local_ci()
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if not summary["ok"]:
        raise SystemExit(int(summary["returncode"]))


if __name__ == "__main__":
    main()
