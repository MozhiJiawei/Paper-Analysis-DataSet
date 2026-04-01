from __future__ import annotations

import json
import os
import subprocess
import sys

from paper_analysis_dataset.services.benchmark_schema_validator import validate_benchmark_schema
from paper_analysis_dataset.shared.paths import DATASET_ROOT_DIR


def run_local_ci() -> dict[str, object]:
    env = dict(os.environ)
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    test_command = [
        sys.executable,
        "-m",
        "unittest",
        "discover",
        "-s",
        str(DATASET_ROOT_DIR / "tests"),
        "-t",
        str(DATASET_ROOT_DIR),
    ]
    schema_summary = validate_benchmark_schema()
    if not schema_summary["ok"]:
        return {
            "ok": False,
            "steps": {
                "schema": schema_summary,
                "unit_tests": {
                    "ok": False,
                    "command": test_command,
                    "returncode": None,
                    "stdout": "",
                    "stderr": "skipped because schema validation failed",
                },
            },
        }

    result = subprocess.run(
        test_command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=DATASET_ROOT_DIR,
        env=env,
        check=False,
    )
    test_summary = {
        "ok": result.returncode == 0,
        "command": test_command,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }
    return {
        "ok": schema_summary["ok"] and test_summary["ok"],
        "steps": {
            "schema": schema_summary,
            "unit_tests": test_summary,
        },
    }


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    summary = run_local_ci()
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if not summary["ok"]:
        returncode = summary["steps"]["unit_tests"]["returncode"]
        raise SystemExit(int(returncode) if isinstance(returncode, int) else 1)


if __name__ == "__main__":
    main()
