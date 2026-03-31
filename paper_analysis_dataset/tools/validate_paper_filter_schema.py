from __future__ import annotations

import argparse
import json
from pathlib import Path

from paper_analysis_dataset.services.benchmark_schema_validator import (
    DEFAULT_BENCHMARK_ROOT,
    validate_benchmark_schema,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="校验 paper-filter benchmark schema 与数据字段契约")
    parser.add_argument(
        "--benchmark-root",
        type=Path,
        default=DEFAULT_BENCHMARK_ROOT,
        help="待校验的数据集目录",
    )
    args = parser.parse_args()
    summary = validate_benchmark_schema(benchmark_root=args.benchmark_root)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if not summary["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
