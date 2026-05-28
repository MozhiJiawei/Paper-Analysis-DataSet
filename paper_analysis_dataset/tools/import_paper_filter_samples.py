from __future__ import annotations

from argparse import ArgumentParser
import json
import sys
from pathlib import Path

from paper_analysis_dataset.services.benchmark_importer import (
    BenchmarkImportError,
    import_benchmark_json,
)


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        prog="paper-analysis-dataset-import-samples",
        description="导入 dataset-native paper-filter 样本和可选 AI 预标注。",
    )
    parser.add_argument("--input-json", type=Path, required=True, help="归一化导入 payload JSON")
    parser.add_argument(
        "--benchmark-root",
        type=Path,
        default=None,
        help="可选 benchmark 根目录，默认使用仓内 data/benchmarks/paper-filter",
    )
    parser.add_argument("--dry-run", action="store_true", help="只校验和汇总，不写入文件")
    return parser


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    args = build_parser().parse_args()
    try:
        summary = import_benchmark_json(
            args.input_json,
            benchmark_root=args.benchmark_root,
            dry_run=args.dry_run,
        )
    except BenchmarkImportError as exc:
        print(f"[import-samples] error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
