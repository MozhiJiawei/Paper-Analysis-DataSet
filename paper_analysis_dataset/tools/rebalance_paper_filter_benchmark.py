from __future__ import annotations

import argparse
import json
from pathlib import Path

from paper_analysis_dataset.services.rebalance_benchmark import (
    DEFAULT_REBALANCE_BATCH_SIZE,
    DEFAULT_REBALANCE_SEED,
    DEFAULT_REBALANCE_VENUES,
    DEFAULT_TARGET_AI_POSITIVE_RATIO,
    rebalance_benchmark,
)
from paper_analysis_dataset.shared.paths import DATASET_ROOT_DIR


BENCHMARK_ROOT = DATASET_ROOT_DIR / "data" / "benchmarks" / "paper-filter"


def main() -> None:
    parser = argparse.ArgumentParser(description="增量补录 accepted 论文并按 AI 层重平衡 paper-filter benchmark")
    parser.add_argument("--paperlists-root", required=True, help="paperlists 根目录")
    parser.add_argument(
        "--venues",
        nargs="+",
        default=[f"{venue}:{year}" for venue, year in DEFAULT_REBALANCE_VENUES],
        help="待补样会议，格式如 iclr:2026 nips:2025",
    )
    parser.add_argument(
        "--target-ai-positive-ratio",
        type=float,
        default=DEFAULT_TARGET_AI_POSITIVE_RATIO,
        help="目标 AI 正样本占比，达到或低于该值后停止",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_REBALANCE_BATCH_SIZE,
        help="每批追加并标注的记录数",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_REBALANCE_SEED,
        help="固定随机种子，保证抽样可复现",
    )
    parser.add_argument(
        "--max-new-records",
        type=int,
        help="最多新增记录数；不传时按目标 AI 占比自动补到停止条件",
    )
    args = parser.parse_args()

    summary = rebalance_benchmark(
        paperlists_root=Path(args.paperlists_root),
        benchmark_root=BENCHMARK_ROOT,
        venue_targets=_parse_venue_targets(args.venues),
        target_ai_positive_ratio=args.target_ai_positive_ratio,
        batch_size=args.batch_size,
        seed=args.seed,
        max_new_records=args.max_new_records,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def _parse_venue_targets(values: list[str]) -> tuple[tuple[str, int], ...]:
    targets: list[tuple[str, int]] = []
    for value in values:
        venue, separator, year = value.partition(":")
        if separator != ":" or not venue.strip() or not year.strip():
            raise ValueError(f"venues 参数格式非法：{value}；应为 venue:year")
        targets.append((venue.strip().lower(), int(year.strip())))
    return tuple(targets)


if __name__ == "__main__":
    main()
