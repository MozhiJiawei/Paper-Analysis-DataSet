from __future__ import annotations

import argparse
import json
from pathlib import Path

from paper_analysis_dataset.services.augment_benchmark import (
    DEFAULT_AUGMENT_BATCH_TAG,
    DEFAULT_AUGMENT_MINIMUM_SCORE,
    augment_benchmark,
)
from paper_analysis_dataset.services.augmentation_plan import DEFAULT_TARGET_POSITIVE_COUNT
from paper_analysis_dataset.services.benchmark_builder import DEFAULT_SCHEDULING_AUGMENT_VENUES
from paper_analysis_dataset.shared.paths import DATASET_ROOT_DIR


BENCHMARK_ROOT = DATASET_ROOT_DIR / "data" / "benchmarks" / "paper-filter"


def main() -> None:
    parser = argparse.ArgumentParser(description="调度类专项增强 paper-filter benchmark，并串联 AI/人工标注流")
    parser.add_argument("--paperlists-root", required=True, help="paperlists 根目录")
    parser.add_argument(
        "--venues",
        nargs="+",
        default=[f"{venue}:{year}" for venue, year in DEFAULT_SCHEDULING_AUGMENT_VENUES],
        help="待扫描会议，格式如 iclr:2026 nips:2025",
    )
    parser.add_argument(
        "--target-positive-count",
        type=int,
        default=DEFAULT_TARGET_POSITIVE_COUNT,
        help="系统与调度优化目标正样本数",
    )
    parser.add_argument(
        "--minimum-score",
        type=int,
        default=DEFAULT_AUGMENT_MINIMUM_SCORE,
        help="调度专项候选最低得分",
    )
    parser.add_argument(
        "--batch-tag",
        default=DEFAULT_AUGMENT_BATCH_TAG,
        help="增强批次标识后缀，用于人工复核队列追踪",
    )
    parser.add_argument(
        "--max-reviewed-candidates",
        type=int,
        help="本轮最多执行 AI 复核的候选数，用于分段跑大召回补样",
    )
    args = parser.parse_args()

    summary = augment_benchmark(
        paperlists_root=Path(args.paperlists_root),
        benchmark_root=BENCHMARK_ROOT,
        venue_targets=_parse_venue_targets(args.venues),
        target_positive_count=args.target_positive_count,
        minimum_score=args.minimum_score,
        batch_tag=args.batch_tag,
        max_reviewed_candidates=args.max_reviewed_candidates,
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
