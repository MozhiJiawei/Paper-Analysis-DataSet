from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.benchmark_builder import (
    BenchmarkBuilder,
    DEFAULT_RELEASE_QUOTA_BY_VENUE,
    DEFAULT_VENUE_TARGETS,
)
from paper_analysis_dataset.services.rebalance_benchmark import refresh_benchmark_stats
from paper_analysis_dataset.services.doubao_abstract_translator import DoubaoAbstractTranslator
from paper_analysis_dataset.services.paper_filter_schema import build_schema_payload
from paper_analysis_dataset.shared.paths import DATASET_ROOT_DIR


BENCHMARK_ROOT = DATASET_ROOT_DIR / "data" / "benchmarks" / "paper-filter"


def rebuild_benchmark(
    *,
    paperlists_root: Path,
    benchmark_root: Path | None = None,
    abstract_translator: object | None = None,
    venue_targets: tuple[tuple[str, int], ...] = DEFAULT_VENUE_TARGETS,
    quota_by_venue: dict[tuple[str, int], int] | None = None,
    minimum_score: int = 12,
) -> dict[str, object]:
    _validate_paperlists_root(paperlists_root, venue_targets)
    target_root = benchmark_root or BENCHMARK_ROOT
    if target_root.exists():
        shutil.rmtree(target_root)

    repository = AnnotationRepository(target_root)
    builder = BenchmarkBuilder(paperlists_root)
    candidates = builder.build_inference_acceleration_candidates(
        venue_targets,
        quota_by_venue=quota_by_venue or DEFAULT_RELEASE_QUOTA_BY_VENUE,
        minimum_score=minimum_score,
    )
    records = builder.build_records(candidates, abstract_translator=abstract_translator)

    repository.write_records(records)
    repository.write_annotations([], repository.annotations_ai_path)
    repository.write_annotations([], repository.annotations_human_path)
    repository.write_annotations([], repository.merged_path)
    repository.write_conflicts([], repository.conflicts_path)
    repository.write_json(build_schema_payload(), repository.schema_path)
    stats = refresh_benchmark_stats(repository, records=records, annotations_ai=[], annotations_human=[], merged_annotations=[])

    return {
        "benchmark_root": str(target_root),
        "paperlists_root": str(paperlists_root),
        "total_records": len(records),
        "annotations_ai": 0,
        "annotations_human": 0,
        "merged": 0,
        "venues": builder.summarize_dataset(records)["venues"],
        "by_primary_research_object": stats["by_primary_research_object"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="重建 paper-filter benchmark 数据集")
    parser.add_argument(
        "--paperlists-root",
        required=True,
        help="会议 JSON 根目录，例如独立 clone 的 paperlists 根路径",
    )
    parser.add_argument(
        "--with-doubao-abstract-translation",
        action="store_true",
        help="显式启用 Doubao 中文摘要生成；未开启时保留原始 abstract_zh",
    )
    args = parser.parse_args()
    summary = rebuild_benchmark(
        paperlists_root=Path(args.paperlists_root),
        abstract_translator=(
            DoubaoAbstractTranslator() if args.with_doubao_abstract_translation else None
        ),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def _validate_paperlists_root(
    paperlists_root: Path,
    venue_targets: tuple[tuple[str, int], ...],
) -> None:
    if not paperlists_root.exists():
        raise ValueError(f"paperlists 根目录不存在：{paperlists_root}")
    if not paperlists_root.is_dir():
        raise ValueError(f"paperlists 根路径不是目录：{paperlists_root}")
    for venue_key, year in venue_targets:
        source_path = paperlists_root / venue_key / f"{venue_key}{year}.json"
        if not source_path.exists():
            raise ValueError(f"缺少会议数据文件：{source_path}")


if __name__ == "__main__":
    main()
