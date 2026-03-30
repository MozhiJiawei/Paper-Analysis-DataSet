from __future__ import annotations

import json

from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.annotator_selection import build_annotator, resolve_annotation_backend
from paper_analysis_dataset.services.rebalance_benchmark import (
    annotate_missing_candidates,
    refresh_benchmark_stats,
)
from paper_analysis_dataset.shared.paths import DATASET_ROOT_DIR


BENCHMARK_ROOT = DATASET_ROOT_DIR / "data" / "benchmarks" / "paper-filter"
DEFAULT_CONCURRENCY = 5


def annotate_benchmark(*, concurrency: int = DEFAULT_CONCURRENCY) -> dict[str, object]:
    repository = AnnotationRepository(BENCHMARK_ROOT)
    backend = resolve_annotation_backend()
    annotator = build_annotator(backend, concurrency=concurrency)
    candidates = repository.load_candidates()

    repository.write_annotations([], repository.annotations_ai_path)
    annotate_summary = annotate_missing_candidates(
        repository,
        candidates,
        annotator=annotator,
        backend=backend,
        concurrency=concurrency,
        skip_existing_annotations=False,
    )
    if all(
        hasattr(repository, attribute)
        for attribute in ("load_records", "annotations_human_path", "merged_path", "stats_path", "write_json")
    ):
        refresh_benchmark_stats(repository)
    return {
        "benchmark_root": str(BENCHMARK_ROOT),
        "total_records": len(candidates),
        "annotations_ai": int(annotate_summary["created"]),
        "backend": backend,
        "concurrency": concurrency,
    }


def main() -> None:
    summary = annotate_benchmark()
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
