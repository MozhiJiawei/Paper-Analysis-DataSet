from __future__ import annotations

import json
from concurrent.futures import FIRST_COMPLETED, Future, wait

from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.annotator_selection import build_annotator, resolve_annotation_backend
from paper_analysis_dataset.shared.paths import DATASET_ROOT_DIR


BENCHMARK_ROOT = DATASET_ROOT_DIR / "data" / "benchmarks" / "paper-filter"
DEFAULT_CONCURRENCY = 5


def annotate_benchmark(*, concurrency: int = DEFAULT_CONCURRENCY) -> dict[str, object]:
    repository = AnnotationRepository(BENCHMARK_ROOT)
    backend = resolve_annotation_backend()
    annotator = build_annotator(backend, concurrency=concurrency)
    candidates = repository.load_candidates()
    annotations_by_id: dict[str, object] = {}
    pending_iter = iter(candidates)
    pending_futures: dict[Future[object], object] = {}

    repository.write_annotations([], repository.annotations_ai_path)

    for _ in range(min(concurrency, len(candidates))):
        candidate = next(pending_iter, None)
        if candidate is None:
            break
        pending_futures[annotator.submit_annotate(candidate)] = candidate

    while pending_futures:
        done, _ = wait(pending_futures.keys(), return_when=FIRST_COMPLETED)
        for future in done:
            candidate = pending_futures.pop(future)
            annotations_by_id[candidate.paper_id] = future.result()
            repository.write_annotations(list(annotations_by_id.values()), repository.annotations_ai_path)

            next_candidate = next(pending_iter, None)
            if next_candidate is not None:
                pending_futures[annotator.submit_annotate(next_candidate)] = next_candidate
    return {
        "benchmark_root": str(BENCHMARK_ROOT),
        "total_records": len(candidates),
        "annotations_ai": len(annotations_by_id),
        "backend": backend,
        "concurrency": concurrency,
    }


def main() -> None:
    summary = annotate_benchmark()
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
