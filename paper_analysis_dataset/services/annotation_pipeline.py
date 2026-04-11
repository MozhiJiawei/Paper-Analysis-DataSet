from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, wait
from typing import Protocol

from paper_analysis_dataset.domain.benchmark import AnnotationRecord, CandidatePaper
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.annotator_selection import build_annotator, resolve_annotation_backend


DEFAULT_CONCURRENCY = 5


class IncrementalAnnotator(Protocol):
    labeler_id: str

    def submit_annotate(self, candidate: CandidatePaper) -> Future[AnnotationRecord]: ...


def annotate_missing_candidates(
    repository: AnnotationRepository,
    candidates: list[CandidatePaper],
    *,
    annotator: IncrementalAnnotator | None = None,
    backend: str | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
    skip_existing_annotations: bool = True,
) -> dict[str, object]:
    if not candidates:
        return {
            "submitted": 0,
            "created": 0,
            "skipped_existing": 0,
            "backend": resolve_annotation_backend(backend=backend),
        }

    existing_annotations = repository.load_annotations(repository.annotations_ai_path)
    existing_ids = {annotation.paper_id for annotation in existing_annotations}
    if skip_existing_annotations:
        missing_candidates = [candidate for candidate in candidates if candidate.paper_id not in existing_ids]
    else:
        missing_candidates = list(candidates)
    skipped_existing = len(candidates) - len(missing_candidates)
    total = len(missing_candidates)

    if not missing_candidates:
        return {
            "submitted": 0,
            "created": 0,
            "skipped_existing": skipped_existing,
            "backend": resolve_annotation_backend(backend=backend),
        }

    runtime_annotator = annotator
    resolved_backend = backend
    if runtime_annotator is None:
        resolved_backend = resolve_annotation_backend(backend=backend)
        runtime_annotator = build_annotator(resolved_backend, concurrency=concurrency)

    pending_iter = iter(missing_candidates)
    pending_futures: dict[Future[AnnotationRecord], CandidatePaper] = {}
    created = 0
    persisted_annotations_by_id = (
        {annotation.paper_id: annotation for annotation in existing_annotations}
        if skip_existing_annotations
        else {}
    )

    for _ in range(min(concurrency, len(missing_candidates))):
        candidate = next(pending_iter, None)
        if candidate is None:
            break
        pending_futures[runtime_annotator.submit_annotate(candidate)] = candidate

    while pending_futures:
        done, _ = wait(pending_futures.keys(), return_when=FIRST_COMPLETED)
        for future in done:
            pending_futures.pop(future)
            annotation = future.result()
            persisted_annotations_by_id[annotation.paper_id] = annotation
            repository.write_annotations(
                list(persisted_annotations_by_id.values()),
                repository.annotations_ai_path,
            )
            created += 1
            print(f"[annotate] {created}/{total} paper_id={annotation.paper_id}")
            print(f"[annotate] checkpoint {created}/{total}")
            next_candidate = next(pending_iter, None)
            if next_candidate is not None:
                pending_futures[runtime_annotator.submit_annotate(next_candidate)] = next_candidate

    return {
        "submitted": len(missing_candidates),
        "created": created,
        "skipped_existing": skipped_existing,
        "backend": resolved_backend or runtime_annotator.labeler_id,
    }


def rebuild_ai_annotations(
    repository: AnnotationRepository,
    candidates: list[CandidatePaper],
    *,
    annotator: IncrementalAnnotator | None = None,
    backend: str | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> dict[str, object]:
    print(
        f"[annotate] start total={len(candidates)} "
        f"backend={resolve_annotation_backend(backend=backend)} concurrency={concurrency}"
    )
    repository.write_annotations([], repository.annotations_ai_path)
    summary = annotate_missing_candidates(
        repository,
        candidates,
        annotator=annotator,
        backend=backend,
        concurrency=concurrency,
        skip_existing_annotations=False,
    )
    print(
        "[annotate] done "
        f"submitted={summary['submitted']} "
        f"created={summary['created']} "
        f"skipped_existing={summary['skipped_existing']}"
    )
    return summary
