from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, wait
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from paper_analysis_dataset.domain.benchmark import AnnotationRecord, CandidatePaper
from paper_analysis_dataset.services.annotation_pipeline import (
    DEFAULT_CONCURRENCY,
    IncrementalAnnotator,
)
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.annotator_selection import build_annotator, resolve_annotation_backend
from paper_analysis_dataset.services.augmentation_plan import (
    DEFAULT_TARGET_POSITIVE_COUNT,
    TARGET_LABEL,
    build_scheduling_augmentation_plan,
)
from paper_analysis_dataset.services.benchmark_builder import (
    DEFAULT_SCHEDULING_AUGMENT_VENUES,
    BenchmarkBuilder,
)
from paper_analysis_dataset.services.rebalance_benchmark import (
    build_title_abstract_fingerprint,
    candidate_to_record,
    dedupe_candidates,
    refresh_benchmark_stats,
)


DEFAULT_AUGMENT_BATCH_TAG = "scheduling"
DEFAULT_AUGMENT_MINIMUM_SCORE = 8
DEFAULT_AUGMENT_REVIEW_BATCH_SIZE = 50


@dataclass(slots=True)
class RankedSchedulingCandidate:
    candidate: CandidatePaper
    venue_rank: int
    score: int
    matched_groups: tuple[str, ...]


def augment_benchmark(
    *,
    paperlists_root: Path,
    benchmark_root: Path,
    venue_targets: tuple[tuple[str, int], ...] = DEFAULT_SCHEDULING_AUGMENT_VENUES,
    target_positive_count: int = DEFAULT_TARGET_POSITIVE_COUNT,
    minimum_score: int = DEFAULT_AUGMENT_MINIMUM_SCORE,
    annotator: IncrementalAnnotator | None = None,
    backend: str | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
    batch_tag: str = DEFAULT_AUGMENT_BATCH_TAG,
    review_batch_size: int = DEFAULT_AUGMENT_REVIEW_BATCH_SIZE,
    max_reviewed_candidates: int | None = None,
) -> dict[str, object]:
    if target_positive_count <= 0:
        raise ValueError("target_positive_count 必须大于 0")
    if review_batch_size <= 0:
        raise ValueError("review_batch_size 必须大于 0")
    if max_reviewed_candidates is not None and max_reviewed_candidates <= 0:
        raise ValueError("max_reviewed_candidates 必须大于 0")

    repository = AnnotationRepository(benchmark_root)
    builder = BenchmarkBuilder(paperlists_root)

    existing_records = repository.load_records()
    ai_annotations = repository.load_annotations(repository.annotations_ai_path)
    human_annotations = repository.load_annotations(repository.annotations_human_path)
    merged_annotations = repository.load_annotations(repository.merged_path)
    conflicts = repository.load_conflicts(repository.conflicts_path)
    stats = refresh_benchmark_stats(
        repository,
        records=existing_records,
        annotations_ai=ai_annotations,
        annotations_human=human_annotations,
        merged_annotations=merged_annotations,
    )
    venue_priority = tuple(f"{venue}:{year}" for venue, year in venue_targets)
    plan = build_scheduling_augmentation_plan(
        stats,
        target_positive_count=target_positive_count,
        venue_priority=venue_priority,
    )
    print(
        f"[augment] start current={plan.current_positive_count} "
        f"target={plan.target_positive_count} gap={plan.gap}"
    )

    if plan.completed:
        print("[augment] done stop_reason=target_already_satisfied added=0")
        return {
            "benchmark_root": str(benchmark_root),
            "paperlists_root": str(paperlists_root),
            "target_label": TARGET_LABEL,
            "plan": plan.to_dict(),
            "added_records": 0,
            "annotated_records": 0,
            "pending_human_review_records": 0,
            "stop_reason": "target_already_satisfied",
            "venues": list(venue_priority),
        }

    blocked_paper_ids = {
        *(record.paper_id for record in existing_records),
        *(annotation.paper_id for annotation in ai_annotations),
        *(annotation.paper_id for annotation in human_annotations),
        *(annotation.paper_id for annotation in merged_annotations),
        *(conflict.paper_id for conflict in conflicts),
    }
    blocked_fingerprints = {
        build_title_abstract_fingerprint(record.title, record.abstract)
        for record in existing_records
    }

    ranked_candidates = _rank_scheduling_candidates(
        builder.build_scheduling_positive_candidates(
            venue_targets=venue_targets,
            minimum_score=minimum_score,
        ),
        venue_targets=venue_targets,
    )
    dedupe_summary = dedupe_candidates(
        [item.candidate for item in ranked_candidates],
        blocked_paper_ids=blocked_paper_ids,
        blocked_fingerprints=blocked_fingerprints,
    )
    ranked_by_id = {item.candidate.paper_id: item for item in ranked_candidates}
    ordered_candidates = [
        ranked_by_id[candidate.paper_id]
        for candidate in dedupe_summary.candidates
        if candidate.paper_id in ranked_by_id
    ]
    if not ordered_candidates:
        print("[augment] done stop_reason=candidate_pool_exhausted added=0")
        return {
            "benchmark_root": str(benchmark_root),
            "paperlists_root": str(paperlists_root),
            "target_label": TARGET_LABEL,
            "plan": plan.to_dict(),
            "added_records": 0,
            "annotated_records": 0,
            "pending_human_review_records": 0,
            "candidate_pool_size": len(dedupe_summary.candidates),
            "stop_reason": "candidate_pool_exhausted",
            "dedupe_summary": _dedupe_summary_payload(dedupe_summary),
            "venues": list(venue_priority),
        }

    batch_id = _build_batch_id(batch_tag)
    accepted_candidates: list[CandidatePaper] = []
    accepted_annotations: list[AnnotationRecord] = []
    reviewed_candidates = 0
    offset = 0

    print(
        f"[augment] review batch={batch_id} candidate_pool={len(ordered_candidates)} "
        f"chunk_size={review_batch_size}"
    )
    while (
        offset < len(ordered_candidates)
        and len(accepted_candidates) < plan.gap
        and (
            max_reviewed_candidates is None
            or reviewed_candidates < max_reviewed_candidates
        )
    ):
        remaining_review_budget = None
        if max_reviewed_candidates is not None:
            remaining_review_budget = max_reviewed_candidates - reviewed_candidates
            if remaining_review_budget <= 0:
                break
        chunk = ordered_candidates[offset : offset + review_batch_size]
        if remaining_review_budget is not None:
            chunk = chunk[:remaining_review_budget]
        chunk_candidates = [item.candidate for item in chunk]
        annotations_by_id = _annotate_candidates_in_memory(
            chunk_candidates,
            annotator=annotator,
            backend=backend,
            concurrency=concurrency,
        )
        reviewed_candidates += len(chunk)
        for item in chunk:
            annotation = annotations_by_id.get(item.candidate.paper_id)
            if annotation is None:
                continue
            if not _is_verified_scheduling_positive(annotation):
                continue
            accepted_candidates.append(
                _with_batch_note(item.candidate, batch_id=batch_id, score=item.score)
            )
            accepted_annotations.append(annotation)
            if len(accepted_candidates) >= plan.gap:
                break
        offset += len(chunk)
        print(
            f"[augment] reviewed={reviewed_candidates}/{len(ordered_candidates)} "
            f"accepted={len(accepted_candidates)} target_gap={plan.gap}"
        )

    if not accepted_candidates:
        print("[augment] done stop_reason=no_ai_verified_scheduling_candidates added=0")
        return {
            "benchmark_root": str(benchmark_root),
            "paperlists_root": str(paperlists_root),
            "target_label": TARGET_LABEL,
            "plan": plan.to_dict(),
            "added_records": 0,
            "annotated_records": 0,
            "pending_human_review_records": 0,
            "candidate_pool_size": len(dedupe_summary.candidates),
            "reviewed_candidates": reviewed_candidates,
            "stop_reason": (
                "review_limit_reached_without_verified_candidates"
                if max_reviewed_candidates is not None and reviewed_candidates >= max_reviewed_candidates
                else "no_ai_verified_scheduling_candidates"
            ),
            "dedupe_summary": _dedupe_summary_payload(dedupe_summary),
            "venues": list(venue_priority),
        }

    print(f"[augment] select batch={batch_id} size={len(accepted_candidates)}")

    next_records = [*existing_records, *[candidate_to_record(item) for item in accepted_candidates]]
    repository.write_records(next_records)
    repository.upsert_annotations(accepted_annotations, repository.annotations_ai_path)
    ai_annotations = repository.load_annotations(repository.annotations_ai_path)
    stats = refresh_benchmark_stats(
        repository,
        records=next_records,
        annotations_ai=ai_annotations,
        annotations_human=human_annotations,
        merged_annotations=merged_annotations,
    )
    final_plan = build_scheduling_augmentation_plan(
        stats,
        target_positive_count=target_positive_count,
        venue_priority=venue_priority,
    )
    fulfilled_count = plan.current_positive_count + len(accepted_candidates)
    remaining_gap = max(0, target_positive_count - fulfilled_count)
    if remaining_gap == 0:
        stop_reason = "target_positive_count_reached"
    elif max_reviewed_candidates is not None and reviewed_candidates >= max_reviewed_candidates:
        stop_reason = "review_limit_reached"
    else:
        stop_reason = "candidate_pool_exhausted_before_target"
    plan_payload = final_plan.to_dict()
    plan_payload["current_positive_counts"][TARGET_LABEL] = fulfilled_count
    plan_payload["gap_by_label"][TARGET_LABEL] = remaining_gap
    print(
        f"[augment] done stop_reason={stop_reason} added={len(accepted_candidates)} "
        f"remaining_gap={remaining_gap}"
    )
    return {
        "benchmark_root": str(benchmark_root),
        "paperlists_root": str(paperlists_root),
        "target_label": TARGET_LABEL,
        "plan": plan_payload,
        "added_records": len(accepted_candidates),
        "annotated_records": len(accepted_annotations),
        "pending_human_review_records": len(accepted_candidates),
        "candidate_pool_size": len(dedupe_summary.candidates),
        "reviewed_candidates": reviewed_candidates,
        "selected_paper_ids": [item.paper_id for item in accepted_candidates],
        "venues": list(venue_priority),
        "batch_id": batch_id,
        "stop_reason": stop_reason,
        "dedupe_summary": _dedupe_summary_payload(dedupe_summary),
    }


def _rank_scheduling_candidates(
    ranked_candidates: list[object],
    *,
    venue_targets: tuple[tuple[str, int], ...],
) -> list[RankedSchedulingCandidate]:
    venue_order = {f"{venue}:{year}": index for index, (venue, year) in enumerate(venue_targets)}
    selected: list[RankedSchedulingCandidate] = []
    for item in ranked_candidates:
        venue_key = f"{item.candidate.venue.split()[0].lower()}:{item.candidate.year}"
        selected.append(
            RankedSchedulingCandidate(
                candidate=item.candidate,
                venue_rank=venue_order.get(venue_key, len(venue_targets)),
                score=item.score,
                matched_groups=item.matched_groups,
            )
        )
    return sorted(selected, key=lambda item: (item.venue_rank, -item.score, item.candidate.paper_id))


def _with_batch_note(candidate: CandidatePaper, *, batch_id: str, score: int) -> CandidatePaper:
    note_parts = [part for part in (candidate.notes, f"augment_batch={batch_id}", f"augment_score={score}") if part]
    return CandidatePaper(
        paper_id=candidate.paper_id,
        title=candidate.title,
        title_zh=candidate.title_zh,
        abstract=candidate.abstract,
        abstract_zh=candidate.abstract_zh,
        authors=candidate.authors,
        venue=candidate.venue,
        year=candidate.year,
        source=candidate.source,
        source_path=candidate.source_path,
        primary_research_object=candidate.primary_research_object,
        candidate_preference_labels=candidate.candidate_preference_labels,
        candidate_negative_tier=candidate.candidate_negative_tier,
        keywords=candidate.keywords,
        notes="；".join(note_parts),
    )


def _build_batch_id(batch_tag: str) -> str:
    return f"{datetime.now().strftime('%Y%m%d')}-{batch_tag}"


def _annotate_candidates_in_memory(
    candidates: list[CandidatePaper],
    *,
    annotator: IncrementalAnnotator | None,
    backend: str | None,
    concurrency: int,
) -> dict[str, AnnotationRecord]:
    if not candidates:
        return {}
    runtime_annotator = annotator
    if runtime_annotator is None:
        runtime_annotator = build_annotator(resolve_annotation_backend(backend=backend), concurrency=concurrency)
    pending_iter = iter(candidates)
    pending_futures: dict[object, CandidatePaper] = {}
    annotations_by_id: dict[str, AnnotationRecord] = {}

    for _ in range(min(concurrency, len(candidates))):
        candidate = next(pending_iter, None)
        if candidate is None:
            break
        pending_futures[runtime_annotator.submit_annotate(candidate)] = candidate

    while pending_futures:
        done, _ = wait(pending_futures.keys(), return_when=FIRST_COMPLETED)
        for future in done:
            pending_futures.pop(future)
            annotation = future.result()
            annotations_by_id[annotation.paper_id] = annotation
            next_candidate = next(pending_iter, None)
            if next_candidate is not None:
                pending_futures[runtime_annotator.submit_annotate(next_candidate)] = next_candidate
    return annotations_by_id


def _is_verified_scheduling_positive(annotation: AnnotationRecord) -> bool:
    return annotation.negative_tier == "positive" and annotation.preference_labels == [TARGET_LABEL]


def _dedupe_summary_payload(summary: object) -> dict[str, object]:
    return {
        "skipped_existing_ids": list(summary.skipped_existing_ids),
        "skipped_duplicate_ids": list(summary.skipped_duplicate_ids),
        "skipped_duplicate_fingerprints": list(summary.skipped_duplicate_fingerprints),
    }
