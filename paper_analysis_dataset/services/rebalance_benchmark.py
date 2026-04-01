from __future__ import annotations

import random
import re
import string
from collections.abc import Iterable
from concurrent.futures import FIRST_COMPLETED, Future, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from paper_analysis_dataset.domain.benchmark import AnnotationRecord, BenchmarkRecord, CandidatePaper
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.annotator_selection import build_annotator, resolve_annotation_backend
from paper_analysis_dataset.services.benchmark_builder import BenchmarkBuilder
from paper_analysis_dataset.services.benchmark_reporter import build_distribution_report
from paper_analysis_dataset.shared.conference.paperlists_parser import (
    filter_accepted_records,
    load_raw_records,
    normalize_records,
)


DEFAULT_REBALANCE_VENUES: tuple[tuple[str, int], ...] = (("iclr", 2026), ("nips", 2025))
DEFAULT_REBALANCE_BATCH_SIZE = 50
DEFAULT_REBALANCE_SEED = 20260328
DEFAULT_TARGET_AI_POSITIVE_RATIO = 0.30
DEFAULT_CONCURRENCY = 5
FINGERPRINT_TRIM_CHARS = string.punctuation + "，。！？；：、（）【】《》“”‘’"


class IncrementalAnnotator(Protocol):
    labeler_id: str

    def submit_annotate(self, candidate: CandidatePaper) -> Future[AnnotationRecord]: ...


@dataclass(slots=True)
class DedupeCandidateSummary:
    candidates: list[CandidatePaper]
    skipped_existing_ids: list[str]
    skipped_duplicate_ids: list[str]
    skipped_duplicate_fingerprints: list[str]


def rebalance_benchmark(
    *,
    paperlists_root: Path,
    benchmark_root: Path,
    venue_targets: tuple[tuple[str, int], ...] = DEFAULT_REBALANCE_VENUES,
    target_ai_positive_ratio: float = DEFAULT_TARGET_AI_POSITIVE_RATIO,
    batch_size: int = DEFAULT_REBALANCE_BATCH_SIZE,
    seed: int = DEFAULT_REBALANCE_SEED,
    max_new_records: int | None = None,
    annotator: IncrementalAnnotator | None = None,
    backend: str | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> dict[str, object]:
    if batch_size <= 0:
        raise ValueError("batch_size 必须大于 0")
    if max_new_records is not None and max_new_records <= 0:
        raise ValueError("max_new_records 必须大于 0")

    _validate_paperlists_root(paperlists_root, venue_targets)
    repository = AnnotationRepository(benchmark_root)

    existing_records = repository.load_records()
    repository.write_records(existing_records)

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
    current_ratio = _read_ai_positive_ratio(stats)

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

    candidate_pool = build_incremental_candidate_pool(
        paperlists_root=paperlists_root,
        venue_targets=venue_targets,
        builder=BenchmarkBuilder(paperlists_root),
        blocked_paper_ids=blocked_paper_ids,
        blocked_fingerprints=blocked_fingerprints,
    )
    print(
        f"[rebalance] start ratio={current_ratio:.2f} candidate_pool={len(candidate_pool.candidates)}"
    )

    rng = random.Random(seed)
    shuffled_candidates = list(candidate_pool.candidates)
    rng.shuffle(shuffled_candidates)
    if max_new_records is not None:
        shuffled_candidates = shuffled_candidates[:max_new_records]

    batches_completed = 0
    added_records = 0
    selected_ids: set[str] = set()
    selected_fingerprints: set[str] = set()
    exhaustion_reason = ""

    while shuffled_candidates:
        if current_ratio <= target_ai_positive_ratio:
            break

        batch_candidates: list[CandidatePaper] = []
        while shuffled_candidates and len(batch_candidates) < batch_size:
            candidate = shuffled_candidates.pop(0)
            fingerprint = build_title_abstract_fingerprint(candidate.title, candidate.abstract)
            if candidate.paper_id in selected_ids or fingerprint in selected_fingerprints:
                continue
            batch_candidates.append(candidate)
            selected_ids.add(candidate.paper_id)
            selected_fingerprints.add(fingerprint)

        if not batch_candidates:
            exhaustion_reason = "candidate_pool_exhausted_after_dedupe"
            break

        print(f"[rebalance] batch={batches_completed + 1} start size={len(batch_candidates)}")

        next_records = [*existing_records, *[candidate_to_record(item) for item in batch_candidates]]
        duplicate_ids = _find_duplicate_paper_ids(record.paper_id for record in next_records)
        if duplicate_ids:
            raise ValueError(f"写回前发现重复 paper_id，整批失败：{', '.join(duplicate_ids)}")

        repository.write_records(next_records)
        existing_records = next_records
        added_records += len(batch_candidates)

        annotate_missing_candidates(
            repository,
            batch_candidates,
            annotator=annotator,
            backend=backend,
            concurrency=concurrency,
        )
        ai_annotations = repository.load_annotations(repository.annotations_ai_path)
        stats = refresh_benchmark_stats(
            repository,
            records=existing_records,
            annotations_ai=ai_annotations,
            annotations_human=human_annotations,
            merged_annotations=merged_annotations,
        )
        current_ratio = _read_ai_positive_ratio(stats)
        batches_completed += 1
        print(f"[rebalance] batch={batches_completed} added={added_records} ratio={current_ratio:.2f}")

        if max_new_records is not None and added_records >= max_new_records:
            exhaustion_reason = "max_new_records_reached"
            break

    if not exhaustion_reason and current_ratio > target_ai_positive_ratio and not shuffled_candidates:
        exhaustion_reason = "candidate_pool_exhausted"
    if not exhaustion_reason and current_ratio <= target_ai_positive_ratio:
        exhaustion_reason = "target_ratio_reached"

    summary = {
        "benchmark_root": str(benchmark_root),
        "paperlists_root": str(paperlists_root),
        "venues": [f"{venue}:{year}" for venue, year in venue_targets],
        "target_ai_positive_ratio": target_ai_positive_ratio,
        "final_ai_positive_ratio": current_ratio,
        "batch_size": batch_size,
        "seed": seed,
        "batches_completed": batches_completed,
        "added_records": added_records,
        "candidate_pool_size": len(candidate_pool.candidates),
        "remaining_candidates": len(shuffled_candidates),
        "dedupe_summary": {
            "skipped_existing_ids": candidate_pool.skipped_existing_ids,
            "skipped_duplicate_ids": candidate_pool.skipped_duplicate_ids,
            "skipped_duplicate_fingerprints": candidate_pool.skipped_duplicate_fingerprints,
        },
        "stop_reason": exhaustion_reason or "no_action_needed",
    }
    print(
        f"[rebalance] done stop_reason={summary['stop_reason']} added={added_records} ratio={current_ratio:.2f}"
    )
    return summary


def build_incremental_candidate_pool(
    *,
    paperlists_root: Path,
    venue_targets: tuple[tuple[str, int], ...],
    builder: BenchmarkBuilder,
    blocked_paper_ids: set[str],
    blocked_fingerprints: set[str],
) -> DedupeCandidateSummary:
    candidates: list[CandidatePaper] = []
    for venue_key, year in venue_targets:
        source_path = paperlists_root / venue_key / f"{venue_key}{year}.json"
        accepted_papers = normalize_records(
            filter_accepted_records(load_raw_records(source_path, venue_key.upper(), year))
        )
        for paper in sorted(accepted_papers, key=lambda item: item.paper_id):
            candidate = builder._to_candidate(paper)
            if candidate.candidate_negative_tier != "negative":
                continue
            candidates.append(candidate)
    return dedupe_candidates(
        candidates,
        blocked_paper_ids=blocked_paper_ids,
        blocked_fingerprints=blocked_fingerprints,
    )


def dedupe_candidates(
    candidates: list[CandidatePaper],
    *,
    blocked_paper_ids: set[str],
    blocked_fingerprints: set[str],
) -> DedupeCandidateSummary:
    deduped: list[CandidatePaper] = []
    skipped_existing_ids: list[str] = []
    skipped_duplicate_ids: list[str] = []
    skipped_duplicate_fingerprints: list[str] = []
    seen_ids: set[str] = set()
    seen_fingerprints: set[str] = set()

    for candidate in candidates:
        fingerprint = build_title_abstract_fingerprint(candidate.title, candidate.abstract)
        if candidate.paper_id in blocked_paper_ids:
            skipped_existing_ids.append(candidate.paper_id)
            continue
        if fingerprint in blocked_fingerprints:
            skipped_duplicate_fingerprints.append(candidate.paper_id)
            continue
        if candidate.paper_id in seen_ids:
            skipped_duplicate_ids.append(candidate.paper_id)
            continue
        if fingerprint in seen_fingerprints:
            skipped_duplicate_fingerprints.append(candidate.paper_id)
            continue
        seen_ids.add(candidate.paper_id)
        seen_fingerprints.add(fingerprint)
        deduped.append(candidate)

    return DedupeCandidateSummary(
        candidates=deduped,
        skipped_existing_ids=sorted(skipped_existing_ids),
        skipped_duplicate_ids=sorted(skipped_duplicate_ids),
        skipped_duplicate_fingerprints=sorted(skipped_duplicate_fingerprints),
    )


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


def refresh_benchmark_stats(
    repository: AnnotationRepository,
    *,
    records: list[BenchmarkRecord] | None = None,
    annotations_ai: list[AnnotationRecord] | None = None,
    annotations_human: list[AnnotationRecord] | None = None,
    merged_annotations: list[AnnotationRecord] | None = None,
) -> dict[str, object]:
    next_records = records if records is not None else repository.load_records()
    next_ai = annotations_ai if annotations_ai is not None else repository.load_annotations(
        repository.annotations_ai_path
    )
    next_human = (
        annotations_human
        if annotations_human is not None
        else repository.load_annotations(repository.annotations_human_path)
    )
    next_merged = (
        merged_annotations
        if merged_annotations is not None
        else repository.load_annotations(repository.merged_path)
    )
    stats = build_distribution_report(
        next_records,
        annotations_ai=next_ai,
        annotations_human=next_human,
        merged_annotations=next_merged,
    )
    repository.write_json(stats, repository.stats_path)
    return stats


def candidate_to_record(candidate: CandidatePaper) -> BenchmarkRecord:
    return BenchmarkRecord(
        paper_id=candidate.paper_id,
        title=candidate.title,
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
        notes=candidate.notes,
    )


def build_title_abstract_fingerprint(title: str, abstract: str) -> str:
    normalized_title = _normalize_fingerprint_text(title)
    normalized_abstract = _normalize_fingerprint_text(abstract)
    return f"{normalized_title}::{normalized_abstract}"


def _normalize_fingerprint_text(value: str) -> str:
    lowered = value.lower().strip()
    compacted = re.sub(r"\s+", " ", lowered)
    return compacted.strip(FINGERPRINT_TRIM_CHARS)


def _read_ai_positive_ratio(stats: dict[str, object]) -> float:
    by_layer = stats.get("by_layer", {})
    if not isinstance(by_layer, dict):
        return 0.0
    annotations_ai = by_layer.get("annotations_ai", {})
    if not isinstance(annotations_ai, dict):
        return 0.0
    value = annotations_ai.get("positive_ratio", 0.0)
    return float(value)


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


def _find_duplicate_paper_ids(paper_ids: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for paper_id in paper_ids:
        if paper_id in seen:
            duplicates.add(paper_id)
            continue
        seen.add(paper_id)
    return sorted(duplicates)
