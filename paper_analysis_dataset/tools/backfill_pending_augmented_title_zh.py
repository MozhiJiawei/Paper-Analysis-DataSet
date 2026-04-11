from __future__ import annotations

import argparse
import json
import shutil
from concurrent.futures import FIRST_COMPLETED, Future, wait
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from paper_analysis_dataset.domain.benchmark import BenchmarkRecord, CandidatePaper
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.doubao_title_translator import DoubaoTitleTranslator
from paper_analysis_dataset.shared.paths import DATASET_ROOT_DIR
from paper_analysis_dataset.tools.backfill_pending_augmented_abstract_zh import (
    extract_augment_batch_tag,
)


BENCHMARK_ROOT = DATASET_ROOT_DIR / "data" / "benchmarks" / "paper-filter"
DEFAULT_WORKERS = 5
DEFAULT_CHECKPOINT_EVERY = 5
DEFAULT_BACKUP_DIR = DATASET_ROOT_DIR / "data" / "benchmarks"


class TitleTranslator(Protocol):
    def submit_translate(self, candidate: CandidatePaper) -> Future[str]: ...


def backfill_pending_augmented_title_zh(
    *,
    benchmark_root: Path = BENCHMARK_ROOT,
    limit: int | None = None,
    workers: int = DEFAULT_WORKERS,
    checkpoint_every: int = DEFAULT_CHECKPOINT_EVERY,
    batch_tags: tuple[str, ...] = (),
    translator: TitleTranslator | None = None,
) -> dict[str, object]:
    repository = AnnotationRepository(benchmark_root)
    runtime_translator = translator or DoubaoTitleTranslator(concurrency=workers)
    records = repository.load_records()
    pending_ids = _load_pending_review_ids(repository)
    normalized_batch_tags = tuple(_normalize_batch_tag(tag) for tag in batch_tags if tag.strip())
    target_indexes = [
        index
        for index, record in enumerate(records)
        if _is_target_record(
            record,
            pending_ids=pending_ids,
            batch_tags=normalized_batch_tags,
        )
    ]
    if limit is not None:
        target_indexes = target_indexes[:limit]
    checkpoint_every = max(1, checkpoint_every)
    backup_path = _backup_records_path(repository.records_path)
    print(
        f"[backfill-pending-augment-title] start total={len(target_indexes)} "
        f"workers={workers} checkpoint_every={checkpoint_every}"
    )

    if not target_indexes:
        return {
            "benchmark_root": str(benchmark_root),
            "updated_records": 0,
            "remaining_records": 0,
            "target_records": 0,
            "workers": workers,
            "checkpoint_every": checkpoint_every,
            "backup_path": str(backup_path),
            "batch_tags": list(normalized_batch_tags),
        }

    updated_records = list(records)
    translated_count = 0
    pending_futures: dict[Future[BenchmarkRecord], int] = {}
    pending_iter = iter(target_indexes)

    for _ in range(min(workers, len(target_indexes))):
        index = next(pending_iter, None)
        if index is None:
            break
        future = _submit_translate_record(records[index], runtime_translator)
        pending_futures[future] = index

    while pending_futures:
        done, _ = wait(pending_futures.keys(), return_when=FIRST_COMPLETED)
        for future in done:
            index = pending_futures.pop(future)
            translated_record = future.result()
            updated_records[index] = translated_record
            translated_count += 1
            print(
                f"[backfill-pending-augment-title] {translated_count}/{len(target_indexes)} "
                f"paper_id={translated_record.paper_id}"
            )
            if translated_count % checkpoint_every == 0:
                repository.write_records(updated_records)
                print(
                    f"[backfill-pending-augment-title] checkpoint "
                    f"{translated_count}/{len(target_indexes)}"
                )
            next_index = next(pending_iter, None)
            if next_index is not None:
                next_future = _submit_translate_record(records[next_index], runtime_translator)
                pending_futures[next_future] = next_index

    repository.write_records(updated_records)
    remaining_records = sum(
        1
        for record in updated_records
        if _is_target_record(
            record,
            pending_ids=pending_ids,
            batch_tags=normalized_batch_tags,
        )
    )
    summary = {
        "benchmark_root": str(benchmark_root),
        "updated_records": translated_count,
        "remaining_records": remaining_records,
        "target_records": len(target_indexes),
        "workers": workers,
        "checkpoint_every": checkpoint_every,
        "backup_path": str(backup_path),
        "batch_tags": list(normalized_batch_tags),
    }
    print(
        f"[backfill-pending-augment-title] done updated={translated_count} "
        f"remaining={remaining_records}"
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="为待人工标注的增样记录回填中文标题")
    parser.add_argument("--limit", type=int, default=None, help="本次最多回填多少条记录")
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"并发 worker 数，默认 {DEFAULT_WORKERS}",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=DEFAULT_CHECKPOINT_EVERY,
        help=f"每成功多少条落盘一次，默认 {DEFAULT_CHECKPOINT_EVERY}",
    )
    parser.add_argument(
        "--batch-tag",
        action="append",
        default=[],
        help="只处理指定 augment_batch，支持重复传入，例如 20260408-scheduling",
    )
    args = parser.parse_args()
    summary = backfill_pending_augmented_title_zh(
        limit=args.limit,
        workers=args.workers,
        checkpoint_every=args.checkpoint_every,
        batch_tags=tuple(args.batch_tag),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def _load_pending_review_ids(repository: AnnotationRepository) -> set[str]:
    human_ids = {
        annotation.paper_id
        for annotation in repository.load_annotations(repository.annotations_human_path)
    }
    merged_ids = {
        annotation.paper_id
        for annotation in repository.load_annotations(repository.merged_path)
    }
    return {
        record.paper_id
        for record in repository.load_records()
        if record.paper_id not in human_ids and record.paper_id not in merged_ids
    }


def _is_target_record(
    record: BenchmarkRecord,
    *,
    pending_ids: set[str],
    batch_tags: tuple[str, ...],
) -> bool:
    if record.paper_id not in pending_ids:
        return False
    if not record.title.strip():
        return False
    if record.title_zh.strip():
        return False
    record_batch_tag = extract_augment_batch_tag(record.notes)
    if record_batch_tag is None:
        return False
    if batch_tags and record_batch_tag not in batch_tags:
        return False
    return True


def _normalize_batch_tag(value: str) -> str:
    stripped = value.strip()
    if stripped.startswith("augment_batch="):
        return stripped.split("=", 1)[1].strip()
    return stripped


def _submit_translate_record(
    record: BenchmarkRecord,
    translator: TitleTranslator,
) -> Future[BenchmarkRecord]:
    outer_future: Future[BenchmarkRecord] = Future()
    inner_future = translator.submit_translate(record.to_candidate_paper())
    inner_future.add_done_callback(lambda done: _resolve_translated_record(record, done, outer_future))
    return outer_future


def _resolve_translated_record(
    record: BenchmarkRecord,
    inner_future: Future[str],
    outer_future: Future[BenchmarkRecord],
) -> None:
    if outer_future.done():
        return
    try:
        outer_future.set_result(_build_translated_record(record, inner_future.result()))
    except Exception as exc:
        outer_future.set_exception(exc)


def _build_translated_record(record: BenchmarkRecord, title_zh: str) -> BenchmarkRecord:
    return BenchmarkRecord(
        paper_id=record.paper_id,
        title=record.title,
        title_zh=title_zh,
        abstract=record.abstract,
        abstract_zh=record.abstract_zh,
        authors=record.authors,
        venue=record.venue,
        year=record.year,
        source=record.source,
        source_path=record.source_path,
        primary_research_object=record.primary_research_object,
        candidate_preference_labels=record.candidate_preference_labels,
        candidate_negative_tier=record.candidate_negative_tier,
        keywords=record.keywords,
        notes=record.notes,
        final_primary_research_object=record.final_primary_research_object,
        final_preference_labels=record.final_preference_labels,
        final_negative_tier=record.final_negative_tier,
        final_labeler_ids=record.final_labeler_ids,
        final_review_status=record.final_review_status,
        final_evidence_spans=record.final_evidence_spans,
    )


def _backup_records_path(records_path: Path) -> Path:
    backup_root = DEFAULT_BACKUP_DIR / f"paper-filter-backup-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    backup_root.mkdir(parents=True, exist_ok=True)
    backup_path = backup_root / records_path.name
    shutil.copy2(records_path, backup_path)
    return backup_path


if __name__ == "__main__":
    main()
