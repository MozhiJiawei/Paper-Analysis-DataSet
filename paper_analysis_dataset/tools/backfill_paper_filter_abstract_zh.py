from __future__ import annotations

import argparse
import json
from concurrent.futures import FIRST_COMPLETED, Future, wait

from paper_analysis_dataset.domain.benchmark import BenchmarkRecord
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.doubao_abstract_translator import DoubaoAbstractTranslator
from paper_analysis_dataset.shared.paths import DATASET_ROOT_DIR


BENCHMARK_ROOT = DATASET_ROOT_DIR / "data" / "benchmarks" / "paper-filter"
DEFAULT_WORKERS = 5
DEFAULT_CHECKPOINT_EVERY = 5


def backfill_abstract_zh(
    *,
    limit: int | None = None,
    workers: int = DEFAULT_WORKERS,
    checkpoint_every: int = DEFAULT_CHECKPOINT_EVERY,
) -> dict[str, object]:
    repository = AnnotationRepository(BENCHMARK_ROOT)
    translator = DoubaoAbstractTranslator(concurrency=workers)
    records = repository.load_records()
    pending_indexes = [index for index, record in enumerate(records) if _needs_backfill(record)]
    if limit is not None:
        pending_indexes = pending_indexes[:limit]
    checkpoint_every = max(1, checkpoint_every)
    print(
        f"[backfill] start total={len(pending_indexes)} workers={workers} checkpoint_every={checkpoint_every}"
    )

    if not pending_indexes:
        return {
            "benchmark_root": str(BENCHMARK_ROOT),
            "total_records": len(records),
            "updated_records": 0,
            "remaining_records": 0,
            "workers": workers,
            "checkpoint_every": checkpoint_every,
        }

    updated_records = list(records)
    translated_count = 0
    total_pending = len(pending_indexes)

    pending_futures: dict[Future[BenchmarkRecord], int] = {}
    pending_iter = iter(pending_indexes)

    for _ in range(min(workers, len(pending_indexes))):
        index = next(pending_iter, None)
        if index is None:
            break
        future = _submit_translate_record(records[index], translator)
        pending_futures[future] = index

    while pending_futures:
        done, _ = wait(pending_futures.keys(), return_when=FIRST_COMPLETED)
        for future in done:
            index = pending_futures.pop(future)
            translated_record = future.result()
            updated_records[index] = translated_record
            translated_count += 1
            print(f"[backfill] {translated_count}/{total_pending} paper_id={translated_record.paper_id}")

            if translated_count % checkpoint_every == 0:
                repository.write_records(updated_records)
                print(f"[backfill] checkpoint {translated_count}/{total_pending}")

            next_index = next(pending_iter, None)
            if next_index is not None:
                next_future = _submit_translate_record(records[next_index], translator)
                pending_futures[next_future] = next_index

    repository.write_records(updated_records)
    remaining_records = sum(1 for record in updated_records if _needs_backfill(record))
    summary = {
        "benchmark_root": str(BENCHMARK_ROOT),
        "total_records": len(records),
        "updated_records": translated_count,
        "remaining_records": remaining_records,
        "workers": workers,
        "checkpoint_every": checkpoint_every,
    }
    print(f"[backfill] done updated={translated_count} remaining={remaining_records}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="为 paper-filter records.jsonl 回填中文摘要")
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
    args = parser.parse_args()
    summary = backfill_abstract_zh(
        limit=args.limit,
        workers=args.workers,
        checkpoint_every=args.checkpoint_every,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def _needs_backfill(record: BenchmarkRecord) -> bool:
    if not record.abstract.strip():
        return False
    if not record.abstract_zh.strip():
        return True
    # 覆盖测试或占位流程写入的伪中文摘要。
    return record.abstract_zh.strip() == f"中文摘要：{record.title}"


def _submit_translate_record(
    record: BenchmarkRecord,
    translator: DoubaoAbstractTranslator,
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


def _build_translated_record(record: BenchmarkRecord, abstract_zh: str) -> BenchmarkRecord:
    return BenchmarkRecord(
        paper_id=record.paper_id,
        title=record.title,
        title_zh=record.title_zh,
        abstract=record.abstract,
        abstract_zh=abstract_zh,
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


if __name__ == "__main__":
    main()
