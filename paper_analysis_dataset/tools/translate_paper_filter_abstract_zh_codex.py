from __future__ import annotations

import argparse
import json
from concurrent.futures import FIRST_COMPLETED, Future, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import shutil

from paper_analysis_dataset.domain.benchmark import BenchmarkRecord
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.codex_abstract_translator import CodexAbstractTranslator
from paper_analysis_dataset.shared.clients.codex_cli_client import DEFAULT_CODEX_CLI_MODEL
from paper_analysis_dataset.shared.paths import ARTIFACTS_DIR, DATASET_ROOT_DIR


BENCHMARK_ROOT = DATASET_ROOT_DIR / "data" / "benchmarks" / "paper-filter"
DEFAULT_OUTPUT_PATH = (
    ARTIFACTS_DIR / "translations" / "paper-filter" / "abstract-zh-codex.jsonl"
)
DEFAULT_BACKUP_DIR = ARTIFACTS_DIR / "backups" / "paper-filter"
DEFAULT_WORKERS = 5
DEFAULT_CHECKPOINT_EVERY = 5


@dataclass(slots=True)
class TranslationPatch:
    paper_id: str
    title: str
    abstract_zh: str
    model: str
    translated_at: str

    def to_dict(self) -> dict[str, str]:
        return {
            "paper_id": self.paper_id,
            "title": self.title,
            "abstract_zh": self.abstract_zh,
            "model": self.model,
            "translated_at": self.translated_at,
        }


def export_codex_abstract_translations(
    *,
    limit: int | None = None,
    workers: int = DEFAULT_WORKERS,
    checkpoint_every: int = DEFAULT_CHECKPOINT_EVERY,
    benchmark_root: Path = BENCHMARK_ROOT,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    model: str | None = DEFAULT_CODEX_CLI_MODEL,
    apply_to_records: bool = False,
    backup_dir: Path = DEFAULT_BACKUP_DIR,
) -> dict[str, object]:
    repository = AnnotationRepository(benchmark_root)
    translator = CodexAbstractTranslator(model=model, concurrency=workers)
    records = repository.load_records()
    updated_records = list(records)
    existing_patches = _load_existing_patches(output_path)
    pending_indexes = [
        index
        for index, record in enumerate(records)
        if _needs_translation(record) and record.paper_id not in existing_patches
    ]
    if limit is not None:
        pending_indexes = pending_indexes[:limit]
    checkpoint_every = max(1, checkpoint_every)
    print(
        f"[translate-codex] start total={len(pending_indexes)} workers={workers} checkpoint_every={checkpoint_every}"
    )

    if not pending_indexes:
        return {
            "benchmark_root": str(benchmark_root),
            "output_path": str(output_path),
            "total_records": len(records),
            "exported_records": 0,
            "existing_patches": len(existing_patches),
            "remaining_records": sum(1 for record in records if _needs_translation(record)),
            "workers": workers,
            "checkpoint_every": checkpoint_every,
            "model": model or "",
            "apply_to_records": apply_to_records,
            "backup_path": "",
        }

    patches = dict(existing_patches)
    translated_count = 0
    total_pending = len(pending_indexes)
    backup_path = _backup_records_path(repository.records_path, backup_dir) if apply_to_records else None
    pending_futures: dict[Future[TranslationPatch], int] = {}
    pending_iter = iter(pending_indexes)

    for _ in range(min(workers, len(pending_indexes))):
        if (index := next(pending_iter, None)) is None:
            break
        future = _submit_translation(records[index], translator, model=model)
        pending_futures[future] = index

    while pending_futures:
        done, _ = wait(pending_futures.keys(), return_when=FIRST_COMPLETED)
        for future in done:
            index = pending_futures.pop(future)
            patch = future.result()
            patches[patch.paper_id] = patch
            updated_records[index] = _build_translated_record(records[index], patch.abstract_zh)
            translated_count += 1
            print(f"[translate-codex] {translated_count}/{total_pending} paper_id={patch.paper_id}")

            if translated_count % checkpoint_every == 0:
                _write_patches(output_path, patches)
                if apply_to_records:
                    repository.write_records(updated_records)
                print(f"[translate-codex] checkpoint {translated_count}/{total_pending}")

            if (next_index := next(pending_iter, None)) is not None:
                next_future = _submit_translation(records[next_index], translator, model=model)
                pending_futures[next_future] = next_index

    _write_patches(output_path, patches)
    if apply_to_records:
        repository.write_records(updated_records)
    remaining_records = sum(
        1 for record in updated_records if _needs_translation(record)
    )
    summary = {
        "benchmark_root": str(benchmark_root),
        "output_path": str(output_path),
        "total_records": len(records),
        "exported_records": translated_count,
        "existing_patches": len(existing_patches),
        "remaining_records": remaining_records,
        "workers": workers,
        "checkpoint_every": checkpoint_every,
        "model": model or "",
        "apply_to_records": apply_to_records,
        "backup_path": str(backup_path) if backup_path is not None else "",
    }
    print(f"[translate-codex] done exported={translated_count} remaining={remaining_records}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="用 Codex CLI 并发生成缺失中文摘要，默认只导出补丁文件，不修改现有数据集"
    )
    parser.add_argument("--limit", type=int, default=None, help="本次最多翻译多少条记录")
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
        help=f"每成功多少条写一次补丁文件，默认 {DEFAULT_CHECKPOINT_EVERY}",
    )
    parser.add_argument(
        "--benchmark-root",
        type=Path,
        default=BENCHMARK_ROOT,
        help="待读取的数据集目录，默认使用仓内 paper-filter benchmark",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="补丁 JSONL 输出路径，默认写到 artifacts 目录",
    )
    parser.add_argument(
        "--model",
        default=None,
        help=f"可选 Codex 模型名，默认 {DEFAULT_CODEX_CLI_MODEL}",
    )
    parser.add_argument(
        "--apply-to-records",
        action="store_true",
        help="将译文安全回填到 benchmark records.jsonl；启用后会先自动备份原文件",
    )
    args = parser.parse_args()
    summary = export_codex_abstract_translations(
        limit=args.limit,
        workers=args.workers,
        checkpoint_every=args.checkpoint_every,
        benchmark_root=args.benchmark_root,
        output_path=args.output_path,
        model=args.model,
        apply_to_records=args.apply_to_records,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def _needs_translation(record: BenchmarkRecord) -> bool:
    return bool(record.abstract.strip()) and not record.abstract_zh.strip()


def _load_existing_patches(path: Path) -> dict[str, TranslationPatch]:
    if not path.exists():
        return {}
    patches: dict[str, TranslationPatch] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"补丁文件记录必须是对象：{path}")
        patch = TranslationPatch(
            paper_id=str(payload.get("paper_id", "")).strip(),
            title=str(payload.get("title", "")).strip(),
            abstract_zh=str(payload.get("abstract_zh", "")).strip(),
            model=str(payload.get("model", "")).strip(),
            translated_at=str(payload.get("translated_at", "")).strip(),
        )
        if not patch.paper_id or not patch.abstract_zh:
            raise ValueError(f"补丁文件记录缺少必要字段：{path}")
        patches[patch.paper_id] = patch
    return patches


def _write_patches(path: Path, patches: dict[str, TranslationPatch]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        json.dumps(patch.to_dict(), ensure_ascii=False)
        for patch in sorted(patches.values(), key=lambda item: item.paper_id)
    ]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def _backup_records_path(records_path: Path, backup_dir: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    for _ in range(10):
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        backup_path = backup_dir / f"{records_path.stem}-{timestamp}{records_path.suffix}"
        if backup_path.exists():
            continue
        shutil.copy2(records_path, backup_path)
        return backup_path
    raise RuntimeError(f"无法创建 records 备份：{records_path}")


def _submit_translation(
    record: BenchmarkRecord,
    translator: CodexAbstractTranslator,
    *,
    model: str | None,
) -> Future[TranslationPatch]:
    outer_future: Future[TranslationPatch] = Future()
    inner_future = translator.submit_translate(record.to_candidate_paper())
    inner_future.add_done_callback(
        lambda done: _resolve_translation(record, done, outer_future, model=model)
    )
    return outer_future


def _resolve_translation(
    record: BenchmarkRecord,
    inner_future: Future[str],
    outer_future: Future[TranslationPatch],
    *,
    model: str | None,
) -> None:
    if outer_future.done():
        return
    try:
        translated_at = datetime.now(timezone.utc).isoformat()
        outer_future.set_result(
            TranslationPatch(
                paper_id=record.paper_id,
                title=record.title,
                abstract_zh=inner_future.result(),
                model=model or "",
                translated_at=translated_at,
            )
        )
    except Exception as exc:
        outer_future.set_exception(exc)


def _build_translated_record(record: BenchmarkRecord, abstract_zh: str) -> BenchmarkRecord:
    return BenchmarkRecord(
        paper_id=record.paper_id,
        title=record.title,
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
