from __future__ import annotations

import json
from dataclasses import dataclass
from json import JSONDecodeError
from pathlib import Path
from typing import Any

from paper_analysis_dataset.domain.benchmark import AnnotationRecord, BenchmarkRecord
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository


class BenchmarkImportError(ValueError):
    """Raised when a dataset-native import payload cannot be imported."""


@dataclass(slots=True)
class BenchmarkImportSummary:
    dry_run: bool
    records_added: int
    records_updated: int
    records_skipped_existing: int
    ai_annotations_added: int
    ai_annotations_updated: int
    ai_annotations_unchanged: int
    ai_positive_count: int
    ai_negative_count: int
    records_path: Path
    annotations_ai_path: Path

    def to_dict(self) -> dict[str, object]:
        return {
            "ok": True,
            "dry_run": self.dry_run,
            "records_added": self.records_added,
            "records_updated": self.records_updated,
            "records_skipped_existing": self.records_skipped_existing,
            "ai_annotations_added": self.ai_annotations_added,
            "ai_annotations_updated": self.ai_annotations_updated,
            "ai_annotations_unchanged": self.ai_annotations_unchanged,
            "ai_positive_count": self.ai_positive_count,
            "ai_negative_count": self.ai_negative_count,
            "records_path": str(self.records_path),
            "annotations_ai_path": str(self.annotations_ai_path),
        }


@dataclass(slots=True)
class BenchmarkImportPayload:
    records: list[BenchmarkRecord]
    annotations_ai: list[AnnotationRecord]
    source_batch: str = ""


def load_import_payload(path: Path) -> BenchmarkImportPayload:
    try:
        raw_payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise BenchmarkImportError(f"导入文件不存在：{path}") from exc
    except JSONDecodeError as exc:
        raise BenchmarkImportError(f"导入文件不是合法 JSON：{path}: {exc.msg}") from exc
    if not isinstance(raw_payload, dict):
        raise BenchmarkImportError("导入 payload 必须是 JSON 对象")
    return parse_import_payload(raw_payload)


def parse_import_payload(payload: dict[str, Any]) -> BenchmarkImportPayload:
    raw_records = payload.get("records")
    if not isinstance(raw_records, list):
        raise BenchmarkImportError("导入 payload 缺少 records 数组")

    raw_annotations_ai = payload.get("annotations_ai", [])
    if not isinstance(raw_annotations_ai, list):
        raise BenchmarkImportError("annotations_ai 必须是数组")

    records = [_build_record(item, index) for index, item in enumerate(raw_records)]
    annotations_ai = [
        _build_annotation(item, index) for index, item in enumerate(raw_annotations_ai)
    ]
    _reject_duplicate_paper_ids("records", [record.paper_id for record in records])
    _reject_duplicate_paper_ids(
        "annotations_ai",
        [annotation.paper_id for annotation in annotations_ai],
    )
    record_ids = {record.paper_id for record in records}
    missing_record_ids = sorted(
        annotation.paper_id for annotation in annotations_ai if annotation.paper_id not in record_ids
    )
    if missing_record_ids:
        raise BenchmarkImportError(
            "annotations_ai 引用了本次导入 records 中不存在的 paper_id："
            + ", ".join(missing_record_ids)
        )
    return BenchmarkImportPayload(
        records=records,
        annotations_ai=annotations_ai,
        source_batch=str(payload.get("source_batch", "")).strip(),
    )


def import_benchmark_payload(
    payload: BenchmarkImportPayload,
    *,
    repository: AnnotationRepository,
    dry_run: bool = False,
) -> BenchmarkImportSummary:
    existing_records = repository.load_record_map()
    existing_ai_annotations = {
        annotation.paper_id: annotation
        for annotation in repository.load_annotations(repository.annotations_ai_path)
    }

    new_records = []
    updated_record_ids: set[str] = set()
    next_records_by_id = dict(existing_records)
    for record in payload.records:
        existing_record = existing_records.get(record.paper_id)
        if existing_record is None:
            new_records.append(record)
            next_records_by_id[record.paper_id] = record
            continue
        if existing_record.to_dict(include_final_annotations=False) != record.to_dict(
            include_final_annotations=False
        ):
            updated_record_ids.add(record.paper_id)
            next_records_by_id[record.paper_id] = record
    merged_records = list(next_records_by_id.values())

    ai_added = 0
    ai_updated = 0
    ai_unchanged = 0
    for annotation in payload.annotations_ai:
        existing_annotation = existing_ai_annotations.get(annotation.paper_id)
        if existing_annotation is None:
            ai_added += 1
            continue
        if existing_annotation.to_dict() == annotation.to_dict():
            ai_unchanged += 1
        else:
            ai_updated += 1

    summary = BenchmarkImportSummary(
        dry_run=dry_run,
        records_added=len(new_records),
        records_updated=len(updated_record_ids),
        records_skipped_existing=len(payload.records) - len(new_records),
        ai_annotations_added=ai_added,
        ai_annotations_updated=ai_updated,
        ai_annotations_unchanged=ai_unchanged,
        ai_positive_count=sum(
            1 for annotation in payload.annotations_ai if annotation.negative_tier == "positive"
        ),
        ai_negative_count=sum(
            1 for annotation in payload.annotations_ai if annotation.negative_tier == "negative"
        ),
        records_path=repository.records_path,
        annotations_ai_path=repository.annotations_ai_path,
    )
    if dry_run:
        return summary

    if new_records or updated_record_ids:
        repository.write_records(merged_records)
    if payload.annotations_ai:
        repository.upsert_annotations(payload.annotations_ai, repository.annotations_ai_path)
    return summary


def import_benchmark_json(
    input_json: Path,
    *,
    benchmark_root: Path | None = None,
    dry_run: bool = False,
) -> BenchmarkImportSummary:
    payload = load_import_payload(input_json)
    repository = AnnotationRepository(benchmark_root)
    return import_benchmark_payload(payload, repository=repository, dry_run=dry_run)


def _build_record(item: object, index: int) -> BenchmarkRecord:
    if not isinstance(item, dict):
        raise BenchmarkImportError(f"records[{index}] 必须是对象")
    try:
        return BenchmarkRecord.from_dict(item)
    except (TypeError, ValueError) as exc:
        raise BenchmarkImportError(f"records[{index}] 非法：{exc}") from exc


def _build_annotation(item: object, index: int) -> AnnotationRecord:
    if not isinstance(item, dict):
        raise BenchmarkImportError(f"annotations_ai[{index}] 必须是对象")
    try:
        return AnnotationRecord.from_dict(item)
    except (TypeError, ValueError) as exc:
        raise BenchmarkImportError(f"annotations_ai[{index}] 非法：{exc}") from exc


def _reject_duplicate_paper_ids(field_name: str, paper_ids: list[str]) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for paper_id in paper_ids:
        if paper_id in seen:
            duplicates.add(paper_id)
        seen.add(paper_id)
    if duplicates:
        raise BenchmarkImportError(
            f"{field_name} 包含重复 paper_id：" + ", ".join(sorted(duplicates))
        )
