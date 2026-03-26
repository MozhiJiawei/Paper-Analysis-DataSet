from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

from paper_analysis_dataset.domain.benchmark import AnnotationRecord, BenchmarkRecord, ConflictRecord
from paper_analysis_dataset.services.annotation_merge import merge_annotations
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.benchmark_reporter import build_distribution_report
from paper_analysis_dataset.tools.rebuild_paper_filter_benchmark import _build_schema_payload


LEGACY_NEGATIVE_TIER_MAP = {
    "easy": "negative",
    "in_domain": "negative",
    "hard": "negative",
}

REQUIRED_FILES = (
    "records.jsonl",
    "annotations-ai.jsonl",
    "annotations-human.jsonl",
    "merged.jsonl",
    "conflicts.jsonl",
    "schema.json",
    "stats.json",
)


def cleanup_legacy_benchmark_protocol(benchmark_root: Path | None = None) -> dict[str, object]:
    repository = AnnotationRepository(benchmark_root)
    _ensure_required_files(repository.root_dir)

    backup_path = _backup_benchmark_root(repository.root_dir)

    records = _load_clean_records(repository.records_path)
    ai_annotations = _load_clean_annotations(repository.annotations_ai_path)
    human_annotations = _load_clean_annotations(repository.annotations_human_path)
    conflicts = _load_clean_conflicts(repository.conflicts_path)

    paired_ids = {item.paper_id for item in ai_annotations} & {item.paper_id for item in human_annotations}
    arbitrations = [
        item.resolved_annotation
        for item in conflicts
        if item.resolved_annotation is not None and item.paper_id in paired_ids
    ]

    result = merge_annotations(
        [item for item in records if item.paper_id in paired_ids],
        [item for item in ai_annotations if item.paper_id in paired_ids],
        [item for item in human_annotations if item.paper_id in paired_ids],
        arbitrations,
    )

    merged_by_id = {item.paper_id: item for item in result.records}
    next_records = [merged_by_id.get(item.paper_id, item) for item in records]
    stats = build_distribution_report(next_records)
    schema = _build_schema_payload()

    repository.write_records(next_records)
    repository.write_annotations(ai_annotations, repository.annotations_ai_path)
    repository.write_annotations(human_annotations, repository.annotations_human_path)
    repository.write_annotations(result.merged_annotations, repository.merged_path)
    repository.write_conflicts(result.conflicts, repository.conflicts_path)
    repository.write_json(schema, repository.schema_path)
    repository.write_json(stats, repository.stats_path)

    return {
        "benchmark_root": str(repository.root_dir),
        "backup_path": str(backup_path),
        "total_records": len(next_records),
        "annotations_ai": len(ai_annotations),
        "annotations_human": len(human_annotations),
        "merged": len(result.merged_annotations),
        "conflicts": len(result.conflicts),
    }


def _ensure_required_files(root_dir: Path) -> None:
    missing = [name for name in REQUIRED_FILES if not (root_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"benchmark 目录缺少核心文件：{', '.join(missing)}")


def _backup_benchmark_root(root_dir: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_parent = root_dir.parent / "paper-filter-backups"
    backup_path = backup_parent / f"{root_dir.name}-{timestamp}"
    backup_parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(root_dir, backup_path)
    return backup_path


def _read_jsonl_dicts(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"JSONL 记录必须是对象：{path}")
        rows.append(payload)
    return rows


def _normalize_negative_tier(value: object) -> str:
    normalized = str(value).strip()
    if normalized in LEGACY_NEGATIVE_TIER_MAP:
        return LEGACY_NEGATIVE_TIER_MAP[normalized]
    if normalized in {"positive", "negative"}:
        return normalized
    raise ValueError(f"negative_tier 非法：{value}")


def _strip_legacy_keys(payload: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(payload)
    cleaned.pop("target_preference_labels", None)
    cleaned.pop("final_target_preference_labels", None)
    return cleaned


def _clean_annotation_payload(payload: dict[str, Any]) -> dict[str, Any]:
    cleaned = _strip_legacy_keys(payload)
    cleaned["negative_tier"] = _normalize_negative_tier(cleaned.get("negative_tier", "negative"))
    if cleaned["negative_tier"] == "negative":
        cleaned["preference_labels"] = []
    return cleaned


def _clean_record_payload(payload: dict[str, Any]) -> dict[str, Any]:
    cleaned = _strip_legacy_keys(payload)
    cleaned["candidate_negative_tier"] = _normalize_negative_tier(
        cleaned.get("candidate_negative_tier", "negative")
    )
    if cleaned["candidate_negative_tier"] == "negative":
        cleaned["candidate_preference_labels"] = []
    final_negative_tier = str(cleaned.get("final_negative_tier", "")).strip()
    if final_negative_tier:
        cleaned["final_negative_tier"] = _normalize_negative_tier(final_negative_tier)
        if cleaned["final_negative_tier"] == "negative":
            cleaned["final_preference_labels"] = []
    return cleaned


def _load_clean_records(path: Path) -> list[BenchmarkRecord]:
    return [BenchmarkRecord.from_dict(_clean_record_payload(row)) for row in _read_jsonl_dicts(path)]


def _load_clean_annotations(path: Path) -> list[AnnotationRecord]:
    return [AnnotationRecord.from_dict(_clean_annotation_payload(row)) for row in _read_jsonl_dicts(path)]


def _load_clean_conflicts(path: Path) -> list[ConflictRecord]:
    rows = _read_jsonl_dicts(path)
    cleaned_rows: list[dict[str, Any]] = []
    for row in rows:
        cleaned = dict(row)
        cleaned["codex_annotation"] = _clean_annotation_payload(dict(cleaned.get("codex_annotation", {})))
        cleaned["human_annotation"] = _clean_annotation_payload(dict(cleaned.get("human_annotation", {})))
        resolved = cleaned.get("resolved_annotation")
        if isinstance(resolved, dict):
            cleaned["resolved_annotation"] = _clean_annotation_payload(dict(resolved))
        cleaned_rows.append(cleaned)
    return [ConflictRecord.from_dict(row) for row in cleaned_rows]


def main() -> None:
    summary = cleanup_legacy_benchmark_protocol()
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
