from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from paper_analysis_dataset.domain.benchmark import (
    AnnotationRecord,
    BenchmarkRecord,
    ConflictRecord,
    NEGATIVE_TIERS,
    PREFERENCE_LABELS,
    RESEARCH_OBJECT_LABELS,
    REVIEW_STATUSES,
)
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.paper_filter_schema import build_schema_payload
from paper_analysis_dataset.shared.paths import DATA_BENCHMARKS_DIR


DEFAULT_BENCHMARK_ROOT = DATA_BENCHMARKS_DIR / "paper-filter"
FORBIDDEN_TRANSLATION_FRAGMENTS = (
    "请提供完整英文摘要内容",
    "无法进行忠实翻译",
    "你当前只给出了标题",
    "未提供摘要正文",
)
MIN_ABSTRACT_LENGTH_FOR_TRANSLATION_QUALITY_CHECK = 400
MIN_TRANSLATION_LENGTH_FOR_LONG_ABSTRACT = 120
MIN_TRANSLATION_TO_ABSTRACT_RATIO = 0.12
CONFLICT_FIELDS = (
    "paper_id",
    "conflicting_fields",
    "codex_annotation",
    "human_annotation",
    "resolved_annotation",
)


@dataclass(slots=True)
class ValidationIssue:
    path: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return {
            "path": self.path,
            "message": self.message,
        }


def validate_benchmark_schema(
    benchmark_root: Path = DEFAULT_BENCHMARK_ROOT,
) -> dict[str, object]:
    repository = AnnotationRepository(benchmark_root)
    issues: list[ValidationIssue] = []
    file_counts: dict[str, int] = {}
    expected_schema = build_schema_payload()
    try:
        schema_payload = _load_json_file(repository.schema_path)
    except Exception as exc:
        schema_payload = {}
        issues.append(ValidationIssue(path=str(repository.schema_path), message=str(exc)))

    if schema_payload != expected_schema:
        issues.append(
            ValidationIssue(
                path=str(repository.schema_path),
                message="schema.json 与代码内建协议不一致",
            )
        )

    record_fields = _as_str_dict(schema_payload.get("record_fields"), issues, str(repository.schema_path))
    annotation_fields = _as_str_dict(
        schema_payload.get("annotation_fields"),
        issues,
        str(repository.schema_path),
    )

    file_counts["records"] = _validate_jsonl_file(
        repository.records_path,
        record_fields,
        BenchmarkRecord.from_dict,
        issues,
        dataset_kind="records",
    )
    for dataset_kind, path in (
        ("annotations_ai", repository.annotations_ai_path),
        ("annotations_human", repository.annotations_human_path),
        ("merged", repository.merged_path),
    ):
        file_counts[dataset_kind] = _validate_jsonl_file(
            path,
            annotation_fields,
            AnnotationRecord.from_dict,
            issues,
            dataset_kind=dataset_kind,
        )

    file_counts["conflicts"] = _validate_conflicts_file(repository.conflicts_path, issues)

    try:
        stats_payload = _load_json_file(repository.stats_path)
        if not isinstance(stats_payload, dict):
            raise ValueError("stats.json 顶层必须是对象")
        file_counts["stats_keys"] = len(stats_payload)
    except Exception as exc:
        issues.append(ValidationIssue(path=str(repository.stats_path), message=str(exc)))
        file_counts["stats_keys"] = 0

    return {
        "ok": not issues,
        "benchmark_root": str(benchmark_root),
        "checked_files": {
            "records": str(repository.records_path),
            "annotations_ai": str(repository.annotations_ai_path),
            "annotations_human": str(repository.annotations_human_path),
            "merged": str(repository.merged_path),
            "conflicts": str(repository.conflicts_path),
            "schema": str(repository.schema_path),
            "stats": str(repository.stats_path),
        },
        "file_counts": file_counts,
        "issues": [issue.to_dict() for issue in issues],
    }


def _validate_jsonl_file(
    path: Path,
    field_types: dict[str, str],
    loader: Any,
    issues: list[ValidationIssue],
    *,
    dataset_kind: str,
) -> int:
    if not path.exists():
        issues.append(ValidationIssue(path=str(path), message="文件不存在"))
        return 0
    count = 0
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        count += 1
        item_path = f"{path}:{line_number}"
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            issues.append(ValidationIssue(path=item_path, message=f"JSON 非法：{exc.msg}"))
            continue
        if not isinstance(payload, dict):
            issues.append(ValidationIssue(path=item_path, message="JSONL 记录必须是对象"))
            continue
        _validate_exact_fields(payload, field_types, issues, item_path)
        _validate_payload_types(payload, field_types, issues, item_path)
        if dataset_kind == "records":
            _validate_record_quality(payload, issues, item_path)
        try:
            loader(payload)
        except Exception as exc:
            issues.append(
                ValidationIssue(path=item_path, message=f"{dataset_kind} 语义校验失败：{exc}")
            )
    return count


def _validate_conflicts_file(path: Path, issues: list[ValidationIssue]) -> int:
    if not path.exists():
        issues.append(ValidationIssue(path=str(path), message="文件不存在"))
        return 0
    count = 0
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        count += 1
        item_path = f"{path}:{line_number}"
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            issues.append(ValidationIssue(path=item_path, message=f"JSON 非法：{exc.msg}"))
            continue
        if not isinstance(payload, dict):
            issues.append(ValidationIssue(path=item_path, message="conflict 记录必须是对象"))
            continue
        _validate_exact_fields(payload, {field: "object" for field in CONFLICT_FIELDS}, issues, item_path)
        try:
            ConflictRecord.from_dict(payload)
        except Exception as exc:
            issues.append(ValidationIssue(path=item_path, message=f"conflict 语义校验失败：{exc}"))
    return count


def _validate_exact_fields(
    payload: dict[str, object],
    field_types: dict[str, str],
    issues: list[ValidationIssue],
    item_path: str,
) -> None:
    actual_fields = set(payload)
    expected_fields = set(field_types)
    missing_fields = sorted(expected_fields - actual_fields)
    extra_fields = sorted(actual_fields - expected_fields)
    if missing_fields:
        issues.append(
            ValidationIssue(
                path=item_path,
                message=f"缺少字段：{', '.join(missing_fields)}",
            )
        )
    if extra_fields:
        issues.append(
            ValidationIssue(
                path=item_path,
                message=f"存在未声明字段：{', '.join(extra_fields)}",
            )
        )


def _validate_payload_types(
    payload: dict[str, object],
    field_types: dict[str, str],
    issues: list[ValidationIssue],
    item_path: str,
) -> None:
    for field_name, declared_type in field_types.items():
        if field_name not in payload:
            continue
        value = payload[field_name]
        field_path = f"{item_path}.{field_name}"
        if declared_type == "string":
            if not isinstance(value, str):
                issues.append(ValidationIssue(path=field_path, message="必须是字符串"))
        elif declared_type == "integer":
            if not isinstance(value, int) or isinstance(value, bool):
                issues.append(ValidationIssue(path=field_path, message="必须是整数"))
        elif declared_type == "string[]":
            if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
                issues.append(ValidationIssue(path=field_path, message="必须是字符串数组"))
        elif declared_type == "enum":
            _validate_enum_field(field_name, value, issues, field_path)
        elif declared_type == "enum[]":
            if not isinstance(value, list):
                issues.append(ValidationIssue(path=field_path, message="必须是枚举数组"))
                continue
            for index, item in enumerate(value):
                _validate_enum_field(
                    field_name,
                    item,
                    issues,
                    f"{field_path}[{index}]",
                )
        elif declared_type == "object":
            if field_name == "evidence_spans":
                _validate_evidence_spans(value, issues, field_path)
            elif not isinstance(value, dict):
                issues.append(ValidationIssue(path=field_path, message="必须是对象"))
        else:
            issues.append(
                ValidationIssue(
                    path=field_path,
                    message=f"未知 schema 类型：{declared_type}",
                )
            )


def _validate_record_quality(
    payload: dict[str, object],
    issues: list[ValidationIssue],
    item_path: str,
) -> None:
    abstract = str(payload.get("abstract", "")).strip()
    abstract_zh = str(payload.get("abstract_zh", "")).strip()
    if not abstract_zh:
        return
    for fragment in FORBIDDEN_TRANSLATION_FRAGMENTS:
        if fragment in abstract_zh:
            issues.append(
                ValidationIssue(
                    path=f"{item_path}.abstract_zh",
                    message=f"中文摘要包含异常拒答文本：{fragment}",
                )
            )
    if _looks_suspiciously_short_translation(abstract, abstract_zh):
        issues.append(
            ValidationIssue(
                path=f"{item_path}.abstract_zh",
                message=(
                    "中文摘要疑似截断或严重过短："
                    f"abstract_chars={len(abstract)}, abstract_zh_chars={len(abstract_zh)}"
                ),
            )
        )


def _validate_enum_field(
    field_name: str,
    value: object,
    issues: list[ValidationIssue],
    field_path: str,
) -> None:
    if not isinstance(value, str):
        issues.append(ValidationIssue(path=field_path, message="必须是字符串枚举值"))
        return
    allowed = _allowed_enum_values(field_name)
    if value not in allowed:
        issues.append(
            ValidationIssue(
                path=field_path,
                message=f"非法枚举值：{value}",
            )
        )


def _validate_evidence_spans(
    value: object,
    issues: list[ValidationIssue],
    field_path: str,
) -> None:
    if not isinstance(value, dict):
        issues.append(ValidationIssue(path=field_path, message="必须是对象"))
        return
    allowed_labels = {"general", "negative", *PREFERENCE_LABELS}
    for label, spans in value.items():
        if not isinstance(label, str):
            issues.append(ValidationIssue(path=field_path, message="evidence_spans 的 key 必须是字符串"))
            continue
        if label not in allowed_labels:
            issues.append(
                ValidationIssue(
                    path=f"{field_path}.{label}",
                    message=f"非法 evidence 标签：{label}",
                )
            )
        if not isinstance(spans, list) or any(not isinstance(item, str) for item in spans):
            issues.append(
                ValidationIssue(
                    path=f"{field_path}.{label}",
                    message="evidence_spans 的 value 必须是字符串数组",
                )
            )


def _looks_suspiciously_short_translation(abstract: str, abstract_zh: str) -> bool:
    if len(abstract) < MIN_ABSTRACT_LENGTH_FOR_TRANSLATION_QUALITY_CHECK:
        return False
    if len(abstract_zh) >= MIN_TRANSLATION_LENGTH_FOR_LONG_ABSTRACT:
        return False
    return (len(abstract_zh) / max(len(abstract), 1)) < MIN_TRANSLATION_TO_ABSTRACT_RATIO


def _allowed_enum_values(field_name: str) -> tuple[str, ...]:
    if field_name in {
        "primary_research_object",
        "final_primary_research_object",
    }:
        return RESEARCH_OBJECT_LABELS
    if field_name in {
        "candidate_preference_labels",
        "preference_labels",
        "final_preference_labels",
    }:
        return PREFERENCE_LABELS
    if field_name in {
        "candidate_negative_tier",
        "negative_tier",
        "final_negative_tier",
    }:
        return NEGATIVE_TIERS
    if field_name in {
        "review_status",
        "final_review_status",
    }:
        return REVIEW_STATUSES
    return ()


def _load_json_file(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("顶层必须是对象")
    return payload


def _as_str_dict(
    value: object,
    issues: list[ValidationIssue],
    path: str,
) -> dict[str, str]:
    if not isinstance(value, dict):
        issues.append(ValidationIssue(path=path, message="schema 字段映射必须是对象"))
        return {}
    result: dict[str, str] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not isinstance(item, str):
            issues.append(ValidationIssue(path=path, message="schema 字段映射必须是 string -> string"))
            continue
        result[key] = item
    return result
