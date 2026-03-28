from __future__ import annotations

from collections import Counter
import json
from pathlib import Path

from paper_analysis_dataset.domain.benchmark import (
    AnnotationRecord,
    PREFERENCE_LABELS,
    RESEARCH_OBJECT_LABELS,
)


NEGATIVE_CLASS = "__negative__"


def build_evaluation_report(
    *,
    truths: list[AnnotationRecord],
    predictions: list[AnnotationRecord],
    request_error_count: int,
    protocol_error_count: int,
) -> dict[str, object]:
    counts = {
        "total_count": len(truths) + request_error_count + protocol_error_count,
        "evaluated_count": len(truths),
        "request_error_count": request_error_count,
        "protocol_error_count": protocol_error_count,
    }
    if not truths:
        return {
            "counts": counts,
            "overall": {
                "accuracy": 0.0,
                "macro_precision": 0.0,
                "macro_recall": 0.0,
                "macro_f1": 0.0,
                "micro_precision": 0.0,
                "micro_recall": 0.0,
                "micro_f1": 0.0,
            },
            "positive_negative": {},
            "by_preference_label": {},
            "by_primary_research_object": {},
        }

    label_classes = list(PREFERENCE_LABELS) + [NEGATIVE_CLASS]
    truth_classes = [_label_class(item) for item in truths]
    predicted_classes = [_label_class(item) for item in predictions]
    exact_matches = [
        _is_exact_match(truth, predicted)
        for truth, predicted in zip(truths, predictions, strict=True)
    ]

    label_metrics = {
        label: _binary_metrics(truth_classes, predicted_classes, positive_label=label)
        for label in label_classes
    }
    macro_precision = sum(item["precision"] for item in label_metrics.values()) / len(label_metrics)
    macro_recall = sum(item["recall"] for item in label_metrics.values()) / len(label_metrics)
    macro_f1 = sum(item["f1"] for item in label_metrics.values()) / len(label_metrics)
    micro = _micro_metrics(truth_classes, predicted_classes, positive_labels=label_classes)
    by_object: dict[str, dict[str, object]] = {}
    for research_object in RESEARCH_OBJECT_LABELS:
        indices = [
            index
            for index, truth in enumerate(truths)
            if truth.primary_research_object == research_object
        ]
        if not indices:
            continue
        accuracy = sum(1 for index in indices if exact_matches[index]) / len(indices)
        by_object[research_object] = {
            "accuracy": round(accuracy, 4),
            "support": len(indices),
        }

    positive_negative = _binary_metrics(
        [truth.negative_tier for truth in truths],
        [prediction.negative_tier for prediction in predictions],
        positive_label="positive",
    )

    return {
        "counts": counts,
        "overall": {
            "accuracy": round(sum(1 for item in exact_matches if item) / len(exact_matches), 4),
            "macro_precision": round(macro_precision, 4),
            "macro_recall": round(macro_recall, 4),
            "macro_f1": round(macro_f1, 4),
            "micro_precision": micro["precision"],
            "micro_recall": micro["recall"],
            "micro_f1": micro["f1"],
        },
        "positive_negative": positive_negative,
        "by_preference_label": {
            ("negative" if label == NEGATIVE_CLASS else label): metrics
            for label, metrics in label_metrics.items()
        },
        "by_primary_research_object": by_object,
    }


def write_evaluation_artifacts(output_dir: Path, report: dict[str, object]) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "report.json"
    summary_path = output_dir / "summary.md"
    stdout_path = output_dir / "stdout.txt"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    summary_path.write_text(_build_summary_markdown(report), encoding="utf-8")
    stdout_path.write_text(_build_stdout(report), encoding="utf-8")
    return {"report": report_path, "summary": summary_path, "stdout": stdout_path}


def _build_summary_markdown(report: dict[str, object]) -> str:
    counts = dict(report["counts"])
    overall = dict(report["overall"])
    lines = [
        "# 评测报告",
        "",
        f"- 有效评测样本数：{counts['evaluated_count']}",
        f"- 请求失败数：{counts['request_error_count']}",
        f"- 协议错误数：{counts['protocol_error_count']}",
        f"- overall accuracy：{overall['accuracy']}",
        f"- macro precision / recall / f1：{overall['macro_precision']} / {overall['macro_recall']} / {overall['macro_f1']}",
        f"- micro precision / recall / f1：{overall['micro_precision']} / {overall['micro_recall']} / {overall['micro_f1']}",
        "",
        "## 偏好标签指标",
        "",
    ]
    for label, metrics in dict(report["by_preference_label"]).items():
        lines.append(
            f"- {label}: precision={metrics['precision']}, recall={metrics['recall']}, f1={metrics['f1']}, support={metrics['support']}"
        )
    lines.extend(["", "## 主研究对象准确率", ""])
    for research_object, metrics in dict(report["by_primary_research_object"]).items():
        lines.append(
            f"- {research_object}: accuracy={metrics['accuracy']}, support={metrics['support']}"
        )
    return "\n".join(lines) + "\n"


def _build_stdout(report: dict[str, object]) -> str:
    counts = dict(report["counts"])
    overall = dict(report["overall"])
    return (
        "[OK] 评测完成\n"
        f"evaluated={counts['evaluated_count']}\n"
        f"request_errors={counts['request_error_count']}\n"
        f"protocol_errors={counts['protocol_error_count']}\n"
        f"accuracy={overall['accuracy']}\n"
    )


def _label_class(annotation: AnnotationRecord) -> str:
    if annotation.negative_tier == "negative":
        return NEGATIVE_CLASS
    if annotation.preference_labels:
        return annotation.preference_labels[0]
    return NEGATIVE_CLASS


def _is_exact_match(truth: AnnotationRecord, predicted: AnnotationRecord) -> bool:
    return (
        truth.primary_research_object == predicted.primary_research_object
        and truth.negative_tier == predicted.negative_tier
        and truth.preference_labels == predicted.preference_labels
    )


def _binary_metrics(
    truths: list[str],
    predictions: list[str],
    *,
    positive_label: str,
) -> dict[str, object]:
    tp = sum(
        1
        for truth, prediction in zip(truths, predictions, strict=True)
        if truth == positive_label and prediction == positive_label
    )
    fp = sum(
        1
        for truth, prediction in zip(truths, predictions, strict=True)
        if truth != positive_label and prediction == positive_label
    )
    fn = sum(
        1
        for truth, prediction in zip(truths, predictions, strict=True)
        if truth == positive_label and prediction != positive_label
    )
    support = sum(1 for truth in truths if truth == positive_label)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "support": support,
    }


def _micro_metrics(
    truths: list[str],
    predictions: list[str],
    *,
    positive_labels: list[str],
) -> dict[str, float]:
    supported_labels = set(positive_labels)
    correct = sum(
        1
        for truth, prediction in zip(truths, predictions, strict=True)
        if truth == prediction and truth in supported_labels
    )
    total = sum(1 for truth in truths if truth in supported_labels)
    precision = correct / total if total else 0.0
    recall = correct / total if total else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
    }
