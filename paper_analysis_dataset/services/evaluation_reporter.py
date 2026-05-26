from __future__ import annotations

import json
from pathlib import Path

from paper_analysis_dataset.domain.benchmark import (
    AnnotationRecord,
    PREFERENCE_LABELS,
    RESEARCH_OBJECT_LABELS,
)


NEGATIVE_CLASS = "__negative__"
AGGREGATED_RESEARCH_OBJECT_LABELS = ("LLM", "VLM", "Diffusion", "其他")


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
    metric_scopes = {
        "overall": "全量样本上的正负样本识别汇总指标。",
        "positive_negative": "全量样本上的 positive vs negative 二分类指标。",
        "by_preference_label": "仅统计 negative_tier=positive 的样本；负样本未标注研究子类，不纳入该维度。",
        "by_primary_research_object": "仅统计 negative_tier=positive 的样本；并将协议标签聚合为 LLM/VLM/Diffusion/其他 四桶后评测。",
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
            "positive_preference_label_overall": {},
            "by_preference_label": {},
            "positive_primary_research_object_overall": {},
            "by_primary_research_object": {},
            "metric_scopes": metric_scopes,
        }

    overall_truth_classes = [truth.negative_tier for truth in truths]
    overall_predicted_classes = [prediction.negative_tier for prediction in predictions]
    overall_metrics = {
        label: _binary_metrics(overall_truth_classes, overall_predicted_classes, positive_label=label)
        for label in ("positive", "negative")
    }
    overall_macro_precision = sum(item["precision"] for item in overall_metrics.values()) / len(
        overall_metrics
    )
    overall_macro_recall = sum(item["recall"] for item in overall_metrics.values()) / len(
        overall_metrics
    )
    overall_macro_f1 = sum(item["f1"] for item in overall_metrics.values()) / len(overall_metrics)
    overall_micro = _micro_metrics(
        overall_truth_classes,
        overall_predicted_classes,
        positive_labels=["positive", "negative"],
    )

    positive_pairs = [
        (truth, prediction)
        for truth, prediction in zip(truths, predictions, strict=True)
        if truth.negative_tier == "positive"
    ]
    positive_truths = [truth for truth, _prediction in positive_pairs]
    positive_predictions = [prediction for _truth, prediction in positive_pairs]

    positive_label_truths = [_label_class(item) for item in positive_truths]
    positive_label_predictions = [_label_class(item) for item in positive_predictions]
    preference_label_metrics = {
        label: _binary_metrics(
            positive_label_truths,
            positive_label_predictions,
            positive_label=label,
        )
        for label in PREFERENCE_LABELS
    }
    preference_label_overall = _classification_summary(
        positive_label_truths,
        positive_label_predictions,
        positive_labels=list(PREFERENCE_LABELS),
    )

    aggregated_truth_objects = [
        _aggregate_primary_research_object(item.primary_research_object) for item in positive_truths
    ]
    aggregated_predicted_objects = [
        _aggregate_primary_research_object(item.primary_research_object)
        for item in positive_predictions
    ]
    research_object_metrics = {
        label: _binary_metrics(
            aggregated_truth_objects,
            aggregated_predicted_objects,
            positive_label=label,
        )
        for label in AGGREGATED_RESEARCH_OBJECT_LABELS
    }
    research_object_overall = _classification_summary(
        aggregated_truth_objects,
        aggregated_predicted_objects,
        positive_labels=list(AGGREGATED_RESEARCH_OBJECT_LABELS),
    )

    positive_negative = _binary_metrics(
        overall_truth_classes,
        overall_predicted_classes,
        positive_label="positive",
    )

    return {
        "counts": counts,
        "overall": {
            "accuracy": round(
                sum(
                    1
                    for truth, prediction in zip(
                        overall_truth_classes,
                        overall_predicted_classes,
                        strict=True,
                    )
                    if truth == prediction
                )
                / len(overall_truth_classes),
                4,
            ),
            "macro_precision": round(overall_macro_precision, 4),
            "macro_recall": round(overall_macro_recall, 4),
            "macro_f1": round(overall_macro_f1, 4),
            "micro_precision": overall_micro["precision"],
            "micro_recall": overall_micro["recall"],
            "micro_f1": overall_micro["f1"],
        },
        "positive_negative": positive_negative,
        "positive_preference_label_overall": preference_label_overall,
        "by_preference_label": preference_label_metrics,
        "positive_primary_research_object_overall": research_object_overall,
        "by_primary_research_object": research_object_metrics,
        "metric_scopes": metric_scopes,
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
    positive_negative = dict(report["positive_negative"])
    positive_preference_overall = dict(report["positive_preference_label_overall"])
    positive_research_object_overall = dict(report["positive_primary_research_object_overall"])
    lines = [
        "# 评测报告",
        "",
        f"- 有效评测样本数：{counts['evaluated_count']}",
        f"- 请求失败数：{counts['request_error_count']}",
        f"- 协议错误数：{counts['protocol_error_count']}",
        "- 全量样本上的正负样本识别：",
        f"  - accuracy：{overall['accuracy']}",
        f"  - macro precision / recall / f1：{overall['macro_precision']} / {overall['macro_recall']} / {overall['macro_f1']}",
        f"  - micro precision / recall / f1：{overall['micro_precision']} / {overall['micro_recall']} / {overall['micro_f1']}",
        "",
        "## 正负样本二分类计数",
        "",
        f"- total={positive_negative.get('total_count', 0)}, tp={positive_negative.get('tp', 0)}, fp={positive_negative.get('fp', 0)}, fn={positive_negative.get('fn', 0)}, tn={positive_negative.get('tn', 0)}",
        f"- predicted={positive_negative.get('predicted_count', 0)}, support={positive_negative.get('support', 0)}, precision={positive_negative.get('precision', 0.0)}, recall={positive_negative.get('recall', 0.0)}, f1={positive_negative.get('f1', 0.0)}",
        "",
        "## 口径说明",
        "",
        "- 负样本未标注研究对象和研究子类，不纳入这两个维度统计。",
        "- 研究对象指标会先把协议标签聚合为 LLM / VLM / Diffusion / 其他 四桶后再评测。",
        "",
        "## 正样本研究子类指标",
        "",
        f"- accuracy：{positive_preference_overall.get('accuracy', 0.0)}",
        f"- macro precision / recall / f1：{positive_preference_overall.get('macro_precision', 0.0)} / {positive_preference_overall.get('macro_recall', 0.0)} / {positive_preference_overall.get('macro_f1', 0.0)}",
        f"- micro precision / recall / f1：{positive_preference_overall.get('micro_precision', 0.0)} / {positive_preference_overall.get('micro_recall', 0.0)} / {positive_preference_overall.get('micro_f1', 0.0)}",
        f"- total={positive_preference_overall.get('total_count', 0)}, correct={positive_preference_overall.get('correct_count', 0)}, incorrect={positive_preference_overall.get('incorrect_count', 0)}",
        "",
    ]
    for label, metrics in dict(report["by_preference_label"]).items():
        lines.append(
            f"- {label}: total={metrics['total_count']}, predicted={metrics['predicted_count']}, tp={metrics['tp']}, fp={metrics['fp']}, fn={metrics['fn']}, tn={metrics['tn']}, precision={metrics['precision']}, recall={metrics['recall']}, f1={metrics['f1']}, support={metrics['support']}"
        )
    lines.extend(
        [
            "",
            "## 正样本研究对象四分类指标",
            "",
            f"- accuracy：{positive_research_object_overall.get('accuracy', 0.0)}",
            f"- macro precision / recall / f1：{positive_research_object_overall.get('macro_precision', 0.0)} / {positive_research_object_overall.get('macro_recall', 0.0)} / {positive_research_object_overall.get('macro_f1', 0.0)}",
            f"- micro precision / recall / f1：{positive_research_object_overall.get('micro_precision', 0.0)} / {positive_research_object_overall.get('micro_recall', 0.0)} / {positive_research_object_overall.get('micro_f1', 0.0)}",
            f"- total={positive_research_object_overall.get('total_count', 0)}, correct={positive_research_object_overall.get('correct_count', 0)}, incorrect={positive_research_object_overall.get('incorrect_count', 0)}",
            "",
        ]
    )
    for research_object, metrics in dict(report["by_primary_research_object"]).items():
        lines.append(
            f"- {research_object}: total={metrics['total_count']}, predicted={metrics['predicted_count']}, tp={metrics['tp']}, fp={metrics['fp']}, fn={metrics['fn']}, tn={metrics['tn']}, precision={metrics['precision']}, recall={metrics['recall']}, f1={metrics['f1']}, support={metrics['support']}"
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
    if annotation.preference_labels:
        return annotation.preference_labels[0]
    return NEGATIVE_CLASS


def _aggregate_primary_research_object(primary_research_object: str) -> str:
    if primary_research_object == "LLM":
        return "LLM"
    if primary_research_object == "多模态 / VLM":
        return "VLM"
    if primary_research_object == "Diffusion / 生成模型":
        return "Diffusion"
    if primary_research_object in RESEARCH_OBJECT_LABELS:
        return "其他"
    raise ValueError(f"primary_research_object 非法：{primary_research_object}")


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
    tn = sum(
        1
        for truth, prediction in zip(truths, predictions, strict=True)
        if truth != positive_label and prediction != positive_label
    )
    support = sum(1 for truth in truths if truth == positive_label)
    predicted_count = sum(1 for prediction in predictions if prediction == positive_label)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "predicted_count": predicted_count,
        "total_count": len(truths),
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


def _classification_summary(
    truths: list[str],
    predictions: list[str],
    *,
    positive_labels: list[str],
) -> dict[str, float | int]:
    if not truths:
        return {
            "accuracy": 0.0,
            "macro_precision": 0.0,
            "macro_recall": 0.0,
            "macro_f1": 0.0,
            "micro_precision": 0.0,
            "micro_recall": 0.0,
            "micro_f1": 0.0,
            "support": 0,
            "total_count": 0,
            "correct_count": 0,
            "incorrect_count": 0,
        }
    per_label = {
        label: _binary_metrics(truths, predictions, positive_label=label)
        for label in positive_labels
    }
    macro_precision = sum(item["precision"] for item in per_label.values()) / len(per_label)
    macro_recall = sum(item["recall"] for item in per_label.values()) / len(per_label)
    macro_f1 = sum(item["f1"] for item in per_label.values()) / len(per_label)
    micro = _micro_metrics(truths, predictions, positive_labels=positive_labels)
    correct_count = sum(
        1 for truth, prediction in zip(truths, predictions, strict=True) if truth == prediction
    )
    accuracy = correct_count / len(truths)
    return {
        "accuracy": round(accuracy, 4),
        "macro_precision": round(macro_precision, 4),
        "macro_recall": round(macro_recall, 4),
        "macro_f1": round(macro_f1, 4),
        "micro_precision": micro["precision"],
        "micro_recall": micro["recall"],
        "micro_f1": micro["f1"],
        "support": len(truths),
        "total_count": len(truths),
        "correct_count": correct_count,
        "incorrect_count": len(truths) - correct_count,
    }
