from __future__ import annotations

from collections import Counter, defaultdict

from paper_analysis_dataset.domain.benchmark import BenchmarkRecord, PREFERENCE_LABELS


def build_distribution_report(records: list[BenchmarkRecord]) -> dict[str, object]:
    by_object = Counter(record.resolved_primary_research_object for record in records)
    by_tier = Counter(record.resolved_negative_tier for record in records)
    by_label = {
        label: {
            "positive": sum(1 for record in records if label in record.resolved_preference_labels),
            "negative": sum(
                1
                for record in records
                if record.resolved_negative_tier == "negative"
                and label in record.resolved_preference_labels
            ),
        }
        for label in PREFERENCE_LABELS
    }
    return {
        "total_records": len(records),
        "by_primary_research_object": dict(sorted(by_object.items())),
        "by_negative_tier": dict(sorted(by_tier.items())),
        "by_preference_label": by_label,
    }


def evaluate_predictions(
    records: list[BenchmarkRecord],
    predicted_paper_ids_by_label: dict[str, set[str]],
) -> dict[str, object]:
    report: dict[str, object] = {"overall": {}, "by_object_and_label": {}}
    cross_buckets: dict[tuple[str, str], list[BenchmarkRecord]] = defaultdict(list)
    for record in records:
        for label in PREFERENCE_LABELS:
            cross_buckets[(record.resolved_primary_research_object, label)].append(record)

    for label in PREFERENCE_LABELS:
        predicted = predicted_paper_ids_by_label.get(label, set())
        positives = {record.paper_id for record in records if label in record.resolved_preference_labels}
        true_positive = len(predicted & positives)
        precision = true_positive / len(predicted) if predicted else 0.0
        recall = true_positive / len(positives) if positives else 0.0
        report["overall"][label] = {
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "predicted_count": len(predicted),
            "positive_count": len(positives),
        }

    for (research_object, label), bucket in cross_buckets.items():
        positives = {record.paper_id for record in bucket if label in record.resolved_preference_labels}
        predicted = predicted_paper_ids_by_label.get(label, set()) & {record.paper_id for record in bucket}
        true_positive = len(predicted & positives)
        precision = true_positive / len(predicted) if predicted else 0.0
        recall = true_positive / len(positives) if positives else 0.0
        report["by_object_and_label"][f"{research_object} × {label}"] = {
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "sample_count": len(bucket),
        }
    return report
