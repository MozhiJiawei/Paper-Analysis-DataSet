from __future__ import annotations

import json
import shutil
from pathlib import Path

from paper_analysis_dataset.domain.benchmark import PREFERENCE_LABELS
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.benchmark_builder import (
    BenchmarkBuilder,
    DEFAULT_RELEASE_QUOTA_BY_VENUE,
    DEFAULT_VENUE_TARGETS,
)
from paper_analysis_dataset.services.benchmark_reporter import build_distribution_report
from paper_analysis_dataset.services.doubao_abstract_translator import DoubaoAbstractTranslator
from paper_analysis_dataset.shared.paths import DATASET_ROOT_DIR, SUPERPROJECT_ROOT_DIR


BENCHMARK_ROOT = DATASET_ROOT_DIR / "data" / "benchmarks" / "paper-filter"
PAPERLISTS_ROOT = SUPERPROJECT_ROOT_DIR / "third_party" / "paperlists"


def _build_schema_payload() -> dict[str, object]:
    return {
        "name": "paper-filter",
        "version": "2026-03-26",
        "description": "单版本 paper-filter benchmark 协议。",
        "files": {
            "records": "records.jsonl",
            "annotations_ai": "annotations-ai.jsonl",
            "annotations_human": "annotations-human.jsonl",
            "merged": "merged.jsonl",
            "conflicts": "conflicts.jsonl",
            "stats": "stats.json",
        },
        "record_fields": {
            "paper_id": "string",
            "title": "string",
            "abstract": "string",
            "abstract_zh": "string",
            "authors": "string[]",
            "venue": "string",
            "year": "integer",
            "source": "string",
            "source_path": "string",
            "primary_research_object": "enum",
            "candidate_preference_labels": "enum[]",
            "candidate_negative_tier": "enum",
            "keywords": "string[]",
            "notes": "string",
        },
        "annotation_fields": {
            "paper_id": "string",
            "labeler_id": "string",
            "primary_research_object": "enum",
            "preference_labels": "enum[]",
            "negative_tier": "enum",
            "evidence_spans": "object",
            "notes": "string",
            "review_status": "enum",
        },
        "annotation_constraints": {
            "preference_labels_cardinality": "0..1",
            "positive_requires_exactly_one_preference_label": True,
        },
        "negative_tiers": ["positive", "negative"],
        "preference_labels": list(PREFERENCE_LABELS),
    }


def rebuild_benchmark(
    *,
    benchmark_root: Path | None = None,
    abstract_translator: object | None = None,
) -> dict[str, object]:
    target_root = benchmark_root or BENCHMARK_ROOT
    if target_root.exists():
        shutil.rmtree(target_root)

    repository = AnnotationRepository(target_root)
    builder = BenchmarkBuilder(PAPERLISTS_ROOT)
    candidates = builder.build_inference_acceleration_candidates(
        DEFAULT_VENUE_TARGETS,
        quota_by_venue=DEFAULT_RELEASE_QUOTA_BY_VENUE,
        minimum_score=12,
    )
    records = builder.build_records(candidates, abstract_translator=abstract_translator)
    stats = build_distribution_report(records)

    repository.write_records(records)
    repository.write_annotations([], repository.annotations_ai_path)
    repository.write_annotations([], repository.annotations_human_path)
    repository.write_annotations([], repository.merged_path)
    repository.write_conflicts([], repository.conflicts_path)
    repository.write_json(_build_schema_payload(), repository.schema_path)
    repository.write_json(stats, repository.stats_path)

    return {
        "benchmark_root": str(target_root),
        "total_records": len(records),
        "annotations_ai": 0,
        "annotations_human": 0,
        "merged": 0,
        "venues": builder.summarize_dataset(records)["venues"],
        "by_primary_research_object": stats["by_primary_research_object"],
    }


def main() -> None:
    summary = rebuild_benchmark(abstract_translator=DoubaoAbstractTranslator())
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
