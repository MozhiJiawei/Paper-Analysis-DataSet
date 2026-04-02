from __future__ import annotations

from paper_analysis_dataset.domain.benchmark import (
    NEGATIVE_TIERS,
    PREFERENCE_LABELS,
)


SCHEMA_VERSION = "2026-04-01"


def build_schema_payload() -> dict[str, object]:
    return {
        "name": "paper-filter",
        "version": SCHEMA_VERSION,
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
            "title_zh": "string",
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
        "negative_tiers": list(NEGATIVE_TIERS),
        "preference_labels": list(PREFERENCE_LABELS),
    }
