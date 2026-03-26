from __future__ import annotations

from dataclasses import dataclass

from paper_analysis_dataset.domain.benchmark import AnnotationRecord, BenchmarkRecord, ConflictRecord


@dataclass(slots=True)
class MergeResult:
    records: list[BenchmarkRecord]
    merged_annotations: list[AnnotationRecord]
    conflicts: list[ConflictRecord]


def merge_annotations(
    records: list[BenchmarkRecord],
    codex_annotations: list[AnnotationRecord],
    human_annotations: list[AnnotationRecord],
    arbitrations: list[AnnotationRecord] | None = None,
) -> MergeResult:
    records_by_id = {item.paper_id: item for item in records}
    codex_by_id = {item.paper_id: item for item in codex_annotations}
    human_by_id = {item.paper_id: item for item in human_annotations}
    arbitration_by_id = {item.paper_id: item for item in (arbitrations or [])}

    all_ids = sorted(set(codex_by_id) | set(human_by_id))
    merged_records: list[BenchmarkRecord] = []
    merged_annotations: list[AnnotationRecord] = []
    conflicts: list[ConflictRecord] = []

    for paper_id in all_ids:
        record = records_by_id.get(paper_id)
        if record is None:
            raise ValueError(f"主表缺少 paper_id：{paper_id}")

        codex = codex_by_id.get(paper_id)
        human = human_by_id.get(paper_id)
        if codex is None or human is None:
            raise ValueError(f"缺少成对标注：{paper_id}")

        conflicting_fields = _detect_conflicting_fields(codex, human)
        if conflicting_fields:
            resolved = arbitration_by_id.get(paper_id)
            conflicts.append(
                ConflictRecord(
                    paper_id=paper_id,
                    conflicting_fields=conflicting_fields,
                    codex_annotation=codex,
                    human_annotation=human,
                    resolved_annotation=resolved,
                )
            )
            if resolved is None:
                merged_records.append(
                    record.with_final_annotation(
                        human,
                        labeler_ids=[codex.labeler_id, human.labeler_id],
                        review_status="conflict",
                    )
                )
                continue
            final_record = record.with_final_annotation(
                resolved,
                labeler_ids=[codex.labeler_id, human.labeler_id, resolved.labeler_id],
                review_status="final",
            )
            merged_records.append(final_record)
            merged_annotations.append(resolved)
            continue

        final_record = record.with_final_annotation(
            human,
            labeler_ids=[codex.labeler_id, human.labeler_id],
            review_status="final",
        )
        merged_records.append(final_record)
        merged_annotations.append(
            AnnotationRecord(
                paper_id=human.paper_id,
                labeler_id="merged",
                primary_research_object=human.primary_research_object,
                preference_labels=human.preference_labels,
                negative_tier=human.negative_tier,
                evidence_spans=human.evidence_spans,
                notes=human.notes,
                review_status="final",
            )
        )

    untouched_records = [record for record in records if record.paper_id not in set(all_ids)]
    return MergeResult(
        records=sorted([*untouched_records, *merged_records], key=lambda item: item.paper_id),
        merged_annotations=sorted(merged_annotations, key=lambda item: item.paper_id),
        conflicts=conflicts,
    )


def _detect_conflicting_fields(codex: AnnotationRecord, human: AnnotationRecord) -> list[str]:
    conflicts: list[str] = []
    if codex.primary_research_object != human.primary_research_object:
        conflicts.append("primary_research_object")
    if sorted(codex.preference_labels) != sorted(human.preference_labels):
        conflicts.append("preference_labels")
    if codex.negative_tier != human.negative_tier:
        conflicts.append("negative_tier")
    return conflicts
