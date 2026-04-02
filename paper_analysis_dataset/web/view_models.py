from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlencode

from paper_analysis_dataset.domain.benchmark import AnnotationRecord, BenchmarkRecord
from paper_analysis_dataset.domain.benchmark import PREFERENCE_LABELS, RESEARCH_OBJECT_LABELS
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.benchmark_reporter import build_distribution_report


@dataclass(slots=True)
class AnnotationAppState:
    repository: AnnotationRepository

    def list_papers(
        self,
        status_filter: str = "all",
        *,
        preference_label_filter: str = "all",
        negative_tier_filter: str = "all",
        research_object_filter: str = "all",
    ) -> list[dict[str, object]]:
        records = {
            item.paper_id: item
            for item in self.repository.load_records()
        }
        ai = {
            item.paper_id: item
            for item in self.repository.load_annotations(self.repository.annotations_ai_path)
        }
        human = {
            item.paper_id: item
            for item in self.repository.load_annotations(self.repository.annotations_human_path)
        }
        conflicts = {
            item.paper_id: item
            for item in self.repository.load_conflicts(self.repository.conflicts_path)
        }
        rows: list[dict[str, object]] = []
        for paper_id, record in records.items():
            has_conflict = paper_id in conflicts and not conflicts[paper_id].is_resolved
            human_completed = paper_id in human
            seed = human.get(paper_id) or ai.get(paper_id)
            negative_tier = _derive_negative_tier(record=record)
            primary_research_object = _display_primary_research_object(
                record=record,
                seed=seed,
            )
            preference_labels = _display_preference_labels(record=record, seed=seed)
            status = _derive_status(
                has_conflict=has_conflict,
                human_completed=human_completed,
                negative_tier=negative_tier,
            )
            if status_filter != "all" and status != status_filter:
                continue
            if negative_tier_filter != "all" and negative_tier != negative_tier_filter:
                continue
            if (
                research_object_filter != "all"
                and primary_research_object != research_object_filter
            ):
                continue
            if (
                preference_label_filter != "all"
                and preference_label_filter not in preference_labels
            ):
                continue
            rows.append(
                {
                    "paper_id": paper_id,
                    "title": record.title,
                    "title_zh": record.title_zh,
                    "display_title": _display_title(record),
                    "venue": record.venue,
                    "primary_research_object": primary_research_object,
                    "preference_labels": preference_labels,
                    "negative_tier": negative_tier,
                    "negative_tier_label": _negative_tier_label(negative_tier),
                    "ai_completed": paper_id in ai,
                    "human_completed": human_completed,
                    "has_conflict": has_conflict,
                    "status": status,
                    "status_label": _status_label(status),
                }
            )
        return rows

    def list_status_counts(self) -> dict[str, int]:
        counts = {"all": 0, "negative": 0, "pending": 0, "completed": 0, "conflict": 0}
        rows = self.list_papers(status_filter="all")
        counts["all"] = len(rows)
        for row in rows:
            counts[str(row["status"])] += 1
        return counts

    def paper_detail(self, paper_id: str) -> dict[str, object]:
        records = {item.paper_id: item for item in self.repository.load_records()}
        record = records[paper_id]
        ai = {
            item.paper_id: item
            for item in self.repository.load_annotations(self.repository.annotations_ai_path)
        }.get(paper_id)
        human = {
            item.paper_id: item
            for item in self.repository.load_annotations(self.repository.annotations_human_path)
        }.get(paper_id)
        merged = {
            item.paper_id: item
            for item in self.repository.load_annotations(self.repository.merged_path)
        }.get(paper_id)
        return {
            "candidate": record.to_candidate_paper(),
            "ai": ai,
            "human": human,
            "core_seed": merged or human or ai,
            "preference_seed": merged or human or ai,
            "supplement_seed": merged or human,
            "preference_labels": PREFERENCE_LABELS,
            "research_object_labels": RESEARCH_OBJECT_LABELS,
        }

    def next_pending_paper_id(self, current_paper_id: str | None = None) -> str | None:
        pending_ids = [item["paper_id"] for item in self.list_papers(status_filter="pending")]
        if not pending_ids:
            return None
        if current_paper_id not in pending_ids:
            return pending_ids[0]
        current_index = pending_ids.index(current_paper_id)
        if current_index + 1 < len(pending_ids):
            return pending_ids[current_index + 1]
        if current_index > 0:
            return pending_ids[0]
        return None

    def conflicts(self) -> list[dict[str, object]]:
        records = {item.paper_id: item for item in self.repository.load_records()}
        rows = []
        for conflict in self.repository.load_conflicts(self.repository.conflicts_path):
            if conflict.is_resolved:
                continue
            record = records[conflict.paper_id]
            rows.append(
                {
                    "paper_id": conflict.paper_id,
                    "title": record.title,
                    "title_zh": record.title_zh,
                    "display_title": _display_title(record),
                    "display_abstract": _display_abstract(record),
                    "conflicting_fields": conflict.conflicting_fields,
                    "resolved": conflict.is_resolved,
                    "codex": conflict.codex_annotation,
                    "human": conflict.human_annotation,
                    "resolved_choice": _resolved_choice(conflict),
                }
            )
        return rows

    def dashboard(self) -> dict[str, object]:
        records = self.repository.load_records()
        ai_annotations = self.repository.load_annotations(self.repository.annotations_ai_path)
        human_annotations = self.repository.load_annotations(self.repository.annotations_human_path)
        merged_annotations = self.repository.load_annotations(self.repository.merged_path)
        if self.repository.stats_path.exists():
            report = self.repository.read_json(self.repository.stats_path)
        else:
            report = build_distribution_report(
                records,
                annotations_ai=ai_annotations,
                annotations_human=human_annotations,
                merged_annotations=merged_annotations,
            )
        return {
            "summary": report,
            "total_candidates": len(records),
            "total_ai_annotations": len(ai_annotations),
            "total_human_annotations": len(human_annotations),
            "total_merged_annotations": len(merged_annotations),
            "total_conflicts": len(self.repository.load_conflicts(self.repository.conflicts_path)),
        }

    def paper_filter_options(self) -> dict[str, object]:
        rows = self.list_papers(status_filter="all")
        preference_labels = sorted(
            {
                label
                for row in rows
                for label in list(row.get("preference_labels", []))
            }
        )
        research_objects = sorted(
            {
                str(row["primary_research_object"])
                for row in rows
                if str(row["primary_research_object"]).strip()
            }
        )
        return {
            "preference_labels": preference_labels,
            "negative_tiers": [
                {"value": "positive", "label": "正样本"},
                {"value": "negative", "label": "负样本"},
            ],
            "research_objects": research_objects,
        }

    def papers_query_string(
        self,
        *,
        status_filter: str,
        preference_label_filter: str,
        negative_tier_filter: str,
        research_object_filter: str,
    ) -> str:
        query = {
            "status": status_filter,
            "preference_label": preference_label_filter,
            "negative_tier": negative_tier_filter,
            "research_object": research_object_filter,
        }
        return urlencode(query)

    def papers_reset_url(self, *, status_filter: str) -> str:
        if status_filter == "all":
            return "/papers"
        return "/papers?" + urlencode({"status": status_filter})


def _derive_status(*, has_conflict: bool, human_completed: bool, negative_tier: str) -> str:
    if has_conflict:
        return "conflict"
    if negative_tier == "negative" and not human_completed:
        return "negative"
    if human_completed:
        return "completed"
    return "pending"


def _status_label(status: str) -> str:
    return {
        "all": "全部",
        "negative": "待抽检",
        "pending": "待复标",
        "completed": "已完成",
        "conflict": "有冲突",
    }[status]


def _derive_negative_tier(*, record: BenchmarkRecord) -> str:
    return record.candidate_negative_tier


def _negative_tier_label(negative_tier: str) -> str:
    return {
        "positive": "正样本",
        "negative": "负样本",
    }[negative_tier]


def _resolved_choice(conflict: object) -> str | None:
    resolved = conflict.resolved_annotation
    if resolved is None:
        return None
    if _same_annotation_payload(resolved, conflict.codex_annotation):
        return "codex"
    if _same_annotation_payload(resolved, conflict.human_annotation):
        return "human"
    return "custom"


def _same_annotation_payload(left: AnnotationRecord, right: AnnotationRecord) -> bool:
    return (
        left.primary_research_object == right.primary_research_object
        and left.preference_labels == right.preference_labels
        and left.negative_tier == right.negative_tier
        and left.evidence_spans == right.evidence_spans
        and left.notes == right.notes
    )


def _display_title(record: BenchmarkRecord) -> str:
    return record.title_zh or record.title


def _display_primary_research_object(
    *,
    record: BenchmarkRecord,
    seed: AnnotationRecord | None,
) -> str:
    if seed is not None:
        return seed.primary_research_object
    return record.primary_research_object


def _display_abstract(record: BenchmarkRecord) -> str:
    return record.abstract_zh or record.abstract


def _display_preference_labels(
    *,
    record: BenchmarkRecord,
    seed: AnnotationRecord | None,
) -> list[str]:
    if seed is not None:
        return seed.preference_labels
    return record.candidate_preference_labels
