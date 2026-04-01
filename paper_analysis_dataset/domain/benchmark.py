from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


PREFERENCE_LABELS = (
    "解码策略优化",
    "上下文与缓存优化",
    "系统与调度优化",
    "算子与内核优化",
    "模型结构侧推理优化",
    "模型压缩",
)

RESEARCH_OBJECT_LABELS = (
    "LLM",
    "多模态 / VLM",
    "Diffusion / 生成模型",
    "通用机器学习",
    "强化学习 / 序列决策",
    "检索 / 推荐 / 搜索",
    "计算机视觉",
    "语音 / 音频",
    "AI 系统 / 基础设施",
    "评测 / Benchmark / 数据集",
)

NEGATIVE_TIERS = ("positive", "negative")
REVIEW_STATUSES = ("pending", "conflict", "final")


def _normalize_unique(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        value = item.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _validate_subset(field_name: str, items: list[str], allowed: tuple[str, ...]) -> list[str]:
    normalized = _normalize_unique(items)
    invalid = [item for item in normalized if item not in allowed]
    if invalid:
        raise ValueError(f"{field_name} 包含非法值：{', '.join(invalid)}")
    return normalized


def _validate_required_text(field_name: str, value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} 不能为空")
    return normalized


def normalize_negative_tier(value: str) -> str:
    normalized = value.strip()
    if normalized in NEGATIVE_TIERS:
        return normalized
    raise ValueError(f"negative_tier 非法：{value}")


def _validate_single_preference_label(field_name: str, items: list[str]) -> list[str]:
    normalized = _validate_subset(field_name, items, PREFERENCE_LABELS)
    if len(normalized) > 1:
        raise ValueError(f"{field_name} 只允许单选")
    return normalized


def _clean_evidence_spans(evidence_spans: dict[str, list[str]]) -> dict[str, list[str]]:
    cleaned_evidence: dict[str, list[str]] = {}
    for label, spans in evidence_spans.items():
        normalized_label = label.strip()
        if normalized_label not in PREFERENCE_LABELS and normalized_label not in ("general", "negative"):
            raise ValueError(f"evidence_spans 包含非法标签：{normalized_label}")
        cleaned_evidence[normalized_label] = _normalize_unique(
            [str(item) for item in spans if str(item).strip()]
        )
    return cleaned_evidence


@dataclass(slots=True)
class CandidatePaper:
    paper_id: str
    title: str
    abstract: str
    authors: list[str]
    venue: str
    year: int
    source: str
    source_path: str
    primary_research_object: str
    title_zh: str = ""
    abstract_zh: str = ""
    candidate_preference_labels: list[str] = field(default_factory=list)
    candidate_negative_tier: str = "negative"
    keywords: list[str] = field(default_factory=list)
    notes: str = ""

    def __post_init__(self) -> None:
        self.paper_id = _validate_required_text("paper_id", self.paper_id)
        self.title = _validate_required_text("title", self.title)
        self.title_zh = self.title_zh.strip()
        self.abstract = self.abstract.strip()
        self.abstract_zh = self.abstract_zh.strip()
        self.authors = _normalize_unique(self.authors)
        self.venue = _validate_required_text("venue", self.venue)
        self.source = _validate_required_text("source", self.source)
        self.source_path = _validate_required_text("source_path", self.source_path)
        self.primary_research_object = _validate_subset(
            "primary_research_object",
            [self.primary_research_object],
            RESEARCH_OBJECT_LABELS,
        )[0]
        self.candidate_preference_labels = _validate_subset(
            "candidate_preference_labels",
            self.candidate_preference_labels,
            PREFERENCE_LABELS,
        )
        self.candidate_negative_tier = normalize_negative_tier(self.candidate_negative_tier)
        if self.candidate_negative_tier == "negative":
            self.candidate_preference_labels = []
        self.keywords = _normalize_unique(self.keywords)
        self.notes = self.notes.strip()

    def to_dict(self) -> dict[str, object]:
        return {
            "paper_id": self.paper_id,
            "title": self.title,
            "title_zh": self.title_zh,
            "abstract": self.abstract,
            "abstract_zh": self.abstract_zh,
            "authors": self.authors,
            "venue": self.venue,
            "year": self.year,
            "source": self.source,
            "source_path": self.source_path,
            "primary_research_object": self.primary_research_object,
            "candidate_preference_labels": self.candidate_preference_labels,
            "candidate_negative_tier": self.candidate_negative_tier,
            "keywords": self.keywords,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> CandidatePaper:
        return cls(
            paper_id=str(payload.get("paper_id", "")),
            title=str(payload.get("title", "")),
            title_zh=str(payload.get("title_zh", "")),
            abstract=str(payload.get("abstract", "")),
            abstract_zh=str(payload.get("abstract_zh", "")),
            authors=[str(item) for item in payload.get("authors", []) if str(item).strip()],
            venue=str(payload.get("venue", "")),
            year=int(payload.get("year", 0)),
            source=str(payload.get("source", "")),
            source_path=str(payload.get("source_path", "")),
            primary_research_object=str(payload.get("primary_research_object", "")),
            candidate_preference_labels=[
                str(item) for item in payload.get("candidate_preference_labels", []) if str(item).strip()
            ],
            candidate_negative_tier=str(payload.get("candidate_negative_tier", "negative")),
            keywords=[str(item) for item in payload.get("keywords", []) if str(item).strip()],
            notes=str(payload.get("notes", "")),
        )


@dataclass(slots=True)
class AnnotationRecord:
    paper_id: str
    labeler_id: str
    primary_research_object: str
    preference_labels: list[str]
    negative_tier: str = "negative"
    evidence_spans: dict[str, list[str]] = field(default_factory=dict)
    notes: str = ""
    review_status: str = "pending"

    def __post_init__(self) -> None:
        self.paper_id = _validate_required_text("paper_id", self.paper_id)
        self.labeler_id = _validate_required_text("labeler_id", self.labeler_id)
        self.primary_research_object = _validate_subset(
            "primary_research_object",
            [self.primary_research_object],
            RESEARCH_OBJECT_LABELS,
        )[0]
        self.preference_labels = _validate_single_preference_label(
            "preference_labels",
            self.preference_labels,
        )
        self.negative_tier = normalize_negative_tier(self.negative_tier)
        if self.negative_tier == "negative":
            self.preference_labels = []
        if self.review_status not in REVIEW_STATUSES:
            raise ValueError(f"review_status 非法：{self.review_status}")
        self.evidence_spans = _clean_evidence_spans(self.evidence_spans)
        self.notes = self.notes.strip()

    def to_dict(self) -> dict[str, object]:
        return {
            "paper_id": self.paper_id,
            "labeler_id": self.labeler_id,
            "primary_research_object": self.primary_research_object,
            "preference_labels": self.preference_labels,
            "negative_tier": self.negative_tier,
            "evidence_spans": self.evidence_spans,
            "notes": self.notes,
            "review_status": self.review_status,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> AnnotationRecord:
        return cls(
            paper_id=str(payload.get("paper_id", "")),
            labeler_id=str(payload.get("labeler_id", "")),
            primary_research_object=str(payload.get("primary_research_object", "")),
            preference_labels=[str(item) for item in payload.get("preference_labels", [])],
            negative_tier=str(payload.get("negative_tier", "negative")),
            evidence_spans={
                str(key): [str(item) for item in value]
                for key, value in dict(payload.get("evidence_spans", {})).items()
            },
            notes=str(payload.get("notes", "")),
            review_status=str(payload.get("review_status", "pending")),
        )


@dataclass(slots=True)
class BenchmarkRecord:
    paper_id: str
    title: str
    abstract: str
    authors: list[str]
    venue: str
    year: int
    source: str
    source_path: str
    primary_research_object: str
    title_zh: str = ""
    abstract_zh: str = ""
    candidate_preference_labels: list[str] = field(default_factory=list)
    candidate_negative_tier: str = "negative"
    keywords: list[str] = field(default_factory=list)
    notes: str = ""
    final_primary_research_object: str = ""
    final_preference_labels: list[str] = field(default_factory=list)
    final_negative_tier: str = ""
    final_labeler_ids: list[str] = field(default_factory=list)
    final_review_status: str = "pending"
    final_evidence_spans: dict[str, list[str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.paper_id = _validate_required_text("paper_id", self.paper_id)
        self.title = _validate_required_text("title", self.title)
        self.title_zh = self.title_zh.strip()
        self.abstract = self.abstract.strip()
        self.abstract_zh = self.abstract_zh.strip()
        self.authors = _normalize_unique(self.authors)
        self.venue = _validate_required_text("venue", self.venue)
        self.source = _validate_required_text("source", self.source)
        self.source_path = _validate_required_text("source_path", self.source_path)
        self.primary_research_object = _validate_subset(
            "primary_research_object",
            [self.primary_research_object],
            RESEARCH_OBJECT_LABELS,
        )[0]
        self.candidate_preference_labels = _validate_subset(
            "candidate_preference_labels",
            self.candidate_preference_labels,
            PREFERENCE_LABELS,
        )
        self.candidate_negative_tier = normalize_negative_tier(self.candidate_negative_tier)
        if self.candidate_negative_tier == "negative":
            self.candidate_preference_labels = []
        self.keywords = _normalize_unique(self.keywords)
        self.notes = self.notes.strip()

        if self.final_primary_research_object:
            self.final_primary_research_object = _validate_subset(
                "final_primary_research_object",
                [self.final_primary_research_object],
                RESEARCH_OBJECT_LABELS,
            )[0]
        self.final_preference_labels = _validate_single_preference_label(
            "final_preference_labels",
            self.final_preference_labels,
        )
        if self.final_negative_tier:
            self.final_negative_tier = normalize_negative_tier(self.final_negative_tier)
            if self.final_negative_tier == "negative":
                self.final_preference_labels = []
        self.final_labeler_ids = _normalize_unique(self.final_labeler_ids)
        if self.final_review_status not in REVIEW_STATUSES:
            raise ValueError(f"final_review_status 非法：{self.final_review_status}")
        self.final_evidence_spans = _clean_evidence_spans(self.final_evidence_spans)

    @property
    def resolved_primary_research_object(self) -> str:
        return self.final_primary_research_object or self.primary_research_object

    @property
    def resolved_preference_labels(self) -> list[str]:
        return self.final_preference_labels

    @property
    def resolved_negative_tier(self) -> str:
        return self.final_negative_tier or self.candidate_negative_tier

    @property
    def resolved_labeler_ids(self) -> list[str]:
        return self.final_labeler_ids

    @property
    def resolved_review_status(self) -> str:
        return self.final_review_status

    @property
    def resolved_evidence_spans(self) -> dict[str, list[str]]:
        return self.final_evidence_spans

    def to_candidate_paper(self) -> CandidatePaper:
        return CandidatePaper(
            paper_id=self.paper_id,
            title=self.title,
            title_zh=self.title_zh,
            abstract=self.abstract,
            abstract_zh=self.abstract_zh,
            authors=self.authors,
            venue=self.venue,
            year=self.year,
            source=self.source,
            source_path=self.source_path,
            primary_research_object=self.primary_research_object,
            candidate_preference_labels=self.candidate_preference_labels,
            candidate_negative_tier=self.candidate_negative_tier,
            keywords=self.keywords,
            notes=self.notes,
        )

    def with_final_annotation(
        self,
        annotation: AnnotationRecord,
        *,
        labeler_ids: list[str],
        review_status: str,
    ) -> BenchmarkRecord:
        return BenchmarkRecord(
            paper_id=self.paper_id,
            title=self.title,
            title_zh=self.title_zh,
            abstract=self.abstract,
            abstract_zh=self.abstract_zh,
            authors=self.authors,
            venue=self.venue,
            year=self.year,
            source=self.source,
            source_path=self.source_path,
            primary_research_object=self.primary_research_object,
            candidate_preference_labels=self.candidate_preference_labels,
            candidate_negative_tier=self.candidate_negative_tier,
            keywords=self.keywords,
            notes=annotation.notes or self.notes,
            final_primary_research_object=annotation.primary_research_object,
            final_preference_labels=annotation.preference_labels,
            final_negative_tier=annotation.negative_tier,
            final_labeler_ids=labeler_ids,
            final_review_status=review_status,
            final_evidence_spans=annotation.evidence_spans,
        )

    def to_dict(self, *, include_final_annotations: bool = True) -> dict[str, object]:
        payload: dict[str, object] = {
            "paper_id": self.paper_id,
            "title": self.title,
            "title_zh": self.title_zh,
            "abstract": self.abstract,
            "abstract_zh": self.abstract_zh,
            "authors": self.authors,
            "venue": self.venue,
            "year": self.year,
            "source": self.source,
            "source_path": self.source_path,
            "primary_research_object": self.primary_research_object,
            "candidate_preference_labels": self.candidate_preference_labels,
            "candidate_negative_tier": self.candidate_negative_tier,
            "keywords": self.keywords,
            "notes": self.notes,
        }
        if include_final_annotations:
            payload.update(
                {
                    "final_primary_research_object": self.final_primary_research_object,
                    "final_preference_labels": self.final_preference_labels,
                    "final_negative_tier": self.final_negative_tier,
                    "final_labeler_ids": self.final_labeler_ids,
                    "final_review_status": self.final_review_status,
                    "final_evidence_spans": self.final_evidence_spans,
                }
            )
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BenchmarkRecord:
        return cls(
            paper_id=str(payload.get("paper_id", "")),
            title=str(payload.get("title", "")),
            title_zh=str(payload.get("title_zh", "")),
            abstract=str(payload.get("abstract", "")),
            abstract_zh=str(payload.get("abstract_zh", "")),
            authors=[str(item) for item in payload.get("authors", [])],
            venue=str(payload.get("venue", "")),
            year=int(payload.get("year", 0)),
            source=str(payload.get("source", "")),
            source_path=str(payload.get("source_path", "")),
            primary_research_object=str(payload.get("primary_research_object", "")),
            candidate_preference_labels=[
                str(item) for item in payload.get("candidate_preference_labels", [])
            ],
            candidate_negative_tier=str(payload.get("candidate_negative_tier", "negative")),
            keywords=[str(item) for item in payload.get("keywords", [])],
            notes=str(payload.get("notes", "")),
            final_primary_research_object=str(payload.get("final_primary_research_object", "")),
            final_preference_labels=[
                str(item) for item in payload.get("final_preference_labels", [])
            ],
            final_negative_tier=str(payload.get("final_negative_tier", "")),
            final_labeler_ids=[str(item) for item in payload.get("final_labeler_ids", [])],
            final_review_status=str(payload.get("final_review_status", "pending")),
            final_evidence_spans={
                str(key): [str(item) for item in value]
                for key, value in dict(payload.get("final_evidence_spans", {})).items()
            },
        )


@dataclass(slots=True)
class ConflictRecord:
    paper_id: str
    conflicting_fields: list[str]
    codex_annotation: AnnotationRecord
    human_annotation: AnnotationRecord
    resolved_annotation: AnnotationRecord | None = None

    def __post_init__(self) -> None:
        self.paper_id = _validate_required_text("paper_id", self.paper_id)
        self.conflicting_fields = _normalize_unique(self.conflicting_fields)
        if not self.conflicting_fields:
            raise ValueError("conflicting_fields 不能为空")

    @property
    def is_resolved(self) -> bool:
        return self.resolved_annotation is not None

    def to_dict(self) -> dict[str, object]:
        return {
            "paper_id": self.paper_id,
            "conflicting_fields": self.conflicting_fields,
            "codex_annotation": self.codex_annotation.to_dict(),
            "human_annotation": self.human_annotation.to_dict(),
            "resolved_annotation": (
                self.resolved_annotation.to_dict() if self.resolved_annotation else None
            ),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ConflictRecord:
        resolved_payload = payload.get("resolved_annotation")
        return cls(
            paper_id=str(payload.get("paper_id", "")),
            conflicting_fields=[str(item) for item in payload.get("conflicting_fields", [])],
            codex_annotation=AnnotationRecord.from_dict(
                dict(payload.get("codex_annotation", {}))
            ),
            human_annotation=AnnotationRecord.from_dict(
                dict(payload.get("human_annotation", {}))
            ),
            resolved_annotation=(
                AnnotationRecord.from_dict(dict(resolved_payload))
                if isinstance(resolved_payload, dict)
                else None
            ),
        )
