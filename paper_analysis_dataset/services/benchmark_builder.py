from __future__ import annotations

from collections import Counter, defaultdict
from concurrent.futures import Future
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Protocol

from paper_analysis_dataset.domain.benchmark import BenchmarkRecord, CandidatePaper, PREFERENCE_LABELS
from paper_analysis_dataset.shared.conference.paper_model import Paper
from paper_analysis_dataset.shared.conference.paperlists_parser import (
    filter_accepted_records,
    load_raw_records,
    normalize_records,
)


DEFAULT_VENUE_TARGETS = (
    ("aaai", 2025),
    ("iclr", 2025),
    ("iclr", 2026),
    ("icml", 2025),
    ("nips", 2025),
)

DEFAULT_RELEASE_QUOTA_BY_VENUE = {
    ("aaai", 2025): 40,
    ("iclr", 2025): 40,
    ("iclr", 2026): 40,
    ("icml", 2025): 40,
    ("nips", 2025): 40,
}

DEFAULT_SCHEDULING_AUGMENT_VENUES = (
    ("iclr", 2026),
    ("nips", 2025),
    ("icml", 2025),
    ("aaai", 2025),
)

PREFERENCE_RULES: dict[str, tuple[str, ...]] = {
    "解码策略优化": (
        "decode",
        "decoding",
        "speculative",
        "early exit",
        "beam search",
        "autoregressive",
        "multi-token prediction",
        "draft model",
        "lookahead",
    ),
    "上下文与缓存优化": (
        "kv cache",
        "cache eviction",
        "cache compression",
        "token pruning",
        "long context",
        "context compression",
        "prompt compression",
        "memory",
        "gist token",
    ),
    "系统与调度优化": (
        "serving",
        "scheduling",
        "batching",
        "throughput",
        "latency",
        "runtime",
        "prefill",
        "deployment",
        "offloading",
        "cluster",
    ),
    "算子与内核优化": (
        "kernel",
        "operator fusion",
        "compiler",
        "fused attention",
        "cuda",
        "attention kernel",
        "gpu",
        "flash attention",
        "gemm",
    ),
    "模型压缩": (
        "quantization",
        "distillation",
        "pruning",
        "low-bit",
        "sparsity",
        "mixed-precision",
        "binary",
        "post-training quantization",
    ),
}

INFERENCE_ACCELERATION_STRONG_TERMS: tuple[str, ...] = (
    "speculative decoding",
    "self-speculative",
    "draft model",
    "multi-token prediction",
    "kv cache",
    "cache compression",
    "cache eviction",
    "token pruning",
    "long context",
    "prefill",
    "continuous batching",
    "batching",
    "throughput",
    "latency",
    "serving",
    "runtime",
    "kernel",
    "fused attention",
    "flash attention",
    "cuda",
    "gemm",
    "quantization",
    "quantized",
    "4-bit",
    "3-bit",
    "2-bit",
    "low-bit",
    "distillation",
    "pruning",
    "sparsity",
    "compression",
    "compressed",
    "offloading",
    "mixed-precision",
)

INFERENCE_ACCELERATION_CONTEXT_TERMS: tuple[str, ...] = (
    "inference",
    "inferencing",
    "decode",
    "decoding",
    "serving",
    "generation",
    "autoregressive",
    "language model",
    "large language model",
    "llm",
    "transformer",
    "vision-language",
    "vlm",
    "multimodal",
    "reasoning model",
    "agent",
)

INFERENCE_ACCELERATION_EXCLUDE_TERMS: tuple[str, ...] = (
    "training",
    "pretraining",
    "pre-training",
    "finetuning",
    "fine-tuning",
    "alignment",
    "preference optimization",
    "reward model",
    "reinforcement learning",
    "segmentation",
    "classification",
    "speech recognition",
)

SCHEDULING_STRONG_PHRASES: tuple[str, ...] = (
    "muxserve",
    "distserve",
    "hexgen-2",
    "adaptive rescheduling in prefill-decode disaggregated llm inference",
    "efficiently serving large multimodal models using epd disaggregation",
    "epd-serve",
    "sem-moe",
    "capacity-aware inference",
    "spaceserve",
)

SCHEDULING_TERM_GROUPS: tuple[tuple[str, tuple[str, ...], int], ...] = (
    (
        "llm_serving",
        (
            "llm serving",
            "language model serving",
            "inference serving",
            "request scheduling",
            "continuous batching",
            "goodput",
            "tail latency",
            "admission control",
            "load balancing",
        ),
        4,
    ),
    (
        "multi_tenant",
        (
            "multiple llm serving",
            "multiple model serving",
            "multi-tenant serving",
            "multi-tenant lora",
            "spatial-temporal multiplexing",
            "gpu multiplexing",
            "co-location",
            "shared gpu",
            "spatial multiplexing",
        ),
        4,
    ),
    (
        "pd_disaggregation",
        (
            "prefill-decode disaggregation",
            "prefill decoding disaggregation",
            "disaggregated llm serving",
            "disaggregated generative inference",
            "decode rescheduling",
            "kv cache transfer",
            "heterogeneous gpus",
            "heterogeneous gpu",
        ),
        5,
    ),
    (
        "epd_disaggregation",
        (
            "encode-prefill-decode",
            "epd disaggregation",
            "multimodal serving",
            "large multimodal models",
            "stage-level disaggregation",
        ),
        5,
    ),
    (
        "moe_scheduling",
        (
            "expert parallelism",
            "expert parallel inference",
            "moe serving",
            "all-to-all communication",
            "straggler effect",
            "expert routing",
            "sample placement",
            "model-data collaborative scheduling",
            "semantic parallelism",
        ),
        5,
    ),
)

SCHEDULING_EXCLUDE_TERMS: tuple[str, ...] = (
    "training",
    "pretraining",
    "pre-training",
    "fine-tuning",
    "finetuning",
    "rlhf",
    "alignment",
    "reinforcement learning",
)

RESEARCH_OBJECT_RULES: tuple[tuple[str, str], ...] = (
    ("LLM", "llm large language model language model autoregressive chat model reasoning model"),
    ("多模态 / VLM", "multimodal vlm vision-language text-image"),
    ("AI 系统 / 基础设施", "serving runtime compiler gpu cluster kernel systems infrastructure"),
    ("评测 / Benchmark / 数据集", "benchmark dataset evaluation leaderboard testbed"),
    ("Diffusion / 生成模型", "diffusion denoising flow-matching flow matching image synthesis"),
    ("强化学习 / 序列决策", "reinforcement learning decision-making policy optimization control bandit"),
    ("检索 / 推荐 / 搜索", "retrieval recommendation search ranking"),
    ("计算机视觉", "vision image video detection segmentation recognition"),
    ("语音 / 音频", "speech audio voice sound"),
    ("通用机器学习", "transformer neural network machine learning"),
)


@dataclass(slots=True)
class DatasetValidationSummary:
    total_records: int
    label_positive_counts: dict[str, int]
    label_negative_counts: dict[str, int]
    unresolved_conflict_count: int
    duplicate_paper_ids: list[str]


@dataclass(slots=True)
class ScoredCandidatePaper:
    candidate: CandidatePaper
    score: int
    matched_terms: tuple[str, ...]


@dataclass(slots=True)
class SchedulingCandidatePaper:
    candidate: CandidatePaper
    score: int
    matched_groups: tuple[str, ...]


class AbstractTranslator(Protocol):
    def submit_translate(self, candidate: CandidatePaper) -> Future[str]: ...


class BenchmarkBuilder:
    def __init__(self, paperlists_root: Path) -> None:
        self.paperlists_root = paperlists_root

    def build_candidates(
        self,
        venue_targets: tuple[tuple[str, int], ...] = DEFAULT_VENUE_TARGETS,
        *,
        limit_per_venue: int | None = None,
    ) -> list[CandidatePaper]:
        candidates: list[CandidatePaper] = []
        for venue_key, year in venue_targets:
            source_path = self.paperlists_root / venue_key / f"{venue_key}{year}.json"
            papers = normalize_records(
                filter_accepted_records(load_raw_records(source_path, venue_key.upper(), year))
            )
            ordered = sorted(papers, key=lambda item: item.paper_id)
            if limit_per_venue is not None:
                ordered = ordered[:limit_per_venue]
            candidates.extend(self._to_candidate(paper) for paper in ordered)
        return candidates

    def build_inference_acceleration_candidates(
        self,
        venue_targets: tuple[tuple[str, int], ...] = DEFAULT_VENUE_TARGETS,
        *,
        quota_by_venue: dict[tuple[str, int], int] | None = None,
        minimum_score: int = 12,
    ) -> list[CandidatePaper]:
        selected: list[CandidatePaper] = []
        quotas = quota_by_venue or DEFAULT_RELEASE_QUOTA_BY_VENUE
        for venue_key, year in venue_targets:
            scored_candidates = self._score_candidates_for_venue(
                venue_key,
                year,
                minimum_score=minimum_score,
            )
            selected.extend(
                item.candidate
                for item in self._select_release_candidates(
                    scored_candidates,
                    quota=quotas.get((venue_key, year), 40),
                )
            )
        return selected

    def build_records(
        self,
        candidates: list[CandidatePaper],
        *,
        abstract_translator: AbstractTranslator | None = None,
    ) -> list[BenchmarkRecord]:
        records: list[BenchmarkRecord] = []
        for candidate in candidates:
            evidence = _build_evidence(candidate)
            abstract_zh = candidate.abstract_zh
            if not abstract_zh and abstract_translator is not None:
                abstract_zh = abstract_translator.submit_translate(candidate).result()
            records.append(
                BenchmarkRecord(
                    paper_id=candidate.paper_id,
                    title=candidate.title,
                    title_zh=candidate.title_zh,
                    abstract=candidate.abstract,
                    abstract_zh=abstract_zh,
                    authors=candidate.authors,
                    venue=candidate.venue,
                    year=candidate.year,
                    source=candidate.source,
                    source_path=candidate.source_path,
                    primary_research_object=candidate.primary_research_object,
                    candidate_preference_labels=candidate.candidate_preference_labels,
                    candidate_negative_tier=candidate.candidate_negative_tier,
                    keywords=candidate.keywords,
                    notes=candidate.notes,
                    final_primary_research_object=candidate.primary_research_object,
                    final_preference_labels=candidate.candidate_preference_labels,
                    final_negative_tier=candidate.candidate_negative_tier,
                    final_labeler_ids=["candidate_seed", "human_reviewer"],
                    final_review_status="final",
                    final_evidence_spans=evidence,
                )
            )
        return records

    def summarize_dataset(self, records: list[BenchmarkRecord]) -> dict[str, object]:
        label_positive_counts = {
            label: sum(1 for record in records if label in record.resolved_preference_labels)
            for label in PREFERENCE_LABELS
        }
        venue_counts = Counter(record.venue for record in records)
        object_counts = Counter(record.resolved_primary_research_object for record in records)
        return {
            "total_records": len(records),
            "venues": dict(sorted(venue_counts.items())),
            "primary_research_objects": dict(sorted(object_counts.items())),
            "preference_labels": {
                label: {
                    "positive": label_positive_counts[label],
                    "negative": sum(
                        1
                        for record in records
                        if record.resolved_negative_tier == "negative"
                        and label in record.resolved_preference_labels
                    ),
                }
                for label in PREFERENCE_LABELS
            },
        }

    def validate_release_dataset(self, records: list[BenchmarkRecord]) -> DatasetValidationSummary:
        label_positive_counts = {
            label: sum(1 for record in records if label in record.resolved_preference_labels)
            for label in PREFERENCE_LABELS
        }
        label_negative_counts = {
            label: sum(
                1
                for record in records
                if record.resolved_negative_tier == "negative"
                and label in record.resolved_preference_labels
            )
            for label in PREFERENCE_LABELS
        }
        duplicates_counter = Counter(record.paper_id for record in records)
        duplicates = sorted([paper_id for paper_id, count in duplicates_counter.items() if count > 1])
        unresolved = sum(1 for record in records if record.resolved_review_status != "final")
        return DatasetValidationSummary(
            total_records=len(records),
            label_positive_counts=label_positive_counts,
            label_negative_counts=label_negative_counts,
            unresolved_conflict_count=unresolved,
            duplicate_paper_ids=duplicates,
        )

    def _to_candidate(self, paper: Paper) -> CandidatePaper:
        primary_research_object = _infer_research_object(paper)
        preference_labels = _infer_preference_labels(paper)
        negative_tier = "positive" if preference_labels else _infer_negative_tier(paper, primary_research_object)
        notes = ""
        if negative_tier == "negative":
            notes = "候选负样本：未命中目标偏好标签，需人工确认。"
        return CandidatePaper(
            paper_id=paper.paper_id,
            title=paper.title,
            abstract=paper.abstract,
            authors=paper.authors,
            venue=paper.venue,
            year=paper.year or 0,
            source=paper.source,
            source_path=paper.source_path,
            primary_research_object=primary_research_object,
            candidate_preference_labels=preference_labels,
            candidate_negative_tier=negative_tier,
            keywords=paper.keywords or paper.tags,
            notes=notes,
        )

    def _score_candidates_for_venue(
        self,
        venue_key: str,
        year: int,
        *,
        minimum_score: int,
    ) -> list[ScoredCandidatePaper]:
        source_path = self.paperlists_root / venue_key / f"{venue_key}{year}.json"
        papers = normalize_records(
            filter_accepted_records(load_raw_records(source_path, venue_key.upper(), year))
        )
        scored: list[ScoredCandidatePaper] = []
        for paper in papers:
            score, matched_terms = _score_inference_acceleration_paper(paper)
            if score < minimum_score:
                continue
            candidate = self._to_candidate(paper)
            note_prefix = f"自动关键词筛选得分={score}"
            if matched_terms:
                note_prefix += f"；命中={', '.join(matched_terms[:8])}"
            merged_notes = note_prefix if not candidate.notes else f"{note_prefix}；{candidate.notes}"
            scored.append(
                ScoredCandidatePaper(
                    candidate=replace(candidate, notes=merged_notes),
                    score=score,
                    matched_terms=matched_terms,
                )
            )
        return sorted(
            scored,
            key=lambda item: (
                -item.score,
                item.candidate.candidate_negative_tier != "positive",
                item.candidate.paper_id,
            ),
        )

    def _select_release_candidates(
        self,
        scored_candidates: list[ScoredCandidatePaper],
        *,
        quota: int,
    ) -> list[ScoredCandidatePaper]:
        positives = [item for item in scored_candidates if item.candidate.candidate_negative_tier == "positive"]
        negatives = [
            item
            for item in scored_candidates
            if item.candidate.candidate_negative_tier == "negative"
        ]

        selected: list[ScoredCandidatePaper] = []
        selected_ids: set[str] = set()

        def take_from(pool: list[ScoredCandidatePaper], limit: int) -> None:
            remaining = limit
            for item in pool:
                if len(selected) >= quota or remaining <= 0:
                    return
                if item.candidate.paper_id in selected_ids:
                    continue
                selected.append(item)
                selected_ids.add(item.candidate.paper_id)
                remaining -= 1

        positive_quota = min(len(positives), max(1, int(quota * 0.7)))
        negative_quota = min(len(negatives), quota - positive_quota)

        take_from(positives, positive_quota)
        take_from(negatives, negative_quota)
        take_from(positives + negatives, quota - len(selected))
        return selected

    def build_scheduling_positive_candidates(
        self,
        venue_targets: tuple[tuple[str, int], ...] = DEFAULT_SCHEDULING_AUGMENT_VENUES,
        *,
        minimum_score: int = 8,
    ) -> list[SchedulingCandidatePaper]:
        selected: list[SchedulingCandidatePaper] = []
        for venue_key, year in venue_targets:
            source_path = self.paperlists_root / venue_key / f"{venue_key}{year}.json"
            papers = normalize_records(
                filter_accepted_records(load_raw_records(source_path, venue_key.upper(), year))
            )
            for paper in papers:
                score, matched_groups = _score_scheduling_paper(paper)
                if score < minimum_score:
                    continue
                candidate = self._to_candidate(paper)
                note_prefix = f"调度专项得分={score}"
                if matched_groups:
                    note_prefix += f"；命中组={', '.join(matched_groups)}"
                merged_notes = note_prefix if not candidate.notes else f"{note_prefix}；{candidate.notes}"
                selected.append(
                    SchedulingCandidatePaper(
                        candidate=replace(candidate, notes=merged_notes),
                        score=score,
                        matched_groups=matched_groups,
                    )
                )
        return sorted(
            selected,
            key=lambda item: (-item.score, item.candidate.paper_id),
        )

def _infer_research_object(paper: Paper) -> str:
    haystack = " ".join(
        [
            paper.title.lower(),
            paper.abstract.lower(),
            paper.primary_area.lower(),
            paper.topic.lower(),
            " ".join(tag.lower() for tag in paper.tags),
        ]
    )
    best_label = "通用机器学习"
    best_score = 0
    for label, keywords in RESEARCH_OBJECT_RULES:
        score = sum(1 for keyword in keywords.split() if keyword in haystack)
        if score > best_score:
            best_label = label
            best_score = score
    if best_score > 0:
        return best_label
    return "通用机器学习"


def _infer_preference_labels(paper: Paper) -> list[str]:
    label_scores = _score_preference_labels(paper)
    if not label_scores:
        return []
    best_score = max(label_scores.values())
    if best_score <= 0:
        return []
    for label in PREFERENCE_LABELS:
        if label_scores.get(label) == best_score:
            return [label]
    return []


def _infer_negative_tier(paper: Paper, primary_research_object: str) -> str:
    haystack = " ".join(
        [paper.title.lower(), paper.abstract.lower(), " ".join(tag.lower() for tag in paper.tags)]
    )
    hard_terms = ("efficient", "fast", "scaling", "cache", "kernel", "compression", "routing")
    if any(token in haystack for token in hard_terms):
        return "negative"
    if primary_research_object in {"LLM", "多模态 / VLM", "AI 系统 / 基础设施"}:
        return "negative"
    return "negative"


def _build_evidence(candidate: CandidatePaper) -> dict[str, list[str]]:
    evidence: dict[str, list[str]] = defaultdict(list)
    abstract = candidate.abstract.strip()
    if abstract:
        first_sentence = abstract.split(".")[0].strip()
        if first_sentence:
            for label in candidate.candidate_preference_labels:
                evidence[label].append(first_sentence)
            if candidate.candidate_negative_tier == "negative":
                evidence["negative"] = [first_sentence]
    return dict(evidence)


def _paper_haystack(paper: Paper) -> str:
    return " ".join(
        [
            paper.title.lower(),
            paper.abstract.lower(),
            paper.primary_area.lower(),
            paper.topic.lower(),
            " ".join(keyword.lower() for keyword in paper.keywords),
            " ".join(tag.lower() for tag in paper.tags),
        ]
    )


def _score_preference_labels(paper: Paper) -> dict[str, int]:
    haystack = _paper_haystack(paper)
    scores = {label: 0 for label in PREFERENCE_LABELS}
    for label, keywords in PREFERENCE_RULES.items():
        scores[label] += sum(2 for keyword in keywords if keyword in haystack)
    scheduling_score, _ = _score_scheduling_paper(paper)
    scores["系统与调度优化"] += scheduling_score
    return scores


def _score_inference_acceleration_paper(paper: Paper) -> tuple[int, tuple[str, ...]]:
    haystack = _paper_haystack(paper)
    score = 0
    matched_terms: list[str] = []

    for term in INFERENCE_ACCELERATION_STRONG_TERMS:
        if term in haystack:
            score += 3
            matched_terms.append(term)
    for term in INFERENCE_ACCELERATION_CONTEXT_TERMS:
        if term in haystack:
            score += 1
            if term not in matched_terms:
                matched_terms.append(term)
    for term in INFERENCE_ACCELERATION_EXCLUDE_TERMS:
        if term in haystack:
            score -= 3

    if any(term in haystack for term in ("llm", "language model", "autoregressive")):
        score += 2
    if any(term in haystack for term in ("inference", "serving", "decoding")):
        score += 2
    return score, tuple(matched_terms)


def _score_scheduling_paper(paper: Paper) -> tuple[int, tuple[str, ...]]:
    haystack = _paper_haystack(paper)
    score = 0
    matched_groups: list[str] = []

    for term in SCHEDULING_EXCLUDE_TERMS:
        if term in haystack:
            score -= 3

    strong_phrase_hits = [phrase for phrase in SCHEDULING_STRONG_PHRASES if phrase in haystack]
    if strong_phrase_hits:
        score += 6 * len(strong_phrase_hits)
        matched_groups.extend(strong_phrase_hits)

    for group_name, terms, group_score in SCHEDULING_TERM_GROUPS:
        hits = [term for term in terms if term in haystack]
        if not hits:
            continue
        if group_name == "multi_tenant" and not any(
            token in haystack for token in ("lora", "llm", "serving", "language model")
        ):
            continue
        if group_name == "pd_disaggregation" and not _contains_all(haystack, ("prefill", "decode")):
            continue
        if group_name == "epd_disaggregation" and not any(
            token in haystack for token in ("multimodal", "encode", "prefill", "decode")
        ):
            continue
        if group_name == "moe_scheduling":
            cooccurrence_hits = sum(
                1 for token in ("expert parallel", "all-to-all", "straggler", "routing") if token in haystack
            )
            if cooccurrence_hits < 2:
                continue
        score += group_score + len(hits)
        matched_groups.append(group_name)

    if "slo" in haystack and any(token in haystack for token in ("serving", "latency", "throughput", "goodput")):
        score += 2
    if "routing" in haystack and any(
        token in haystack for token in ("expert", "request", "token", "scheduler", "serving")
    ):
        score += 2
    if any(token in haystack for token in ("llm", "language model", "multimodal", "inference")):
        score += 2
    return score, tuple(_dedupe_preserve_order(matched_groups))


def _contains_all(haystack: str, terms: tuple[str, ...]) -> bool:
    return all(term in haystack for term in terms)


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result
