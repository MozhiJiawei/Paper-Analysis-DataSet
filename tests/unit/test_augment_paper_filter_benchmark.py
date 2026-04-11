from __future__ import annotations

import json
import shutil
import unittest
from concurrent.futures import Future
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from paper_analysis_dataset.domain.benchmark import AnnotationRecord, BenchmarkRecord, CandidatePaper
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.augment_benchmark import augment_benchmark
from paper_analysis_dataset.services.augmentation_plan import build_scheduling_augmentation_plan
from paper_analysis_dataset.tools.augment_paper_filter_benchmark import _parse_venue_targets


ROOT_DIR = Path(__file__).resolve().parents[2]


class _FakeAnnotator:
    def __init__(self, annotations_by_id: dict[str, AnnotationRecord]) -> None:
        self.annotations_by_id = annotations_by_id
        self.labeler_id = "fake_ai"
        self.seen_paper_ids: list[str] = []

    def submit_annotate(self, candidate: CandidatePaper) -> Future[AnnotationRecord]:
        self.seen_paper_ids.append(candidate.paper_id)
        future: Future[AnnotationRecord] = Future()
        future.set_result(self.annotations_by_id[candidate.paper_id])
        return future


class AugmentPaperFilterBenchmarkTests(unittest.TestCase):
    def test_build_scheduling_plan_reads_current_gap_from_stats(self) -> None:
        plan = build_scheduling_augmentation_plan(
            {
                "by_preference_label": {
                    "系统与调度优化": {"positive": 19, "negative": 0},
                }
            },
            target_positive_count=100,
            venue_priority=("iclr:2026", "nips:2025"),
        )

        self.assertEqual(19, plan.current_positive_count)
        self.assertEqual(81, plan.gap)

    def test_augment_adds_targeted_records_and_reports_pending_human_review(self) -> None:
        benchmark_root = ROOT_DIR / "artifacts" / "test-output" / "augment-benchmark"
        paperlists_root = ROOT_DIR / "artifacts" / "test-output" / "augment-benchmark-paperlists"
        _reset_dir(benchmark_root)
        _reset_dir(paperlists_root)

        _write_paperlists(
            paperlists_root,
            "iclr",
            2026,
            [
                _paper_payload(
                    "iclr26-sched-1",
                    "DistServe Scheduling",
                    "DistServe improves llm serving with request scheduling, continuous batching, and goodput.",
                )
            ],
        )
        _write_paperlists(
            paperlists_root,
            "nips",
            2025,
            [
                _paper_payload(
                    "nips25-sched-1",
                    "Multi-tenant LoRA Serving",
                    "A multi-tenant LoRA serving runtime with gpu multiplexing and load balancing for llm serving.",
                )
            ],
        )

        repository = AnnotationRepository(benchmark_root)
        repository.write_records(
            [_record("existing-positive", "Existing scheduling", "A serving scheduler for llm inference.")]
        )
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="existing-positive",
                    labeler_id="human_reviewer",
                    primary_research_object="AI 系统 / 基础设施",
                    preference_labels=["系统与调度优化"],
                    negative_tier="positive",
                    evidence_spans={"general": ["existing"]},
                    review_status="final",
                )
            ],
            repository.merged_path,
        )
        repository.write_annotations([], repository.annotations_ai_path)
        repository.write_annotations([], repository.annotations_human_path)
        repository.write_conflicts([], repository.conflicts_path)

        annotator = _FakeAnnotator(
            {
                "iclr26-sched-1": _positive_annotation("iclr26-sched-1"),
                "nips25-sched-1": _positive_annotation("nips25-sched-1"),
            }
        )
        stdout = StringIO()
        with redirect_stdout(stdout):
            summary = augment_benchmark(
                paperlists_root=paperlists_root,
                benchmark_root=benchmark_root,
                venue_targets=(("iclr", 2026), ("nips", 2025)),
                target_positive_count=3,
                minimum_score=1,
                annotator=annotator,
                batch_tag="sched",
            )

        self.assertEqual(2, summary["added_records"])
        self.assertEqual(2, summary["annotated_records"])
        self.assertEqual(2, summary["pending_human_review_records"])
        self.assertEqual("target_positive_count_reached", summary["stop_reason"])
        self.assertEqual(
            ["existing-positive", "iclr26-sched-1", "nips25-sched-1"],
            [item.paper_id for item in repository.load_records()],
        )
        self.assertIn("[augment] start", stdout.getvalue())
        self.assertIn("[augment] done", stdout.getvalue())
        self.assertEqual(
            ["iclr26-sched-1", "nips25-sched-1"],
            [item.paper_id for item in repository.load_annotations(repository.annotations_ai_path)],
        )

    def test_augment_respects_venue_priority_when_gap_is_smaller_than_pool(self) -> None:
        benchmark_root = ROOT_DIR / "artifacts" / "test-output" / "augment-priority"
        paperlists_root = ROOT_DIR / "artifacts" / "test-output" / "augment-priority-paperlists"
        _reset_dir(benchmark_root)
        _reset_dir(paperlists_root)

        _write_paperlists(
            paperlists_root,
            "iclr",
            2026,
            [_paper_payload("iclr26-first", "First", "Request scheduling for llm serving and goodput.")],
        )
        _write_paperlists(
            paperlists_root,
            "nips",
            2025,
            [_paper_payload("nips25-second", "Second", "Request scheduling for llm serving and goodput.")],
        )

        repository = AnnotationRepository(benchmark_root)
        repository.write_records([])
        repository.write_annotations([], repository.annotations_ai_path)
        repository.write_annotations([], repository.annotations_human_path)
        repository.write_annotations([], repository.merged_path)
        repository.write_conflicts([], repository.conflicts_path)

        summary = augment_benchmark(
            paperlists_root=paperlists_root,
            benchmark_root=benchmark_root,
            venue_targets=(("iclr", 2026), ("nips", 2025)),
            target_positive_count=1,
            minimum_score=1,
            annotator=_FakeAnnotator(
                {
                    "iclr26-first": _positive_annotation("iclr26-first"),
                    "nips25-second": _positive_annotation("nips25-second"),
                }
            ),
            batch_tag="sched",
        )

        self.assertEqual(["iclr26-first"], summary["selected_paper_ids"])

    def test_augment_skips_ai_negative_and_wrong_label_candidates(self) -> None:
        benchmark_root = ROOT_DIR / "artifacts" / "test-output" / "augment-filtered"
        paperlists_root = ROOT_DIR / "artifacts" / "test-output" / "augment-filtered-paperlists"
        _reset_dir(benchmark_root)
        _reset_dir(paperlists_root)

        _write_paperlists(
            paperlists_root,
            "iclr",
            2026,
            [
                _paper_payload("sched-positive", "Positive", "Request scheduling for llm serving and goodput."),
                _paper_payload("sched-negative", "Negative", "Request scheduling for llm serving and goodput."),
                _paper_payload("sched-other", "Other", "Request scheduling for llm serving and goodput."),
            ],
        )

        repository = AnnotationRepository(benchmark_root)
        repository.write_records([])
        repository.write_annotations([], repository.annotations_ai_path)
        repository.write_annotations([], repository.annotations_human_path)
        repository.write_annotations([], repository.merged_path)
        repository.write_conflicts([], repository.conflicts_path)

        summary = augment_benchmark(
            paperlists_root=paperlists_root,
            benchmark_root=benchmark_root,
            venue_targets=(("iclr", 2026),),
            target_positive_count=2,
            minimum_score=1,
            annotator=_FakeAnnotator(
                {
                    "sched-positive": _positive_annotation("sched-positive"),
                    "sched-negative": _negative_annotation("sched-negative"),
                    "sched-other": _other_positive_annotation("sched-other"),
                }
            ),
            batch_tag="sched",
        )

        self.assertEqual(["sched-positive"], summary["selected_paper_ids"])
        self.assertEqual(["sched-positive"], [item.paper_id for item in repository.load_records()])
        self.assertEqual(["sched-positive"], [item.paper_id for item in repository.load_annotations(repository.annotations_ai_path)])

    def test_parse_venue_targets_supports_cli_format(self) -> None:
        self.assertEqual((("iclr", 2026), ("nips", 2025)), _parse_venue_targets(["iclr:2026", "nips:2025"]))

    def test_augment_can_stop_at_review_limit(self) -> None:
        benchmark_root = ROOT_DIR / "artifacts" / "test-output" / "augment-review-limit"
        paperlists_root = ROOT_DIR / "artifacts" / "test-output" / "augment-review-limit-paperlists"
        _reset_dir(benchmark_root)
        _reset_dir(paperlists_root)

        _write_paperlists(
            paperlists_root,
            "iclr",
            2026,
            [
                _paper_payload("cand-1", "One", "Request scheduling for llm serving and goodput."),
                _paper_payload("cand-2", "Two", "Request scheduling for llm serving and goodput."),
                _paper_payload("cand-3", "Three", "Request scheduling for llm serving and goodput."),
            ],
        )

        repository = AnnotationRepository(benchmark_root)
        repository.write_records([])
        repository.write_annotations([], repository.annotations_ai_path)
        repository.write_annotations([], repository.annotations_human_path)
        repository.write_annotations([], repository.merged_path)
        repository.write_conflicts([], repository.conflicts_path)

        summary = augment_benchmark(
            paperlists_root=paperlists_root,
            benchmark_root=benchmark_root,
            venue_targets=(("iclr", 2026),),
            target_positive_count=3,
            minimum_score=1,
            annotator=_FakeAnnotator(
                {
                    "cand-1": _positive_annotation("cand-1"),
                    "cand-2": _positive_annotation("cand-2"),
                    "cand-3": _positive_annotation("cand-3"),
                }
            ),
            batch_tag="sched",
            review_batch_size=2,
            max_reviewed_candidates=2,
        )

        self.assertEqual("review_limit_reached", summary["stop_reason"])
        self.assertEqual(2, summary["reviewed_candidates"])
        self.assertEqual(2, summary["added_records"])


def _paper_payload(paper_id: str, title: str, abstract: str) -> dict[str, object]:
    return {
        "id": paper_id,
        "title": title,
        "abstract": abstract,
        "status": "Poster",
        "keywords": "continuous batching;serving;scheduling",
        "primary_area": "systems",
        "author": "Alice",
    }


def _record(paper_id: str, title: str, abstract: str) -> BenchmarkRecord:
    return BenchmarkRecord(
        paper_id=paper_id,
        title=title,
        abstract=abstract,
        abstract_zh="",
        authors=["Alice"],
        venue="ICLR 2025",
        year=2025,
        source="conference",
        source_path="records.json",
        primary_research_object="AI 系统 / 基础设施",
        candidate_preference_labels=["系统与调度优化"],
        candidate_negative_tier="positive",
        keywords=[],
        notes="",
    )


def _positive_annotation(paper_id: str) -> AnnotationRecord:
    return AnnotationRecord(
        paper_id=paper_id,
        labeler_id="fake_ai",
        primary_research_object="AI 系统 / 基础设施",
        preference_labels=["系统与调度优化"],
        negative_tier="positive",
        evidence_spans={"general": ["positive"]},
        review_status="pending",
    )


def _negative_annotation(paper_id: str) -> AnnotationRecord:
    return AnnotationRecord(
        paper_id=paper_id,
        labeler_id="fake_ai",
        primary_research_object="AI 系统 / 基础设施",
        preference_labels=[],
        negative_tier="negative",
        evidence_spans={"general": ["negative"]},
        review_status="pending",
    )


def _other_positive_annotation(paper_id: str) -> AnnotationRecord:
    return AnnotationRecord(
        paper_id=paper_id,
        labeler_id="fake_ai",
        primary_research_object="LLM",
        preference_labels=["上下文与缓存优化"],
        negative_tier="positive",
        evidence_spans={"general": ["other"]},
        review_status="pending",
    )


def _write_paperlists(root: Path, venue: str, year: int, payload: list[dict[str, object]]) -> None:
    target = root / venue / f"{venue}{year}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    unittest.main()
