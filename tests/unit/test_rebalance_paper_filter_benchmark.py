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
from paper_analysis_dataset.services.rebalance_benchmark import (
    annotate_missing_candidates,
    build_title_abstract_fingerprint,
    rebalance_benchmark,
)


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


class RebalancePaperFilterBenchmarkTests(unittest.TestCase):
    def test_annotate_missing_candidates_only_submits_new_records(self) -> None:
        """验证增量 AI 标注只提交缺失标注的论文。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotate-missing-candidates"
        if temp_root.exists():
            shutil.rmtree(temp_root)
        repository = AnnotationRepository(temp_root)
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="existing-paper",
                    labeler_id="codex_cli",
                    primary_research_object="LLM",
                    preference_labels=["解码策略优化"],
                    negative_tier="positive",
                    evidence_spans={"general": ["existing"]},
                    review_status="pending",
                )
            ],
            repository.annotations_ai_path,
        )

        annotator = _FakeAnnotator(
            {
                "new-paper": AnnotationRecord(
                    paper_id="new-paper",
                    labeler_id="fake_ai",
                    primary_research_object="AI 系统 / 基础设施",
                    preference_labels=[],
                    negative_tier="negative",
                    evidence_spans={"general": ["new"]},
                    review_status="pending",
                )
            }
        )

        summary = annotate_missing_candidates(
            repository,
            [
                _candidate("existing-paper", "Existing paper", "Existing abstract."),
                _candidate("new-paper", "New paper", "New abstract."),
            ],
            annotator=annotator,
        )

        self.assertEqual(["new-paper"], annotator.seen_paper_ids)
        self.assertEqual(1, summary["created"])
        self.assertEqual(1, summary["skipped_existing"])
        self.assertEqual(
            ["existing-paper", "new-paper"],
            [item.paper_id for item in repository.load_annotations(repository.annotations_ai_path)],
        )

    def test_rebalance_appends_records_and_stops_when_ai_ratio_reaches_target(self) -> None:
        """验证分批补样后按 AI 层比例停止，并保持其他表不变。"""

        benchmark_root = ROOT_DIR / "artifacts" / "test-output" / "rebalance-stop-at-target"
        paperlists_root = ROOT_DIR / "artifacts" / "test-output" / "rebalance-stop-at-target-paperlists"
        _reset_dir(benchmark_root)
        _reset_dir(paperlists_root)

        _write_paperlists(
            paperlists_root,
            "iclr",
            2026,
            [
                _paper_payload("iclr26-neg-1", "Negative One", "Serving systems paper."),
                _paper_payload("iclr26-neg-2", "Negative Two", "Kernel systems paper."),
            ],
        )
        _write_paperlists(
            paperlists_root,
            "nips",
            2025,
            [
                _paper_payload("nips25-neg-1", "Negative Three", "Cache systems paper."),
            ],
        )

        repository = AnnotationRepository(benchmark_root)
        repository.write_records([_record("existing-paper", "Existing positive", "Positive abstract.")])
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="existing-paper",
                    labeler_id="codex_cli",
                    primary_research_object="LLM",
                    preference_labels=["解码策略优化"],
                    negative_tier="positive",
                    evidence_spans={"general": ["positive"]},
                    review_status="pending",
                )
            ],
            repository.annotations_ai_path,
        )
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="human-paper",
                    labeler_id="human_reviewer",
                    primary_research_object="LLM",
                    preference_labels=[],
                    negative_tier="negative",
                    evidence_spans={"general": ["human"]},
                    review_status="pending",
                )
            ],
            repository.annotations_human_path,
        )
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="merged-paper",
                    labeler_id="merged",
                    primary_research_object="LLM",
                    preference_labels=[],
                    negative_tier="negative",
                    evidence_spans={"general": ["merged"]},
                    review_status="final",
                )
            ],
            repository.merged_path,
        )
        repository.write_conflicts([], repository.conflicts_path)

        human_before = repository.annotations_human_path.read_text(encoding="utf-8")
        merged_before = repository.merged_path.read_text(encoding="utf-8")
        conflicts_before = repository.conflicts_path.read_text(encoding="utf-8")

        annotator = _FakeAnnotator(
            {
                "iclr26-neg-1": _negative_annotation("iclr26-neg-1"),
                "iclr26-neg-2": _negative_annotation("iclr26-neg-2"),
                "nips25-neg-1": _negative_annotation("nips25-neg-1"),
            }
        )

        stdout = StringIO()
        with redirect_stdout(stdout):
            summary = rebalance_benchmark(
                paperlists_root=paperlists_root,
                benchmark_root=benchmark_root,
                target_ai_positive_ratio=0.5,
                batch_size=1,
                seed=7,
                annotator=annotator,
            )

        self.assertEqual(1, summary["added_records"])
        self.assertEqual(1, summary["batches_completed"])
        self.assertEqual("target_ratio_reached", summary["stop_reason"])
        self.assertEqual(0.5, summary["final_ai_positive_ratio"])
        self.assertIn("[rebalance] start", stdout.getvalue())
        self.assertIn("[rebalance] batch=1 start", stdout.getvalue())
        self.assertIn("[rebalance] batch=1 added=1 ratio=0.50", stdout.getvalue())
        self.assertIn("[rebalance] done", stdout.getvalue())
        self.assertEqual(2, len(repository.load_records()))
        self.assertEqual(
            ["existing-paper", annotator.seen_paper_ids[0]],
            [item.paper_id for item in repository.load_annotations(repository.annotations_ai_path)],
        )
        self.assertEqual(human_before, repository.annotations_human_path.read_text(encoding="utf-8"))
        self.assertEqual(merged_before, repository.merged_path.read_text(encoding="utf-8"))
        self.assertEqual(conflicts_before, repository.conflicts_path.read_text(encoding="utf-8"))

    def test_rebalance_dedupes_candidate_pool_by_id_and_fingerprint(self) -> None:
        """验证候选池会去掉重复 paper_id 与同标题同摘要的不同 paper_id。"""

        benchmark_root = ROOT_DIR / "artifacts" / "test-output" / "rebalance-dedupe"
        paperlists_root = ROOT_DIR / "artifacts" / "test-output" / "rebalance-dedupe-paperlists"
        _reset_dir(benchmark_root)
        _reset_dir(paperlists_root)

        _write_paperlists(
            paperlists_root,
            "iclr",
            2026,
            [
                _paper_payload("dup-id", "Same Title", "Same abstract."),
                _paper_payload("dup-id", "Same Title", "Same abstract."),
                _paper_payload("dup-fp-1", "Fingerprint Title", "Fingerprint abstract."),
                _paper_payload("dup-fp-2", "Fingerprint Title", "Fingerprint abstract."),
            ],
        )
        _write_paperlists(
            paperlists_root,
            "nips",
            2025,
            [
                _paper_payload("unique-paper", "Unique Title", "Unique abstract."),
            ],
        )

        repository = AnnotationRepository(benchmark_root)
        repository.write_records([_record("existing-paper", "Existing", "Existing abstract.")])
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="existing-paper",
                    labeler_id="codex_cli",
                    primary_research_object="LLM",
                    preference_labels=["解码策略优化"],
                    negative_tier="positive",
                    evidence_spans={"general": ["existing"]},
                    review_status="pending",
                )
            ],
            repository.annotations_ai_path,
        )

        annotator = _FakeAnnotator(
            {
                "dup-id": _negative_annotation("dup-id"),
                "dup-fp-1": _negative_annotation("dup-fp-1"),
                "unique-paper": _negative_annotation("unique-paper"),
            }
        )

        summary = rebalance_benchmark(
            paperlists_root=paperlists_root,
            benchmark_root=benchmark_root,
            target_ai_positive_ratio=0.0,
            batch_size=10,
            seed=11,
            max_new_records=10,
            annotator=annotator,
        )

        self.assertEqual(3, summary["candidate_pool_size"])
        self.assertEqual(["dup-id"], summary["dedupe_summary"]["skipped_duplicate_ids"])
        self.assertEqual(["dup-fp-2"], summary["dedupe_summary"]["skipped_duplicate_fingerprints"])
        record_ids = [item.paper_id for item in repository.load_records()]
        self.assertEqual(4, len(record_ids))
        self.assertEqual(4, len(set(record_ids)))

    def test_rebalance_skips_existing_fingerprint_and_is_idempotent_on_rerun(self) -> None:
        """验证已存在同标题摘要指纹的论文会被跳过，重复运行不会重复补录。"""

        benchmark_root = ROOT_DIR / "artifacts" / "test-output" / "rebalance-idempotent"
        paperlists_root = ROOT_DIR / "artifacts" / "test-output" / "rebalance-idempotent-paperlists"
        _reset_dir(benchmark_root)
        _reset_dir(paperlists_root)

        _write_paperlists(
            paperlists_root,
            "iclr",
            2026,
            [
                _paper_payload("new-id-same-fingerprint", "Shared Title", "Shared abstract."),
                _paper_payload("unique-id", "Unique Title", "Unique abstract."),
            ],
        )
        _write_paperlists(
            paperlists_root,
            "nips",
            2025,
            [],
        )

        repository = AnnotationRepository(benchmark_root)
        repository.write_records(
            [
                _record("existing-paper", "Shared Title", "Shared abstract."),
                _record("positive-paper", "Positive", "Positive abstract."),
            ]
        )
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="positive-paper",
                    labeler_id="codex_cli",
                    primary_research_object="LLM",
                    preference_labels=["解码策略优化"],
                    negative_tier="positive",
                    evidence_spans={"general": ["positive"]},
                    review_status="pending",
                )
            ],
            repository.annotations_ai_path,
        )

        first_annotator = _FakeAnnotator({"unique-id": _negative_annotation("unique-id")})
        first_summary = rebalance_benchmark(
            paperlists_root=paperlists_root,
            benchmark_root=benchmark_root,
            target_ai_positive_ratio=0.4,
            batch_size=10,
            seed=3,
            annotator=first_annotator,
        )

        second_annotator = _FakeAnnotator({})
        second_summary = rebalance_benchmark(
            paperlists_root=paperlists_root,
            benchmark_root=benchmark_root,
            target_ai_positive_ratio=0.4,
            batch_size=10,
            seed=3,
            annotator=second_annotator,
        )

        self.assertEqual(["new-id-same-fingerprint"], first_summary["dedupe_summary"]["skipped_duplicate_fingerprints"])
        self.assertEqual(1, first_summary["added_records"])
        self.assertEqual(0, second_summary["added_records"])
        self.assertEqual([], second_annotator.seen_paper_ids)
        self.assertEqual(3, len(repository.load_records()))

    def test_rebalance_reports_pool_exhaustion_without_looping(self) -> None:
        """验证候选池耗尽时会给出明确 summary。"""

        benchmark_root = ROOT_DIR / "artifacts" / "test-output" / "rebalance-exhausted"
        paperlists_root = ROOT_DIR / "artifacts" / "test-output" / "rebalance-exhausted-paperlists"
        _reset_dir(benchmark_root)
        _reset_dir(paperlists_root)

        _write_paperlists(
            paperlists_root,
            "iclr",
            2026,
            [_paper_payload("only-neg", "Only Negative", "Only abstract.")],
        )
        _write_paperlists(paperlists_root, "nips", 2025, [])

        repository = AnnotationRepository(benchmark_root)
        repository.write_records([_record("positive-paper", "Positive", "Positive abstract.")])
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="positive-paper",
                    labeler_id="codex_cli",
                    primary_research_object="LLM",
                    preference_labels=["解码策略优化"],
                    negative_tier="positive",
                    evidence_spans={"general": ["positive"]},
                    review_status="pending",
                )
            ],
            repository.annotations_ai_path,
        )

        summary = rebalance_benchmark(
            paperlists_root=paperlists_root,
            benchmark_root=benchmark_root,
            target_ai_positive_ratio=0.1,
            batch_size=1,
            seed=2,
            annotator=_FakeAnnotator({"only-neg": _negative_annotation("only-neg")}),
        )

        self.assertEqual("candidate_pool_exhausted", summary["stop_reason"])
        self.assertEqual(1, summary["added_records"])
        self.assertGreater(summary["final_ai_positive_ratio"], 0.1)

    def test_rebalance_rewrites_root_without_final_fields(self) -> None:
        """验证补样前会先重写根主表，清理历史 final_* 漂移字段。"""

        benchmark_root = ROOT_DIR / "artifacts" / "test-output" / "rebalance-root-rewrite"
        paperlists_root = ROOT_DIR / "artifacts" / "test-output" / "rebalance-root-rewrite-paperlists"
        _reset_dir(benchmark_root)
        _reset_dir(paperlists_root)

        _write_paperlists(paperlists_root, "iclr", 2026, [])
        _write_paperlists(paperlists_root, "nips", 2025, [])

        (benchmark_root / "records.jsonl").parent.mkdir(parents=True, exist_ok=True)
        (benchmark_root / "records.jsonl").write_text(
            json.dumps(
                {
                    "paper_id": "paper-1",
                    "title": "Legacy root row",
                    "abstract": "Legacy abstract.",
                    "abstract_zh": "",
                    "authors": ["Alice"],
                    "venue": "ICLR 2025",
                    "year": 2025,
                    "source": "conference",
                    "source_path": "legacy.json",
                    "primary_research_object": "LLM",
                    "candidate_preference_labels": [],
                    "candidate_negative_tier": "negative",
                    "keywords": [],
                    "notes": "",
                    "final_primary_research_object": "LLM",
                    "final_preference_labels": ["解码策略优化"],
                    "final_negative_tier": "positive",
                    "final_labeler_ids": ["merged"],
                    "final_review_status": "final",
                    "final_evidence_spans": {"general": ["legacy"]},
                },
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )
        (benchmark_root / "annotations-ai.jsonl").write_text("", encoding="utf-8")

        rebalance_benchmark(
            paperlists_root=paperlists_root,
            benchmark_root=benchmark_root,
            target_ai_positive_ratio=1.0,
            batch_size=1,
            seed=1,
            annotator=_FakeAnnotator({}),
        )

        payload = (benchmark_root / "records.jsonl").read_text(encoding="utf-8")
        self.assertNotIn('"final_primary_research_object"', payload)
        self.assertNotIn('"final_preference_labels"', payload)
        self.assertNotIn('"final_negative_tier"', payload)
        self.assertNotIn('"final_labeler_ids"', payload)
        self.assertNotIn('"final_review_status"', payload)
        self.assertNotIn('"final_evidence_spans"', payload)

    def test_fingerprint_normalization_ignores_case_and_punctuation_edges(self) -> None:
        self.assertEqual(
            build_title_abstract_fingerprint(" Hello, World! ", " Test abstract... "),
            build_title_abstract_fingerprint("hello, world", "test abstract"),
        )


def _candidate(paper_id: str, title: str, abstract: str) -> CandidatePaper:
    return CandidatePaper(
        paper_id=paper_id,
        title=title,
        abstract=abstract,
        abstract_zh="",
        authors=["Alice"],
        venue="ICLR 2026",
        year=2026,
        source="conference",
        source_path="tests.json",
        primary_research_object="AI 系统 / 基础设施",
        candidate_preference_labels=[],
        candidate_negative_tier="negative",
        keywords=[],
        notes="",
    )


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
        primary_research_object="LLM",
        candidate_preference_labels=[],
        candidate_negative_tier="negative",
        keywords=[],
        notes="",
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


def _paper_payload(paper_id: str, title: str, abstract: str) -> dict[str, object]:
    return {
        "id": paper_id,
        "title": title,
        "abstract": abstract,
        "status": "Poster",
        "keywords": "systems",
        "primary_area": "systems",
        "author": "Alice",
    }


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
