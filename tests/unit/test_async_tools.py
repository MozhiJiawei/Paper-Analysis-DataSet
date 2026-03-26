from __future__ import annotations

import unittest
from concurrent.futures import Future
from unittest.mock import patch

from paper_analysis_dataset.domain.benchmark import AnnotationRecord, BenchmarkRecord, CandidatePaper
from paper_analysis_dataset.tools.annotate_paper_filter_benchmark import annotate_benchmark
from paper_analysis_dataset.tools.backfill_paper_filter_abstract_zh import backfill_abstract_zh


def _resolved_future(value: object) -> Future[object]:
    future: Future[object] = Future()
    future.set_result(value)
    return future


class AnnotateBenchmarkToolTests(unittest.TestCase):
    def test_annotate_benchmark_regenerates_all_candidates(self) -> None:
        candidate_1 = CandidatePaper(
            paper_id="paper-1",
            title="A",
            abstract="a",
            authors=["Alice"],
            venue="ICLR 2025",
            year=2025,
            source="conference",
            source_path="tests.json",
            primary_research_object="LLM",
        )
        candidate_2 = CandidatePaper(
            paper_id="paper-2",
            title="B",
            abstract="b",
            authors=["Bob"],
            venue="ICLR 2025",
            year=2025,
            source="conference",
            source_path="tests.json",
            primary_research_object="LLM",
        )
        annotation_1 = AnnotationRecord(
            paper_id="paper-1",
            labeler_id="codex_cli",
            primary_research_object="LLM",
            preference_labels=[],
            negative_tier="negative",
        )
        new_annotation = AnnotationRecord(
            paper_id="paper-2",
            labeler_id="codex_cli",
            primary_research_object="LLM",
            preference_labels=["解码策略优化"],
            negative_tier="positive",
            evidence_spans={"解码策略优化": ["beam search"]},
        )
        writes: list[list[AnnotationRecord]] = []
        submitted: list[str] = []

        class FakeRepository:
            annotations_ai_path = "annotations-ai.jsonl"

            def load_candidates(self) -> list[CandidatePaper]:
                return [candidate_1, candidate_2]

            def load_annotations(self, _path: str) -> list[AnnotationRecord]:
                return [annotation_1]

            def write_annotations(self, annotations: list[AnnotationRecord], _path: str) -> None:
                writes.append(list(annotations))

        class FakeAnnotator:
            labeler_id = "codex_cli"

            def submit_annotate(self, candidate: CandidatePaper) -> Future[AnnotationRecord]:
                submitted.append(candidate.paper_id)
                if candidate.paper_id == "paper-1":
                    return _resolved_future(annotation_1)
                return _resolved_future(new_annotation)

        with patch("paper_analysis_dataset.tools.annotate_paper_filter_benchmark.AnnotationRepository", return_value=FakeRepository()):
            with patch("paper_analysis_dataset.tools.annotate_paper_filter_benchmark.resolve_annotation_backend", return_value="codex_cli"):
                with patch("paper_analysis_dataset.tools.annotate_paper_filter_benchmark.build_annotator", return_value=FakeAnnotator()) as build_annotator:
                    summary = annotate_benchmark(concurrency=3)

        self.assertEqual(["paper-1", "paper-2"], submitted)
        self.assertEqual(3, len(writes))
        self.assertEqual([], writes[0])
        self.assertEqual(1, len(writes[1]))
        self.assertIn(writes[1][0].paper_id, {"paper-1", "paper-2"})
        self.assertEqual({"paper-1", "paper-2"}, {item.paper_id for item in writes[2]})
        self.assertEqual(2, summary["annotations_ai"])
        self.assertEqual(3, summary["concurrency"])
        build_annotator.assert_called_once_with("codex_cli", concurrency=3)


class BackfillAbstractToolTests(unittest.TestCase):
    def test_backfill_uses_async_translator_and_checkpoint(self) -> None:
        record_1 = BenchmarkRecord(
            paper_id="paper-1",
            title="A",
            abstract="a",
            abstract_zh="",
            authors=["Alice"],
            venue="ICLR 2025",
            year=2025,
            source="conference",
            source_path="tests.json",
            primary_research_object="LLM",
        )
        record_2 = BenchmarkRecord(
            paper_id="paper-2",
            title="B",
            abstract="b",
            abstract_zh="中文摘要：B",
            authors=["Bob"],
            venue="ICLR 2025",
            year=2025,
            source="conference",
            source_path="tests.json",
            primary_research_object="LLM",
        )
        writes: list[list[BenchmarkRecord]] = []
        translator_concurrency: list[int] = []
        submitted: list[str] = []

        class FakeRepository:
            def load_records(self) -> list[BenchmarkRecord]:
                return [record_1, record_2]

            def write_records(self, records: list[BenchmarkRecord]) -> None:
                writes.append(list(records))

        class FakeTranslator:
            def __init__(self, *, concurrency: int) -> None:
                translator_concurrency.append(concurrency)

            def submit_translate(self, candidate: CandidatePaper) -> Future[str]:
                submitted.append(candidate.paper_id)
                return _resolved_future(f"译文：{candidate.title}")

        with patch("paper_analysis_dataset.tools.backfill_paper_filter_abstract_zh.AnnotationRepository", return_value=FakeRepository()):
            with patch("paper_analysis_dataset.tools.backfill_paper_filter_abstract_zh.DoubaoAbstractTranslator", FakeTranslator):
                summary = backfill_abstract_zh(workers=2, checkpoint_every=1)

        self.assertEqual([2], translator_concurrency)
        self.assertEqual(["paper-1", "paper-2"], submitted)
        self.assertGreaterEqual(len(writes), 2)
        self.assertEqual("译文：A", writes[-1][0].abstract_zh)
        self.assertEqual("译文：B", writes[-1][1].abstract_zh)
        self.assertEqual(2, summary["updated_records"])
        self.assertEqual(0, summary["remaining_records"])


if __name__ == "__main__":
    unittest.main()
