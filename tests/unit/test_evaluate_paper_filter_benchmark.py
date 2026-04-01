from __future__ import annotations

import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from paper_analysis_dataset.domain.benchmark import AnnotationRecord, BenchmarkRecord
from paper_analysis_dataset.tools.evaluate_paper_filter_benchmark import evaluate_benchmark


class EvaluateBenchmarkLoggingTests(unittest.TestCase):
    def test_evaluate_benchmark_prints_start_progress_and_done(self) -> None:
        truth = AnnotationRecord(
            paper_id="paper-1",
            labeler_id="merged",
            primary_research_object="LLM",
            preference_labels=[],
            negative_tier="negative",
            evidence_spans={"negative": ["benchmark"]},
            review_status="final",
        )
        record = BenchmarkRecord(
            paper_id="paper-1",
            title="Paper",
            abstract="Abstract",
            abstract_zh="摘要",
            authors=["Alice"],
            venue="ICLR 2025",
            year=2025,
            source="conference",
            source_path="tests.json",
            primary_research_object="LLM",
            candidate_preference_labels=[],
            candidate_negative_tier="negative",
            keywords=[],
            notes="",
        )

        class FakeRepository:
            merged_path = "merged.jsonl"

            def load_record_map(self) -> dict[str, BenchmarkRecord]:
                return {"paper-1": record}

            def load_annotations(self, _path: str) -> list[AnnotationRecord]:
                return [truth]

        class FakeClient:
            batch_sizes: list[int]

            def __init__(self) -> None:
                self.batch_sizes = []

            def annotate_many(
                self,
                items: list[tuple[object, str]],
            ) -> list[AnnotationRecord]:
                self.batch_sizes.append(len(items))
                return [
                    AnnotationRecord(
                        paper_id=request_id,
                        labeler_id="evaluation_api",
                        primary_research_object="LLM",
                        preference_labels=[],
                        negative_tier="negative",
                        evidence_spans={"negative": ["benchmark"]},
                        review_status="pending",
                    )
                    for _candidate, request_id in items
                ]

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            stdout = StringIO()
            fake_client = FakeClient()
            with redirect_stdout(stdout):
                with patch(
                    "paper_analysis_dataset.tools.evaluate_paper_filter_benchmark.AnnotationRepository",
                    return_value=FakeRepository(),
                ):
                    with patch(
                        "paper_analysis_dataset.tools.evaluate_paper_filter_benchmark.EvaluationApiClient",
                        return_value=fake_client,
                    ):
                        summary = evaluate_benchmark(
                            base_url="http://127.0.0.1:8765",
                            output_dir=output_dir,
                        )

        self.assertTrue(summary["ok"])
        self.assertEqual([1], fake_client.batch_sizes)
        self.assertIn("[evaluate] start", stdout.getvalue())
        self.assertIn("[evaluate] 1/1 errors=0 protocol_errors=0 paper_id=paper-1", stdout.getvalue())
        self.assertIn("[evaluate] done", stdout.getvalue())

    def test_evaluate_benchmark_uses_default_batch_size_50(self) -> None:
        truths = [
            AnnotationRecord(
                paper_id=f"paper-{index}",
                labeler_id="merged",
                primary_research_object="LLM",
                preference_labels=[],
                negative_tier="negative",
                evidence_spans={"negative": ["benchmark"]},
                review_status="final",
            )
            for index in range(1, 56)
        ]
        records = {
            truth.paper_id: BenchmarkRecord(
                paper_id=truth.paper_id,
                title=f"Paper {truth.paper_id}",
                abstract="Abstract",
                abstract_zh="摘要",
                authors=["Alice"],
                venue="ICLR 2025",
                year=2025,
                source="conference",
                source_path="tests.json",
                primary_research_object="LLM",
                candidate_preference_labels=[],
                candidate_negative_tier="negative",
                keywords=[],
                notes="",
            )
            for truth in truths
        }

        class FakeRepository:
            merged_path = "merged.jsonl"

            def load_record_map(self) -> dict[str, BenchmarkRecord]:
                return records

            def load_annotations(self, _path: str) -> list[AnnotationRecord]:
                return truths

        class FakeClient:
            def __init__(self) -> None:
                self.batch_sizes: list[int] = []

            def annotate_many(
                self,
                items: list[tuple[object, str]],
            ) -> list[AnnotationRecord]:
                self.batch_sizes.append(len(items))
                return [
                    AnnotationRecord(
                        paper_id=request_id,
                        labeler_id="evaluation_api",
                        primary_research_object="LLM",
                        preference_labels=[],
                        negative_tier="negative",
                        evidence_spans={"negative": ["benchmark"]},
                        review_status="pending",
                    )
                    for _candidate, request_id in items
                ]

        with tempfile.TemporaryDirectory() as temp_dir:
            fake_client = FakeClient()
            with patch(
                "paper_analysis_dataset.tools.evaluate_paper_filter_benchmark.AnnotationRepository",
                return_value=FakeRepository(),
            ):
                with patch(
                    "paper_analysis_dataset.tools.evaluate_paper_filter_benchmark.EvaluationApiClient",
                    return_value=fake_client,
                ):
                    summary = evaluate_benchmark(
                        base_url="http://127.0.0.1:8765",
                        output_dir=Path(temp_dir),
                    )

        self.assertTrue(summary["ok"])
        self.assertEqual([50, 5], fake_client.batch_sizes)


if __name__ == "__main__":
    unittest.main()
