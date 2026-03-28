from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

from paper_analysis_dataset.domain.benchmark import AnnotationRecord
from paper_analysis_dataset.services.evaluation_reporter import (
    build_evaluation_report,
    write_evaluation_artifacts,
)


ROOT_DIR = Path(__file__).resolve().parents[2]


class EvaluationReporterTests(unittest.TestCase):
    def test_report_contains_aggregated_metrics_only(self) -> None:
        truths = [
            AnnotationRecord(
                paper_id="paper-1",
                labeler_id="merged",
                primary_research_object="LLM",
                preference_labels=["解码策略优化"],
                negative_tier="positive",
                review_status="final",
            ),
            AnnotationRecord(
                paper_id="paper-2",
                labeler_id="merged",
                primary_research_object="评测 / Benchmark / 数据集",
                preference_labels=[],
                negative_tier="negative",
                review_status="final",
            ),
        ]
        predictions = [
            AnnotationRecord(
                paper_id="paper-1",
                labeler_id="evaluation_api",
                primary_research_object="LLM",
                preference_labels=["解码策略优化"],
                negative_tier="positive",
            ),
            AnnotationRecord(
                paper_id="paper-2",
                labeler_id="evaluation_api",
                primary_research_object="评测 / Benchmark / 数据集",
                preference_labels=[],
                negative_tier="negative",
            ),
        ]
        report = build_evaluation_report(
            truths=truths,
            predictions=predictions,
            request_error_count=0,
            protocol_error_count=0,
        )
        temp_dir = ROOT_DIR / "artifacts" / "test-output" / "evaluation-reporter"
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        artifacts = write_evaluation_artifacts(temp_dir, report)

        self.assertEqual(1.0, report["overall"]["accuracy"])
        payload = json.loads(artifacts["report"].read_text(encoding="utf-8"))
        summary = artifacts["summary"].read_text(encoding="utf-8")
        serialized = json.dumps(payload, ensure_ascii=False) + "\n" + summary
        self.assertNotIn("paper-1", serialized)
        self.assertNotIn("paper-2", serialized)
        self.assertNotIn("title", serialized.lower())
        self.assertNotIn("abstract", serialized.lower())


if __name__ == "__main__":
    unittest.main()
