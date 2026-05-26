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
    def test_report_uses_positive_only_metrics_for_preference_and_research_object(self) -> None:
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
                primary_research_object="多模态 / VLM",
                preference_labels=["系统与调度优化"],
                negative_tier="positive",
                review_status="final",
            ),
            AnnotationRecord(
                paper_id="paper-3",
                labeler_id="merged",
                primary_research_object="Diffusion / 生成模型",
                preference_labels=["模型压缩"],
                negative_tier="positive",
                review_status="final",
            ),
            AnnotationRecord(
                paper_id="paper-4",
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
                primary_research_object="LLM",
                preference_labels=[],
                negative_tier="negative",
            ),
            AnnotationRecord(
                paper_id="paper-3",
                labeler_id="evaluation_api",
                primary_research_object="Diffusion / 生成模型",
                preference_labels=["模型压缩"],
                negative_tier="positive",
            ),
            AnnotationRecord(
                paper_id="paper-4",
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

        self.assertEqual(0.75, report["overall"]["accuracy"])
        self.assertEqual(4, report["positive_negative"]["total_count"])
        self.assertEqual(2, report["positive_negative"]["tp"])
        self.assertEqual(0, report["positive_negative"]["fp"])
        self.assertEqual(1, report["positive_negative"]["fn"])
        self.assertEqual(1, report["positive_negative"]["tn"])
        self.assertEqual(3, report["positive_preference_label_overall"]["support"])
        self.assertEqual(3, report["positive_preference_label_overall"]["total_count"])
        self.assertEqual(2, report["positive_preference_label_overall"]["correct_count"])
        self.assertEqual(1, report["positive_preference_label_overall"]["incorrect_count"])
        self.assertEqual(3, report["positive_primary_research_object_overall"]["support"])
        self.assertEqual(3, report["positive_primary_research_object_overall"]["total_count"])
        self.assertEqual(2, report["positive_primary_research_object_overall"]["correct_count"])
        self.assertEqual(1, report["positive_primary_research_object_overall"]["incorrect_count"])
        self.assertEqual(3, report["by_preference_label"]["解码策略优化"]["total_count"])
        self.assertEqual(1, report["by_preference_label"]["解码策略优化"]["predicted_count"])
        self.assertEqual(1.0, report["by_primary_research_object"]["Diffusion"]["f1"])
        self.assertEqual(1, report["by_primary_research_object"]["Diffusion"]["tp"])
        self.assertEqual(0.0, report["by_primary_research_object"]["VLM"]["recall"])
        self.assertEqual(3, report["by_primary_research_object"]["其他"]["total_count"])
        payload = json.loads(artifacts["report"].read_text(encoding="utf-8"))
        summary = artifacts["summary"].read_text(encoding="utf-8")
        serialized = json.dumps(payload, ensure_ascii=False) + "\n" + summary
        self.assertIn("负样本未标注研究对象和研究子类", summary)
        self.assertIn("LLM / VLM / Diffusion / 其他 四桶", summary)
        self.assertIn("tp=2, fp=0, fn=1, tn=1", summary)
        self.assertIn("total=3, correct=2, incorrect=1", summary)
        self.assertNotIn("paper-1", serialized)
        self.assertNotIn("paper-2", serialized)
        self.assertNotIn("paper-3", serialized)
        self.assertNotIn("paper-4", serialized)
        self.assertNotIn("title", serialized.lower())
        self.assertNotIn("abstract", serialized.lower())


if __name__ == "__main__":
    unittest.main()
