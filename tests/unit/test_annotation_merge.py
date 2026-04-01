from __future__ import annotations

import unittest

from paper_analysis_dataset.domain.benchmark import AnnotationRecord, BenchmarkRecord
from paper_analysis_dataset.services.annotation_merge import merge_annotations


def _record() -> BenchmarkRecord:
    return BenchmarkRecord(
        paper_id="paper-1",
        title="Test Paper",
        abstract="Test abstract.",
        authors=["Alice"],
        venue="ICLR 2025",
        year=2025,
        source="conference",
        source_path="tests/fixtures/paperlists_repo/iclr/iclr2025.json",
        primary_research_object="LLM",
        candidate_preference_labels=["解码策略优化"],
        candidate_negative_tier="positive",
    )


def _annotation(
    *,
    labeler_id: str,
    primary_research_object: str = "LLM",
    preference_labels: list[str] | None = None,
    negative_tier: str = "positive",
) -> AnnotationRecord:
    return AnnotationRecord(
        paper_id="paper-1",
        labeler_id=labeler_id,
        primary_research_object=primary_research_object,
        preference_labels=preference_labels or ["解码策略优化"],
        negative_tier=negative_tier,
        evidence_spans={"general": ["evidence"]},
        review_status="pending",
    )


class AnnotationMergeTests(unittest.TestCase):
    def test_merge_annotations_creates_merged_annotation_when_two_labelers_agree(self) -> None:
        """验证 AI 与人工一致时会直接产出 merged 结果。"""

        result = merge_annotations(
            [_record()],
            [_annotation(labeler_id="codex_cli")],
            [_annotation(labeler_id="human_reviewer")],
        )

        self.assertEqual(1, len(result.records))
        self.assertEqual(1, len(result.merged_annotations))
        self.assertEqual([], result.conflicts)
        self.assertEqual("final", result.records[0].resolved_review_status)
        self.assertEqual(["解码策略优化"], result.merged_annotations[0].preference_labels)

    def test_merge_annotations_surfaces_conflict_without_merged_output(self) -> None:
        """验证冲突未仲裁时不会写入 merged。"""

        result = merge_annotations(
            [_record()],
            [_annotation(labeler_id="codex_cli", preference_labels=["解码策略优化"])],
            [_annotation(labeler_id="human_reviewer", preference_labels=["模型压缩"])],
        )

        self.assertEqual(1, len(result.records))
        self.assertEqual(0, len(result.merged_annotations))
        self.assertEqual(1, len(result.conflicts))
        self.assertIn("preference_labels", result.conflicts[0].conflicting_fields)
        self.assertEqual("conflict", result.records[0].resolved_review_status)

    def test_negative_samples_skip_conflict_review_and_merge_human_result(self) -> None:
        """验证负样本抽检修改后直接进入 merged，不进入冲突。"""

        negative_record = BenchmarkRecord(
            paper_id="paper-1",
            title="Test Paper",
            abstract="Test abstract.",
            authors=["Alice"],
            venue="ICLR 2025",
            year=2025,
            source="conference",
            source_path="tests/fixtures/paperlists_repo/iclr/iclr2025.json",
            primary_research_object="LLM",
            candidate_preference_labels=[],
            candidate_negative_tier="negative",
        )

        result = merge_annotations(
            [negative_record],
            [_annotation(labeler_id="codex_cli", negative_tier="negative", preference_labels=[])],
            [
                _annotation(
                    labeler_id="human_reviewer",
                    primary_research_object="AI 系统 / 基础设施",
                    negative_tier="positive",
                    preference_labels=["模型压缩"],
                )
            ],
        )

        self.assertEqual([], result.conflicts)
        self.assertEqual(1, len(result.merged_annotations))
        self.assertEqual("final", result.records[0].resolved_review_status)
        self.assertEqual("positive", result.records[0].resolved_negative_tier)
        self.assertEqual(["模型压缩"], result.merged_annotations[0].preference_labels)


if __name__ == "__main__":
    unittest.main()
