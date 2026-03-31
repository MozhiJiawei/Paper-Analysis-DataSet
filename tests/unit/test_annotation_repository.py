from __future__ import annotations

import shutil
import unittest
from pathlib import Path

from paper_analysis_dataset.domain.benchmark import AnnotationRecord, BenchmarkRecord
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository


ROOT_DIR = Path(__file__).resolve().parents[2]


class AnnotationRepositoryTests(unittest.TestCase):
    def test_upsert_annotation_writes_valid_jsonl(self) -> None:
        """验证 repository 可以写入并读取人工标注。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-repository"
        if temp_root.exists():
            shutil.rmtree(temp_root)
        repository = AnnotationRepository(temp_root)

        annotation = AnnotationRecord(
            paper_id="paper-1",
            labeler_id="human_reviewer",
            primary_research_object="LLM",
            preference_labels=["解码策略优化"],
            negative_tier="positive",
            evidence_spans={"general": ["evidence"]},
            review_status="pending",
        )

        repository.upsert_annotation(annotation, repository.annotations_human_path)
        loaded = repository.load_annotations(repository.annotations_human_path)

        self.assertEqual(1, len(loaded))
        self.assertEqual("paper-1", loaded[0].paper_id)

    def test_upsert_annotations_preserves_existing_ai_records(self) -> None:
        """验证批量 upsert AI 标注时不会清空已有记录。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-repository-upsert-many"
        if temp_root.exists():
            shutil.rmtree(temp_root)
        repository = AnnotationRepository(temp_root)

        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="paper-1",
                    labeler_id="codex_cli",
                    primary_research_object="LLM",
                    preference_labels=["解码策略优化"],
                    negative_tier="positive",
                    evidence_spans={"general": ["old"]},
                    review_status="pending",
                )
            ],
            repository.annotations_ai_path,
        )

        repository.upsert_annotations(
            [
                AnnotationRecord(
                    paper_id="paper-2",
                    labeler_id="codex_cli",
                    primary_research_object="AI 系统 / 基础设施",
                    preference_labels=[],
                    negative_tier="negative",
                    evidence_spans={"general": ["new"]},
                    review_status="pending",
                )
            ],
            repository.annotations_ai_path,
        )

        loaded = repository.load_annotations(repository.annotations_ai_path)
        self.assertEqual(["paper-1", "paper-2"], [item.paper_id for item in loaded])

    def test_invalid_annotation_is_rejected_before_write(self) -> None:
        """验证非法标签不会被写入磁盘。"""

        with self.assertRaises(ValueError):
            AnnotationRecord(
                paper_id="paper-2",
                labeler_id="human_reviewer",
                primary_research_object="未知对象",
                preference_labels=[],
                negative_tier="easy",
                evidence_spans={},
                review_status="pending",
            )

    def test_load_annotations_rejects_legacy_negative_tier(self) -> None:
        """验证读取旧标注值时会直接报错，而不是继续兼容。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-repository-legacy"
        if temp_root.exists():
            shutil.rmtree(temp_root)
        repository = AnnotationRepository(temp_root)
        repository.annotations_human_path.parent.mkdir(parents=True, exist_ok=True)
        repository.annotations_human_path.write_text(
            '{"paper_id":"paper-legacy","labeler_id":"human_reviewer","primary_research_object":"LLM","preference_labels":["解码策略优化"],"negative_tier":"hard","evidence_spans":{},"notes":"","review_status":"pending"}\n',
            encoding="utf-8",
        )

        with self.assertRaises(ValueError):
            repository.load_annotations(repository.annotations_human_path)

    def test_write_records_strips_final_annotation_fields_from_root_table(self) -> None:
        """验证根主表写入时不会混入 final 聚合标注字段。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-repository-records"
        if temp_root.exists():
            shutil.rmtree(temp_root)
        repository = AnnotationRepository(temp_root)

        repository.write_records(
            [
                BenchmarkRecord(
                    paper_id="paper-1",
                    title="Root Table Record",
                    title_zh="根主表记录",
                    abstract="A paper about inference.",
                    abstract_zh="一篇关于推理的论文。",
                    authors=["Alice"],
                    venue="ICLR 2025",
                    year=2025,
                    source="conference",
                    source_path="tests.json",
                    primary_research_object="LLM",
                    candidate_preference_labels=["解码策略优化"],
                    candidate_negative_tier="positive",
                    final_primary_research_object="LLM",
                    final_preference_labels=["解码策略优化"],
                    final_negative_tier="positive",
                    final_labeler_ids=["merged"],
                    final_review_status="final",
                    final_evidence_spans={"解码策略优化": ["evidence"]},
                )
            ]
        )

        payload = repository.records_path.read_text(encoding="utf-8")
        self.assertNotIn('"target_preference_labels"', payload)
        self.assertNotIn('"final_primary_research_object"', payload)
        self.assertNotIn('"final_preference_labels"', payload)
        self.assertNotIn('"final_negative_tier"', payload)
        self.assertNotIn('"final_labeler_ids"', payload)
        self.assertNotIn('"final_review_status"', payload)
        self.assertNotIn('"final_evidence_spans"', payload)
        self.assertIn('"title_zh"', payload)
        self.assertIn('"abstract_zh"', payload)


if __name__ == "__main__":
    unittest.main()
