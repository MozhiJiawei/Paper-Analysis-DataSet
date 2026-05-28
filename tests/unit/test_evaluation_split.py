from __future__ import annotations

import shutil
import unittest
from pathlib import Path

from paper_analysis_dataset.domain.benchmark import AnnotationRecord, BenchmarkRecord
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.evaluation_split import (
    assign_new_merged_papers_to_splits,
    paper_split_map,
)


ROOT_DIR = Path(__file__).resolve().parents[2]


class EvaluationSplitTests(unittest.TestCase):
    def test_assign_new_merged_papers_only_adds_unassigned_human_labeled_records(self) -> None:
        temp_root = ROOT_DIR / "artifacts" / "test-output" / "evaluation-split"
        if temp_root.exists():
            shutil.rmtree(temp_root)
        repository = AnnotationRepository(temp_root)
        repository.write_records(
            [
                _record("paper-old"),
                _record("paper-new"),
                _record("paper-unmerged"),
            ]
        )
        repository.write_annotations(
            [
                _annotation("paper-old"),
                _annotation("paper-new"),
                _annotation("paper-missing-record"),
            ],
            repository.merged_path,
        )
        repository.write_json(
            {
                "version": 1,
                "seed": 42,
                "ratios": {"dev": 0.7, "dev_validation": 0.15, "test": 0.15},
                "splits": {
                    "dev": [],
                    "dev_validation": [],
                    "test": ["paper-old"],
                },
            },
            repository.split_manifest_path,
        )

        summary = assign_new_merged_papers_to_splits(repository)
        split_map = paper_split_map(repository)

        self.assertEqual(1, summary.assigned_count)
        self.assertEqual(2, summary.eligible_count)
        self.assertEqual(1, summary.already_assigned_count)
        self.assertEqual(1, summary.skipped_without_record_count)
        self.assertEqual("test", split_map["paper-old"])
        self.assertIn(split_map["paper-new"], {"dev", "dev_validation", "test"})
        self.assertNotIn("paper-unmerged", split_map)
        self.assertNotIn("paper-missing-record", split_map)

    def test_assign_is_stable_on_repeated_runs(self) -> None:
        temp_root = ROOT_DIR / "artifacts" / "test-output" / "evaluation-split-stable"
        if temp_root.exists():
            shutil.rmtree(temp_root)
        repository = AnnotationRepository(temp_root)
        repository.write_records([_record("paper-stable")])
        repository.write_annotations([_annotation("paper-stable")], repository.merged_path)

        first = assign_new_merged_papers_to_splits(repository)
        first_split = paper_split_map(repository)["paper-stable"]
        second = assign_new_merged_papers_to_splits(repository)

        self.assertEqual(1, first.assigned_count)
        self.assertEqual(0, second.assigned_count)
        self.assertEqual(first_split, paper_split_map(repository)["paper-stable"])


def _record(paper_id: str) -> BenchmarkRecord:
    return BenchmarkRecord(
        paper_id=paper_id,
        title=f"Paper {paper_id}",
        abstract="Abstract.",
        authors=["Alice"],
        venue="ICLR 2025",
        year=2025,
        source="conference",
        source_path="tests.json",
        primary_research_object="LLM",
        candidate_preference_labels=["解码策略优化"],
        candidate_negative_tier="positive",
    )


def _annotation(paper_id: str) -> AnnotationRecord:
    return AnnotationRecord(
        paper_id=paper_id,
        labeler_id="merged",
        primary_research_object="LLM",
        preference_labels=["解码策略优化"],
        negative_tier="positive",
        evidence_spans={"general": ["evidence"]},
        review_status="final",
    )


if __name__ == "__main__":
    unittest.main()
