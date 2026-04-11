from __future__ import annotations

import shutil
import unittest
from concurrent.futures import Future
from pathlib import Path

from paper_analysis_dataset.domain.benchmark import AnnotationRecord, BenchmarkRecord, CandidatePaper
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.tools.backfill_pending_augmented_title_zh import (
    backfill_pending_augmented_title_zh,
)


ROOT_DIR = Path(__file__).resolve().parents[2]


class _FakeTranslator:
    def __init__(self, translations: dict[str, str]) -> None:
        self.translations = translations
        self.seen_paper_ids: list[str] = []

    def submit_translate(self, candidate: CandidatePaper) -> Future[str]:
        self.seen_paper_ids.append(candidate.paper_id)
        future: Future[str] = Future()
        future.set_result(self.translations[candidate.paper_id])
        return future


class BackfillPendingAugmentedTitleZhTests(unittest.TestCase):
    def test_backfill_only_updates_pending_augmented_records_without_title_zh(self) -> None:
        benchmark_root = ROOT_DIR / "artifacts" / "test-output" / "backfill-pending-augment-title"
        if benchmark_root.exists():
            shutil.rmtree(benchmark_root)
        repository = AnnotationRepository(benchmark_root)
        repository.write_records(
            [
                _record(
                    "pending-augment",
                    notes="augment_batch=20260408-scheduling；augment_score=7",
                    title_zh="",
                ),
                _record(
                    "already-translated",
                    notes="augment_batch=20260408-scheduling；augment_score=7",
                    title_zh="已有中文标题",
                ),
                _record(
                    "non-augment",
                    notes="manual_seed=true",
                    title_zh="",
                ),
                _record(
                    "already-reviewed",
                    notes="augment_batch=20260408-scheduling；augment_score=7",
                    title_zh="",
                ),
            ]
        )
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="already-reviewed",
                    labeler_id="human_reviewer",
                    primary_research_object="LLM",
                    preference_labels=["系统与调度优化"],
                    negative_tier="positive",
                    evidence_spans={"general": ["done"]},
                    review_status="pending",
                )
            ],
            repository.annotations_human_path,
        )
        repository.write_annotations([], repository.merged_path)
        translator = _FakeTranslator({"pending-augment": "待标注增样论文的中文标题"})

        summary = backfill_pending_augmented_title_zh(
            benchmark_root=benchmark_root,
            translator=translator,
            workers=1,
            checkpoint_every=1,
        )

        loaded = {record.paper_id: record for record in repository.load_records()}
        self.assertEqual(["pending-augment"], translator.seen_paper_ids)
        self.assertEqual("待标注增样论文的中文标题", loaded["pending-augment"].title_zh)
        self.assertEqual("已有中文标题", loaded["already-translated"].title_zh)
        self.assertEqual("", loaded["non-augment"].title_zh)
        self.assertEqual("", loaded["already-reviewed"].title_zh)
        self.assertEqual(1, summary["updated_records"])
        self.assertEqual(0, summary["remaining_records"])

    def test_backfill_can_filter_specific_batch_tag(self) -> None:
        benchmark_root = ROOT_DIR / "artifacts" / "test-output" / "backfill-pending-augment-title-batch"
        if benchmark_root.exists():
            shutil.rmtree(benchmark_root)
        repository = AnnotationRepository(benchmark_root)
        repository.write_records(
            [
                _record("batch-a", notes="augment_batch=20260407-scheduling", title_zh=""),
                _record("batch-b", notes="augment_batch=20260408-scheduling", title_zh=""),
            ]
        )
        repository.write_annotations([], repository.annotations_human_path)
        repository.write_annotations([], repository.merged_path)
        translator = _FakeTranslator(
            {
                "batch-a": "批次 A 中文标题",
                "batch-b": "批次 B 中文标题",
            }
        )

        summary = backfill_pending_augmented_title_zh(
            benchmark_root=benchmark_root,
            translator=translator,
            workers=1,
            checkpoint_every=1,
            batch_tags=("20260408-scheduling",),
        )

        loaded = {record.paper_id: record for record in repository.load_records()}
        self.assertEqual(["batch-b"], translator.seen_paper_ids)
        self.assertEqual("", loaded["batch-a"].title_zh)
        self.assertEqual("批次 B 中文标题", loaded["batch-b"].title_zh)
        self.assertEqual(["20260408-scheduling"], summary["batch_tags"])


def _record(
    paper_id: str,
    *,
    notes: str,
    title_zh: str,
) -> BenchmarkRecord:
    return BenchmarkRecord(
        paper_id=paper_id,
        title=f"Title {paper_id}",
        title_zh=title_zh,
        abstract="An abstract for translation.",
        abstract_zh="已有中文摘要",
        authors=["Alice"],
        venue="ICLR 2025",
        year=2025,
        source="conference",
        source_path="paperlists/iclr2025.json",
        primary_research_object="LLM",
        candidate_preference_labels=["系统与调度优化"],
        candidate_negative_tier="positive",
        notes=notes,
    )


if __name__ == "__main__":
    unittest.main()
