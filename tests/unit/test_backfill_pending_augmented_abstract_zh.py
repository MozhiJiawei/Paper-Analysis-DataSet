from __future__ import annotations

import shutil
import unittest
from concurrent.futures import Future
from pathlib import Path

from paper_analysis_dataset.domain.benchmark import AnnotationRecord, BenchmarkRecord, CandidatePaper
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.tools.backfill_pending_augmented_abstract_zh import (
    backfill_pending_augmented_abstract_zh,
    extract_augment_batch_tag,
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


class BackfillPendingAugmentedAbstractZhTests(unittest.TestCase):
    def test_backfill_only_updates_pending_augmented_records_without_abstract_zh(self) -> None:
        benchmark_root = ROOT_DIR / "artifacts" / "test-output" / "backfill-pending-augment"
        if benchmark_root.exists():
            shutil.rmtree(benchmark_root)
        repository = AnnotationRepository(benchmark_root)
        repository.write_records(
            [
                _record(
                    "pending-augment",
                    notes="augment_batch=20260408-scheduling；augment_score=7",
                    abstract_zh="",
                ),
                _record(
                    "already-translated",
                    notes="augment_batch=20260408-scheduling；augment_score=7",
                    abstract_zh="已有中文摘要",
                ),
                _record(
                    "non-augment",
                    notes="manual_seed=true",
                    abstract_zh="",
                ),
                _record(
                    "already-reviewed",
                    notes="augment_batch=20260408-scheduling；augment_score=7",
                    abstract_zh="",
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
        translator = _FakeTranslator({"pending-augment": "这是待标注增样论文的中文摘要。"})

        summary = backfill_pending_augmented_abstract_zh(
            benchmark_root=benchmark_root,
            translator=translator,
            workers=1,
            checkpoint_every=1,
        )

        loaded = {record.paper_id: record for record in repository.load_records()}
        self.assertEqual(["pending-augment"], translator.seen_paper_ids)
        self.assertEqual("这是待标注增样论文的中文摘要。", loaded["pending-augment"].abstract_zh)
        self.assertEqual("已有中文摘要", loaded["already-translated"].abstract_zh)
        self.assertEqual("", loaded["non-augment"].abstract_zh)
        self.assertEqual("", loaded["already-reviewed"].abstract_zh)
        self.assertEqual(1, summary["updated_records"])
        self.assertEqual(0, summary["remaining_records"])

    def test_backfill_can_filter_specific_batch_tag(self) -> None:
        benchmark_root = ROOT_DIR / "artifacts" / "test-output" / "backfill-pending-augment-batch"
        if benchmark_root.exists():
            shutil.rmtree(benchmark_root)
        repository = AnnotationRepository(benchmark_root)
        repository.write_records(
            [
                _record("batch-a", notes="augment_batch=20260407-scheduling", abstract_zh=""),
                _record("batch-b", notes="augment_batch=20260408-scheduling", abstract_zh=""),
            ]
        )
        repository.write_annotations([], repository.annotations_human_path)
        repository.write_annotations([], repository.merged_path)
        translator = _FakeTranslator(
            {
                "batch-a": "批次 A 中文摘要",
                "batch-b": "批次 B 中文摘要",
            }
        )

        summary = backfill_pending_augmented_abstract_zh(
            benchmark_root=benchmark_root,
            translator=translator,
            workers=1,
            checkpoint_every=1,
            batch_tags=("20260408-scheduling",),
        )

        loaded = {record.paper_id: record for record in repository.load_records()}
        self.assertEqual(["batch-b"], translator.seen_paper_ids)
        self.assertEqual("", loaded["batch-a"].abstract_zh)
        self.assertEqual("批次 B 中文摘要", loaded["batch-b"].abstract_zh)
        self.assertEqual(["20260408-scheduling"], summary["batch_tags"])

    def test_extract_augment_batch_tag_supports_chinese_separator(self) -> None:
        self.assertEqual(
            "20260408-scheduling",
            extract_augment_batch_tag("调度专项得分=7；augment_batch=20260408-scheduling；augment_score=7"),
        )


def _record(
    paper_id: str,
    *,
    notes: str,
    abstract_zh: str,
) -> BenchmarkRecord:
    return BenchmarkRecord(
        paper_id=paper_id,
        title=f"Title {paper_id}",
        title_zh="",
        abstract="An abstract for translation.",
        abstract_zh=abstract_zh,
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
