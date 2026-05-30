from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.benchmark_importer import (
    BenchmarkImportError,
    import_benchmark_json,
)


class BenchmarkImporterTests(unittest.TestCase):
    def test_import_positive_record_and_ai_annotation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            benchmark_root = Path(temp_dir) / "benchmark"
            input_json = Path(temp_dir) / "payload.json"
            _write_payload(
                input_json,
                {
                    "source_batch": "arxiv-review-2026-05-28",
                    "records": [_record("paper-1", notes="recommender=positive; ai_review=accepted")],
                    "annotations_ai": [
                        _annotation(
                            "paper-1",
                            negative_tier="positive",
                            preference_labels=["解码策略优化"],
                            notes="AI agreed with recommender positive.",
                        )
                    ],
                },
            )

            summary = import_benchmark_json(input_json, benchmark_root=benchmark_root)
            repository = AnnotationRepository(benchmark_root)
            records = repository.load_records()
            annotations = repository.load_annotations(repository.annotations_ai_path)

        self.assertEqual(1, summary.records_added)
        self.assertEqual(1, summary.ai_annotations_added)
        self.assertEqual(1, summary.ai_positive_count)
        self.assertEqual("paper-1", records[0].paper_id)
        self.assertIn("recommender=positive", records[0].notes)
        self.assertEqual("positive", annotations[0].negative_tier)
        self.assertEqual(["解码策略优化"], annotations[0].preference_labels)

    def test_import_negative_ai_annotation_preserves_notes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            benchmark_root = Path(temp_dir) / "benchmark"
            input_json = Path(temp_dir) / "payload.json"
            _write_payload(
                input_json,
                {
                    "records": [
                        _record(
                            "paper-2",
                            notes="recommender=positive; ai_review=likely_false_positive",
                        )
                    ],
                    "annotations_ai": [
                        _annotation(
                            "paper-2",
                            negative_tier="negative",
                            preference_labels=["解码策略优化"],
                            notes="AI marked this as suspected false positive.",
                        )
                    ],
                },
            )

            summary = import_benchmark_json(input_json, benchmark_root=benchmark_root)
            repository = AnnotationRepository(benchmark_root)
            annotations = repository.load_annotations(repository.annotations_ai_path)

        self.assertEqual(1, summary.ai_negative_count)
        self.assertEqual([], annotations[0].preference_labels)
        self.assertIn("suspected false positive", annotations[0].notes)

    def test_import_records_without_annotations_writes_only_records(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            benchmark_root = Path(temp_dir) / "benchmark"
            input_json = Path(temp_dir) / "payload.json"
            _write_payload(input_json, {"records": [_record("paper-3")], "annotations_ai": []})

            summary = import_benchmark_json(input_json, benchmark_root=benchmark_root)
            repository = AnnotationRepository(benchmark_root)

        self.assertEqual(1, summary.records_added)
        self.assertFalse(repository.annotations_ai_path.exists())

    def test_repeated_import_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            benchmark_root = Path(temp_dir) / "benchmark"
            input_json = Path(temp_dir) / "payload.json"
            _write_payload(
                input_json,
                {
                    "records": [_record("paper-4")],
                    "annotations_ai": [_annotation("paper-4", negative_tier="negative")],
                },
            )

            first = import_benchmark_json(input_json, benchmark_root=benchmark_root)
            second = import_benchmark_json(input_json, benchmark_root=benchmark_root)
            repository = AnnotationRepository(benchmark_root)
            records = repository.load_records()
            annotations = repository.load_annotations(repository.annotations_ai_path)

        self.assertEqual(1, first.records_added)
        self.assertEqual(0, second.records_added)
        self.assertEqual(1, second.records_skipped_existing)
        self.assertEqual(0, second.records_updated)
        self.assertEqual(1, second.ai_annotations_unchanged)
        self.assertEqual(["paper-4"], [record.paper_id for record in records])
        self.assertEqual(["paper-4"], [annotation.paper_id for annotation in annotations])

    def test_repeated_import_updates_existing_record_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            benchmark_root = Path(temp_dir) / "benchmark"
            input_json = Path(temp_dir) / "payload.json"
            _write_payload(
                input_json,
                {
                    "records": [_record("paper-update", notes="old notes")],
                    "annotations_ai": [_annotation("paper-update", negative_tier="negative")],
                },
            )
            import_benchmark_json(input_json, benchmark_root=benchmark_root)
            _write_payload(
                input_json,
                {
                    "records": [_record("paper-update", notes="new readable notes")],
                    "annotations_ai": [_annotation("paper-update", negative_tier="negative")],
                },
            )

            summary = import_benchmark_json(input_json, benchmark_root=benchmark_root)
            repository = AnnotationRepository(benchmark_root)
            records = repository.load_records()

        self.assertEqual(0, summary.records_added)
        self.assertEqual(1, summary.records_updated)
        self.assertEqual(1, summary.records_skipped_existing)
        self.assertEqual("new readable notes", records[0].notes)

    def test_invalid_preference_label_fails_before_writing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            benchmark_root = Path(temp_dir) / "benchmark"
            input_json = Path(temp_dir) / "payload.json"
            _write_payload(
                input_json,
                {
                    "records": [_record("paper-5")],
                    "annotations_ai": [
                        _annotation(
                            "paper-5",
                            negative_tier="positive",
                            preference_labels=["其他推理加速"],
                            notes="Unknown label must stay in notes, not preference_labels.",
                        )
                    ],
                },
            )

            with self.assertRaises(BenchmarkImportError):
                import_benchmark_json(input_json, benchmark_root=benchmark_root)

            repository = AnnotationRepository(benchmark_root)
            self.assertFalse(repository.records_path.exists())
            self.assertFalse(repository.annotations_ai_path.exists())

    def test_missing_required_record_field_fails_before_writing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            benchmark_root = Path(temp_dir) / "benchmark"
            input_json = Path(temp_dir) / "payload.json"
            record = _record("paper-6")
            record["title"] = ""
            _write_payload(input_json, {"records": [record]})

            with self.assertRaises(BenchmarkImportError):
                import_benchmark_json(input_json, benchmark_root=benchmark_root)

            repository = AnnotationRepository(benchmark_root)
            self.assertFalse(repository.records_path.exists())

    def test_duplicate_paper_id_in_payload_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            input_json = Path(temp_dir) / "payload.json"
            _write_payload(input_json, {"records": [_record("paper-7"), _record("paper-7")]})

            with self.assertRaises(BenchmarkImportError):
                import_benchmark_json(input_json, benchmark_root=Path(temp_dir) / "benchmark")

    def test_dry_run_validates_without_writing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            benchmark_root = Path(temp_dir) / "benchmark"
            input_json = Path(temp_dir) / "payload.json"
            _write_payload(
                input_json,
                {
                    "records": [_record("paper-8")],
                    "annotations_ai": [_annotation("paper-8", negative_tier="negative")],
                },
            )

            summary = import_benchmark_json(input_json, benchmark_root=benchmark_root, dry_run=True)
            repository = AnnotationRepository(benchmark_root)

        self.assertTrue(summary.dry_run)
        self.assertEqual(1, summary.records_added)
        self.assertFalse(repository.records_path.exists())
        self.assertFalse(repository.annotations_ai_path.exists())

    def test_malformed_json_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            input_json = Path(temp_dir) / "payload.json"
            input_json.write_text("{", encoding="utf-8")

            with self.assertRaisesRegex(BenchmarkImportError, "不是合法 JSON"):
                import_benchmark_json(input_json, benchmark_root=Path(temp_dir) / "benchmark")


def _write_payload(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _record(paper_id: str, *, notes: str = "") -> dict[str, object]:
    return {
        "paper_id": paper_id,
        "title": f"Paper {paper_id}",
        "abstract": "A paper about efficient LLM inference.",
        "authors": ["Alice", "Bob"],
        "venue": "arXiv",
        "year": 2026,
        "source": "arxiv",
        "source_path": "arxiv:2605.00001",
        "primary_research_object": "LLM",
        "candidate_preference_labels": ["解码策略优化"],
        "candidate_negative_tier": "positive",
        "keywords": ["inference"],
        "notes": notes,
    }


def _annotation(
    paper_id: str,
    *,
    negative_tier: str,
    preference_labels: list[str] | None = None,
    notes: str = "",
) -> dict[str, object]:
    return {
        "paper_id": paper_id,
        "labeler_id": "arxiv_ai_review",
        "primary_research_object": "LLM",
        "preference_labels": preference_labels or [],
        "negative_tier": negative_tier,
        "evidence_spans": {"general": ["evidence"]},
        "notes": notes,
        "review_status": "pending",
    }


if __name__ == "__main__":
    unittest.main()
