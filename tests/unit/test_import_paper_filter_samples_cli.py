from __future__ import annotations

import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.tools.import_paper_filter_samples import main


class ImportPaperFilterSamplesCliTests(unittest.TestCase):
    def test_cli_imports_payload_into_requested_benchmark_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            benchmark_root = Path(temp_dir) / "benchmark"
            input_json = Path(temp_dir) / "payload.json"
            _write_payload(input_json, _payload("paper-cli-1"))
            stdout = StringIO()

            with patch(
                "sys.argv",
                [
                    "paper-analysis-dataset-import-samples",
                    "--input-json",
                    str(input_json),
                    "--benchmark-root",
                    str(benchmark_root),
                ],
            ):
                with redirect_stdout(stdout):
                    main()

            repository = AnnotationRepository(benchmark_root)
            summary = json.loads(stdout.getvalue())
            paper_ids = [record.paper_id for record in repository.load_records()]

        self.assertTrue(summary["ok"])
        self.assertEqual(1, summary["records_added"])
        self.assertEqual(["paper-cli-1"], paper_ids)

    def test_cli_dry_run_writes_no_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            benchmark_root = Path(temp_dir) / "benchmark"
            input_json = Path(temp_dir) / "payload.json"
            _write_payload(input_json, _payload("paper-cli-2"))
            stdout = StringIO()

            with patch(
                "sys.argv",
                [
                    "paper-analysis-dataset-import-samples",
                    "--input-json",
                    str(input_json),
                    "--benchmark-root",
                    str(benchmark_root),
                    "--dry-run",
                ],
            ):
                with redirect_stdout(stdout):
                    main()

            repository = AnnotationRepository(benchmark_root)
            summary = json.loads(stdout.getvalue())

        self.assertTrue(summary["dry_run"])
        self.assertFalse(repository.records_path.exists())
        self.assertFalse(repository.annotations_ai_path.exists())

    def test_cli_invalid_payload_exits_nonzero_with_message(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            benchmark_root = Path(temp_dir) / "benchmark"
            input_json = Path(temp_dir) / "payload.json"
            _write_payload(
                input_json,
                {
                    "records": [
                        {
                            **_record("paper-cli-3"),
                            "primary_research_object": "未知对象",
                        }
                    ]
                },
            )
            stderr = StringIO()

            with patch(
                "sys.argv",
                [
                    "paper-analysis-dataset-import-samples",
                    "--input-json",
                    str(input_json),
                    "--benchmark-root",
                    str(benchmark_root),
                ],
            ):
                with redirect_stderr(stderr):
                    with self.assertRaises(SystemExit) as raised:
                        main()

            repository = AnnotationRepository(benchmark_root)

        self.assertEqual(1, raised.exception.code)
        self.assertIn("[import-samples] error:", stderr.getvalue())
        self.assertFalse(repository.records_path.exists())


def _write_payload(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _payload(paper_id: str) -> dict[str, object]:
    return {
        "records": [_record(paper_id)],
        "annotations_ai": [
            {
                "paper_id": paper_id,
                "labeler_id": "arxiv_ai_review",
                "primary_research_object": "LLM",
                "preference_labels": [],
                "negative_tier": "negative",
                "evidence_spans": {"general": ["AI review says not a fit"]},
                "notes": "recommender=positive; ai_review=likely_false_positive",
                "review_status": "pending",
            }
        ],
    }


def _record(paper_id: str) -> dict[str, object]:
    return {
        "paper_id": paper_id,
        "title": f"Paper {paper_id}",
        "abstract": "A paper about efficient LLM inference.",
        "authors": ["Alice"],
        "venue": "arXiv",
        "year": 2026,
        "source": "arxiv",
        "source_path": "arxiv:2605.00002",
        "primary_research_object": "LLM",
        "candidate_preference_labels": [],
        "candidate_negative_tier": "negative",
        "keywords": [],
        "notes": "imported_from=arxiv_daily_review",
    }


if __name__ == "__main__":
    unittest.main()
