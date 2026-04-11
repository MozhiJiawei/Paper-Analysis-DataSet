from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from paper_analysis_dataset.domain.benchmark import BenchmarkRecord
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.benchmark_schema_validator import validate_benchmark_schema
from paper_analysis_dataset.services.paper_filter_schema import build_schema_payload
from paper_analysis_dataset.tools.rebuild_paper_filter_benchmark import rebuild_benchmark


ROOT_DIR = Path(__file__).resolve().parents[2]
FIXTURE_PAPERLISTS_ROOT = ROOT_DIR / "tests" / "fixtures" / "paperlists_repo"


class _FakeTranslator:
    def submit_translate(self, candidate: object):
        from concurrent.futures import Future

        future: Future[str] = Future()
        future.set_result("中文摘要：" + str(candidate.title))
        return future


class BenchmarkSchemaValidatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.benchmark_root = Path(self.temp_dir.name) / "paper-filter"
        rebuild_benchmark(
            paperlists_root=FIXTURE_PAPERLISTS_ROOT,
            benchmark_root=self.benchmark_root,
            abstract_translator=_FakeTranslator(),
            quota_by_venue={
                ("aaai", 2025): 1,
                ("iclr", 2025): 1,
                ("iclr", 2026): 1,
                ("icml", 2025): 1,
                ("nips", 2025): 1,
            },
            minimum_score=6,
        )
        _populate_title_zh(self.benchmark_root)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_validator_accepts_valid_dataset(self) -> None:
        summary = validate_benchmark_schema(self.benchmark_root)

        self.assertTrue(summary["ok"])
        self.assertEqual(build_schema_payload()["version"], "2026-04-01")
        self.assertEqual(5, summary["file_counts"]["records"])

    def test_validator_rejects_invalid_record_field_type(self) -> None:
        repository = AnnotationRepository(self.benchmark_root)
        payload = repository.records_path.read_text(encoding="utf-8").splitlines()
        payload[0] = payload[0].replace('"year": 2025', '"year": "2025"')
        repository.records_path.write_text("\n".join(payload) + "\n", encoding="utf-8")

        summary = validate_benchmark_schema(self.benchmark_root)

        self.assertFalse(summary["ok"])
        self.assertTrue(any("必须是整数" in issue["message"] for issue in summary["issues"]))

    def test_validator_rejects_schema_drift(self) -> None:
        repository = AnnotationRepository(self.benchmark_root)
        repository.schema_path.write_text('{"name":"drift"}', encoding="utf-8")

        summary = validate_benchmark_schema(self.benchmark_root)

        self.assertFalse(summary["ok"])
        self.assertTrue(any("schema.json 与代码内建协议不一致" in issue["message"] for issue in summary["issues"]))

    def test_validator_rejects_refusal_text_in_record_translation(self) -> None:
        repository = AnnotationRepository(self.benchmark_root)
        payload = repository.records_path.read_text(encoding="utf-8").splitlines()
        payload[0] = payload[0].replace("中文摘要：", "请提供完整英文摘要内容；")
        repository.records_path.write_text("\n".join(payload) + "\n", encoding="utf-8")

        summary = validate_benchmark_schema(self.benchmark_root)

        self.assertFalse(summary["ok"])
        self.assertTrue(any("中文摘要包含异常拒答文本" in issue["message"] for issue in summary["issues"]))

    def test_validator_rejects_suspiciously_short_translation_for_long_abstract(self) -> None:
        repository = AnnotationRepository(self.benchmark_root)
        lines = repository.records_path.read_text(encoding="utf-8").splitlines()
        first = json.loads(lines[0])
        first["abstract"] = "A" * 500
        first["abstract_zh"] = "过短摘要。"
        lines[0] = json.dumps(first, ensure_ascii=False)
        repository.records_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        summary = validate_benchmark_schema(self.benchmark_root)

        self.assertFalse(summary["ok"])
        self.assertTrue(any("中文摘要疑似截断或严重过短" in issue["message"] for issue in summary["issues"]))

    def test_validator_rejects_missing_title_zh_when_title_exists(self) -> None:
        repository = AnnotationRepository(self.benchmark_root)
        lines = repository.records_path.read_text(encoding="utf-8").splitlines()
        first = json.loads(lines[0])
        first["title_zh"] = ""
        lines[0] = json.dumps(first, ensure_ascii=False)
        repository.records_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        summary = validate_benchmark_schema(self.benchmark_root)

        self.assertFalse(summary["ok"])
        self.assertTrue(any("title_zh 不能为空" in issue["message"] for issue in summary["issues"]))

    def test_validator_rejects_missing_abstract_zh_when_abstract_exists(self) -> None:
        repository = AnnotationRepository(self.benchmark_root)
        lines = repository.records_path.read_text(encoding="utf-8").splitlines()
        first = json.loads(lines[0])
        first["abstract_zh"] = ""
        lines[0] = json.dumps(first, ensure_ascii=False)
        repository.records_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        summary = validate_benchmark_schema(self.benchmark_root)

        self.assertFalse(summary["ok"])
        self.assertTrue(any("abstract_zh 不能为空" in issue["message"] for issue in summary["issues"]))


if __name__ == "__main__":
    unittest.main()


def _populate_title_zh(benchmark_root: Path) -> None:
    repository = AnnotationRepository(benchmark_root)
    records = repository.load_records()
    repository.write_records(
        [
            BenchmarkRecord(
                paper_id=record.paper_id,
                title=record.title,
                title_zh=f"中文标题：{record.title}",
                abstract=record.abstract,
                abstract_zh=record.abstract_zh,
                authors=record.authors,
                venue=record.venue,
                year=record.year,
                source=record.source,
                source_path=record.source_path,
                primary_research_object=record.primary_research_object,
                candidate_preference_labels=record.candidate_preference_labels,
                candidate_negative_tier=record.candidate_negative_tier,
                keywords=record.keywords,
                notes=record.notes,
                final_primary_research_object=record.final_primary_research_object,
                final_preference_labels=record.final_preference_labels,
                final_negative_tier=record.final_negative_tier,
                final_labeler_ids=record.final_labeler_ids,
                final_review_status=record.final_review_status,
                final_evidence_spans=record.final_evidence_spans,
            )
            for record in records
        ]
    )
