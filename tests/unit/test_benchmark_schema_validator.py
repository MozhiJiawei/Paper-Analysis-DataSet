from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

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

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_validator_accepts_valid_dataset(self) -> None:
        summary = validate_benchmark_schema(self.benchmark_root)

        self.assertTrue(summary["ok"])
        self.assertEqual(build_schema_payload()["version"], "2026-03-31")
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


if __name__ == "__main__":
    unittest.main()
