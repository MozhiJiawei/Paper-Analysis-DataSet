from __future__ import annotations

from concurrent.futures import Future
import json
import shutil
import unittest
from pathlib import Path

from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.benchmark_schema_validator import (
    DEFAULT_BENCHMARK_ROOT,
    validate_benchmark_schema,
)
from paper_analysis_dataset.tools.rebuild_paper_filter_benchmark import rebuild_benchmark


ROOT_DIR = Path(__file__).resolve().parents[2]
FIXTURE_PAPERLISTS_ROOT = ROOT_DIR / "tests" / "fixtures" / "paperlists_repo"


class _FakeTranslator:
    def submit_translate(self, candidate: object) -> Future[str]:
        future: Future[str] = Future()
        future.set_result("中文摘要：" + str(candidate.title))
        return future


class BenchmarkDatasetContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_root = ROOT_DIR / "artifacts" / "test-output" / "benchmark-dataset-contract"
        if cls.temp_root.exists():
            shutil.rmtree(cls.temp_root)
        rebuild_benchmark(
            paperlists_root=FIXTURE_PAPERLISTS_ROOT,
            benchmark_root=cls.temp_root,
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

    def test_single_version_dataset_files_exist(self) -> None:
        """验证单版本目录包含预期核心文件。"""

        repository = AnnotationRepository(self.temp_root)

        self.assertTrue(repository.records_path.exists())
        self.assertTrue(repository.annotations_ai_path.exists())
        self.assertTrue(repository.annotations_human_path.exists())
        self.assertTrue(repository.merged_path.exists())
        self.assertTrue(repository.conflicts_path.exists())
        self.assertTrue(repository.schema_path.exists())
        self.assertTrue(repository.stats_path.exists())

    def test_root_records_are_unique_metadata_source(self) -> None:
        """验证只有根 records.jsonl 保存论文元数据主表。"""

        repository = AnnotationRepository(self.temp_root)
        records = repository.load_records()
        self.assertEqual(len(records), len({record.paper_id for record in records}))
        self.assertTrue(all(record.abstract_zh.startswith("中文摘要：") for record in records))

        for path in (
            repository.annotations_ai_path,
            repository.annotations_human_path,
            repository.merged_path,
        ):
            payload = path.read_text(encoding="utf-8")
            self.assertNotIn('"title"', payload)
            self.assertNotIn('"abstract"', payload)
            self.assertNotIn('"abstract_zh"', payload)
            self.assertNotIn('"venue"', payload)

        conflicts_payload = repository.conflicts_path.read_text(encoding="utf-8")
        self.assertNotIn('"title"', conflicts_payload)
        self.assertNotIn('"abstract"', conflicts_payload)
        self.assertNotIn('"abstract_zh"', conflicts_payload)
        self.assertNotIn('"venue"', conflicts_payload)

        root_payload = repository.records_path.read_text(encoding="utf-8")
        self.assertNotIn('"target_preference_labels"', root_payload)
        self.assertNotIn('"final_primary_research_object"', root_payload)
        self.assertNotIn('"final_preference_labels"', root_payload)
        self.assertNotIn('"final_negative_tier"', root_payload)
        self.assertNotIn('"final_labeler_ids"', root_payload)
        self.assertNotIn('"final_review_status"', root_payload)
        self.assertNotIn('"final_evidence_spans"', root_payload)

    def test_schema_declares_single_version_file_contract(self) -> None:
        """验证 schema.json 描述的是单版本文件协议。"""

        repository = AnnotationRepository(self.temp_root)
        payload = json.loads(repository.schema_path.read_text(encoding="utf-8"))

        self.assertEqual("paper-filter", payload["name"])
        self.assertEqual("2026-03-26", payload["version"])
        self.assertEqual("annotations-ai.jsonl", payload["files"]["annotations_ai"])
        self.assertEqual("merged.jsonl", payload["files"]["merged"])
        self.assertEqual("string", payload["record_fields"]["abstract_zh"])
        self.assertNotIn("target_preference_labels", payload["record_fields"])
        self.assertNotIn("target_preference_labels", payload["annotation_fields"])
        self.assertEqual("0..1", payload["annotation_constraints"]["preference_labels_cardinality"])
        self.assertTrue(payload["annotation_constraints"]["positive_requires_exactly_one_preference_label"])
        self.assertEqual(["positive", "negative"], payload["negative_tiers"])
        self.assertNotIn("migration", payload)
        self.assertNotIn("splits", json.dumps(payload, ensure_ascii=False))

    def test_stats_include_layered_annotation_view(self) -> None:
        """验证 stats.json 暴露按层统计，并保留顶层兼容字段。"""

        repository = AnnotationRepository(self.temp_root)
        stats = repository.read_json(repository.stats_path)

        self.assertIn("total_records", stats)
        self.assertIn("by_negative_tier", stats)
        self.assertIn("by_layer", stats)
        self.assertEqual(0, stats["by_layer"]["annotations_ai"]["total_records"])
        self.assertIn("positive_ratio", stats["by_layer"]["annotations_ai"])
        self.assertIn("by_negative_tier", stats["by_layer"]["annotations_ai"])

    def test_rebuild_requires_explicit_existing_paperlists_root(self) -> None:
        with self.assertRaises(ValueError):
            rebuild_benchmark(
                paperlists_root=ROOT_DIR / "tests" / "fixtures" / "missing-paperlists",
                benchmark_root=self.temp_root / "missing",
                abstract_translator=_FakeTranslator(),
            )

    def test_repository_records_jsonl_passes_full_schema_scan(self) -> None:
        """验证仓内 records.jsonl 会被测试全量扫描，且字段契约全部通过。"""

        summary = validate_benchmark_schema(DEFAULT_BENCHMARK_ROOT)
        records_path = str(DEFAULT_BENCHMARK_ROOT / "records.jsonl")
        record_issues = [
            issue for issue in summary["issues"] if str(issue["path"]).startswith(records_path)
        ]
        repository = AnnotationRepository(DEFAULT_BENCHMARK_ROOT)

        self.assertEqual(
            len(repository.load_records()),
            summary["file_counts"]["records"],
        )
        self.assertGreater(summary["file_counts"]["records"], 0)
        self.assertEqual([], record_issues)


if __name__ == "__main__":
    unittest.main()
