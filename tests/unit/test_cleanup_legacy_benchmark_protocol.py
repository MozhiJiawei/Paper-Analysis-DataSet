from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.tools.cleanup_legacy_benchmark_protocol import (
    cleanup_legacy_benchmark_protocol,
)


ROOT_DIR = Path(__file__).resolve().parents[2]


class CleanupLegacyBenchmarkProtocolTests(unittest.TestCase):
    def test_cleanup_backs_up_root_and_rewrites_current_protocol(self) -> None:
        """验证清理脚本会先备份，再清洗旧字段，并重建派生产物。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "cleanup-legacy-benchmark-protocol"
        if temp_root.exists():
            shutil.rmtree(temp_root)
        temp_root.mkdir(parents=True, exist_ok=True)

        (temp_root / "records.jsonl").write_text(
            "\n".join(
                [
                    '{"paper_id":"paper-1","title":"Paper 1","abstract":"Abstract 1.","abstract_zh":"摘要 1。","authors":["Alice"],"venue":"ICLR 2025","year":2025,"source":"conference","source_path":"tests.json","primary_research_object":"LLM","candidate_preference_labels":["解码策略优化"],"target_preference_labels":["模型压缩"],"candidate_negative_tier":"positive","keywords":[],"notes":""}',
                    '{"paper_id":"paper-2","title":"Paper 2","abstract":"Abstract 2.","abstract_zh":"摘要 2。","authors":["Bob"],"venue":"ICLR 2025","year":2025,"source":"conference","source_path":"tests.json","primary_research_object":"LLM","candidate_preference_labels":["解码策略优化"],"target_preference_labels":["系统与调度优化"],"candidate_negative_tier":"hard","keywords":[],"notes":"legacy"}',
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (temp_root / "annotations-ai.jsonl").write_text(
            "\n".join(
                [
                    '{"paper_id":"paper-1","labeler_id":"codex_cli","primary_research_object":"LLM","preference_labels":["解码策略优化"],"target_preference_labels":["模型压缩"],"negative_tier":"positive","evidence_spans":{"general":["ai"]},"notes":"","review_status":"pending"}',
                    '{"paper_id":"paper-2","labeler_id":"codex_cli","primary_research_object":"LLM","preference_labels":["解码策略优化"],"target_preference_labels":["模型压缩"],"negative_tier":"hard","evidence_spans":{"general":["ai"]},"notes":"","review_status":"pending"}',
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (temp_root / "annotations-human.jsonl").write_text(
            "\n".join(
                [
                    '{"paper_id":"paper-1","labeler_id":"human_reviewer","primary_research_object":"AI 系统 / 基础设施","preference_labels":["解码策略优化"],"target_preference_labels":["模型压缩"],"negative_tier":"positive","evidence_spans":{"general":["human"]},"notes":"","review_status":"pending"}',
                    '{"paper_id":"paper-2","labeler_id":"human_reviewer","primary_research_object":"LLM","preference_labels":["解码策略优化"],"target_preference_labels":["模型压缩"],"negative_tier":"in_domain","evidence_spans":{"general":["human"]},"notes":"","review_status":"pending"}',
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (temp_root / "merged.jsonl").write_text("", encoding="utf-8")
        (temp_root / "conflicts.jsonl").write_text(
            '{"paper_id":"paper-1","conflicting_fields":["primary_research_object"],"codex_annotation":{"paper_id":"paper-1","labeler_id":"codex_cli","primary_research_object":"LLM","preference_labels":["解码策略优化"],"target_preference_labels":["模型压缩"],"negative_tier":"positive","evidence_spans":{"general":["ai"]},"notes":"","review_status":"pending"},"human_annotation":{"paper_id":"paper-1","labeler_id":"human_reviewer","primary_research_object":"AI 系统 / 基础设施","preference_labels":["解码策略优化"],"target_preference_labels":["模型压缩"],"negative_tier":"positive","evidence_spans":{"general":["human"]},"notes":"","review_status":"pending"},"resolved_annotation":{"paper_id":"paper-1","labeler_id":"arbiter","primary_research_object":"AI 系统 / 基础设施","preference_labels":["解码策略优化"],"target_preference_labels":["模型压缩"],"negative_tier":"positive","evidence_spans":{"general":["human"]},"notes":"仲裁采纳：human","review_status":"final"}}\n',
            encoding="utf-8",
        )
        (temp_root / "schema.json").write_text(
            json.dumps({"name": "paper-filter", "migration": "legacy", "record_fields": {}, "annotation_fields": {}}, ensure_ascii=False),
            encoding="utf-8",
        )
        (temp_root / "stats.json").write_text(json.dumps({"legacy": True}, ensure_ascii=False), encoding="utf-8")

        summary = cleanup_legacy_benchmark_protocol(temp_root)
        repository = AnnotationRepository(temp_root)

        backup_path = Path(str(summary["backup_path"]))
        self.assertTrue(backup_path.exists())
        self.assertTrue((backup_path / "records.jsonl").exists())

        records_payload = repository.records_path.read_text(encoding="utf-8")
        records = repository.load_records()
        self.assertNotIn("target_preference_labels", records_payload)
        self.assertNotIn("final_target_preference_labels", records_payload)
        self.assertNotIn('"candidate_negative_tier":"hard"', records_payload)
        self.assertEqual(["positive", "negative"], [item.candidate_negative_tier for item in records])

        ai_payload = repository.annotations_ai_path.read_text(encoding="utf-8")
        human_payload = repository.annotations_human_path.read_text(encoding="utf-8")
        ai_annotations = repository.load_annotations(repository.annotations_ai_path)
        human_annotations = repository.load_annotations(repository.annotations_human_path)
        self.assertNotIn("target_preference_labels", ai_payload)
        self.assertNotIn("target_preference_labels", human_payload)
        self.assertNotIn('"negative_tier":"hard"', ai_payload)
        self.assertNotIn('"negative_tier":"in_domain"', human_payload)
        self.assertEqual(["positive", "negative"], [item.negative_tier for item in ai_annotations])
        self.assertEqual(["positive", "negative"], [item.negative_tier for item in human_annotations])

        merged = repository.load_annotations(repository.merged_path)
        conflicts = repository.load_conflicts(repository.conflicts_path)
        self.assertEqual(["paper-1", "paper-2"], [item.paper_id for item in merged])
        self.assertEqual(["paper-1"], [item.paper_id for item in conflicts])
        self.assertTrue(conflicts[0].is_resolved)

        stats = repository.read_json(repository.stats_path)
        self.assertEqual(2, stats["total_records"])
        self.assertEqual({"negative": 1, "positive": 1}, stats["by_negative_tier"])

        schema = repository.read_json(repository.schema_path)
        self.assertEqual(["positive", "negative"], schema["negative_tiers"])
        self.assertNotIn("migration", schema)


if __name__ == "__main__":
    unittest.main()
