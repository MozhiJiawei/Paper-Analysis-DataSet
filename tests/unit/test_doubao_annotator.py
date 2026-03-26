from __future__ import annotations

import unittest
from pathlib import Path

from paper_analysis_dataset.domain.benchmark import CandidatePaper
from paper_analysis_dataset.services.doubao_annotator import (
    DoubaoAnnotator,
    build_doubao_annotation_messages,
)


class DoubaoAnnotatorTests(unittest.TestCase):
    def test_build_messages_reuse_annotation_contract(self) -> None:
        candidate = CandidatePaper(
            paper_id="paper-1",
            title="Prompt Test",
            abstract="About speculative decoding.",
            authors=["Alice"],
            venue="ICLR 2025",
            year=2025,
            source="conference",
            source_path="tests.json",
            primary_research_object="LLM",
            candidate_preference_labels=["解码策略优化"],
            candidate_negative_tier="positive",
        )

        messages = build_doubao_annotation_messages(candidate, force_decision=True)

        self.assertEqual("system", messages[0]["role"])
        self.assertIn("严格返回一个 JSON 对象", messages[0]["content"])
        self.assertIn("必须直接完成标注", messages[1]["content"])

    def test_annotate_uses_runner_payload(self) -> None:
        candidate = CandidatePaper(
            paper_id="paper-2",
            title="Prompt Test",
            abstract="About speculative decoding.",
            authors=["Alice"],
            venue="ICLR 2025",
            year=2025,
            source="conference",
            source_path="tests.json",
            primary_research_object="LLM",
            candidate_preference_labels=["解码策略优化"],
            candidate_negative_tier="positive",
        )
        annotator = DoubaoAnnotator(
            runner=lambda _: {
                "success": True,
                "content": """{"primary_research_object":"LLM","preference_labels":["解码策略优化"],"negative_tier":"positive","evidence_spans":{"解码策略优化":["speculative decoding"]},"notes":"ok"}""",
            },
            config_path=Path("missing.yaml"),
        )

        annotation = annotator.submit_annotate(candidate).result()

        self.assertEqual("doubao", annotation.labeler_id)
        self.assertEqual(["解码策略优化"], annotation.preference_labels)

    def test_runner_failure_raises_runtime_error(self) -> None:
        candidate = CandidatePaper(
            paper_id="paper-3",
            title="Prompt Test",
            abstract="About speculative decoding.",
            authors=["Alice"],
            venue="ICLR 2025",
            year=2025,
            source="conference",
            source_path="tests.json",
            primary_research_object="LLM",
            candidate_preference_labels=["解码策略优化"],
            candidate_negative_tier="positive",
        )
        annotator = DoubaoAnnotator(
            runner=lambda _: {"success": False, "error": "boom", "content": None},
            config_path=Path("missing.yaml"),
        )

        with self.assertRaises(RuntimeError):
            annotator.submit_annotate(candidate).result()


if __name__ == "__main__":
    unittest.main()
