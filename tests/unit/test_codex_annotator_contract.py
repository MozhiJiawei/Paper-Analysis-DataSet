from __future__ import annotations

import unittest
from unittest.mock import patch

from paper_analysis_dataset.domain.benchmark import CandidatePaper
from paper_analysis_dataset.services.codex_annotator import (
    CodexCliAnnotator,
    build_codex_annotation_prompt,
    parse_codex_annotation_payload,
)
from paper_analysis_dataset.shared.clients.codex_cli_client import (
    CodexCliClient,
    DEFAULT_CODEX_CLI_MODEL,
)


class CodexAnnotatorContractTests(unittest.TestCase):
    def test_build_prompt_contains_required_contract_fields(self) -> None:
        """验证 prompt 会显式要求结构化 JSON 字段。"""

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

        prompt = build_codex_annotation_prompt(candidate)
        self.assertIn("primary_research_object", prompt)
        self.assertIn("preference_labels", prompt)
        self.assertIn("negative_tier", prompt)
        self.assertNotIn("target_preference_labels", prompt)
        self.assertIn("最小充分标注", prompt)
        self.assertIn("必须且只能选择一个最相关的偏好标签", prompt)
        self.assertIn("不允许输出多个偏好标签", prompt)
        self.assertIn("系统与调度优化", prompt)
        self.assertIn("被优化的主要对象", prompt)
        self.assertIn("上下文压缩、KV cache 压缩、prompt 压缩不算模型压缩", prompt)
        self.assertIn("为解码方法配套 head 结构", prompt)
        self.assertIn("如果 kernel 只是支撑压缩、缓存或系统方法落地，默认不单独标", prompt)
        self.assertNotIn("模型结构侧推理优化", prompt)

    def test_parse_payload_and_build_annotation(self) -> None:
        """验证 Codex 输出能被解析并转成 AnnotationRecord。"""

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
        payload = """{"primary_research_object":"LLM","preference_labels":["解码策略优化"],"negative_tier":"positive","evidence_spans":{"解码策略优化":["speculative decoding"]},"notes":"ok"}"""
        parsed = parse_codex_annotation_payload(payload)
        annotation = CodexCliAnnotator(runner=lambda _: payload).submit_annotate(candidate).result()

        self.assertEqual("LLM", parsed["primary_research_object"])
        self.assertEqual(["解码策略优化"], annotation.preference_labels)
        self.assertEqual("codex_cli", annotation.labeler_id)

    def test_small_codex_model_keeps_codex_cli_labeler_id(self) -> None:
        candidate = CandidatePaper(
            paper_id="paper-spark",
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
        payload = """{"primary_research_object":"LLM","preference_labels":["解码策略优化"],"negative_tier":"positive","evidence_spans":{"解码策略优化":["speculative decoding"]},"notes":"ok"}"""

        annotation = CodexCliAnnotator(
            runner=lambda _: payload,
            model="gpt-5.1-codex-mini",
        ).submit_annotate(candidate).result()

        self.assertEqual("codex_cli", annotation.labeler_id)

    def test_annotator_accepts_shared_client(self) -> None:
        candidate = CandidatePaper(
            paper_id="paper-2",
            title="Client Test",
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
        payload = """{"primary_research_object":"LLM","preference_labels":["解码策略优化"],"negative_tier":"positive","evidence_spans":{"解码策略优化":["speculative decoding"]},"notes":"ok"}"""
        annotator = CodexCliAnnotator(client=CodexCliClient(runner=lambda _: payload))

        annotation = annotator.submit_annotate(candidate).result()

        self.assertEqual(["解码策略优化"], annotation.preference_labels)
        self.assertEqual("pending", annotation.review_status)

    def test_runner_is_forwarded_to_codex_cli_client(self) -> None:
        runner = lambda prompt: prompt

        with patch("paper_analysis_dataset.services.codex_annotator.CodexCliClient") as client_cls:
            annotator = CodexCliAnnotator(runner=runner)

        client_cls.assert_called_once_with(runner=runner, model=DEFAULT_CODEX_CLI_MODEL, concurrency=1)
        self.assertIsNotNone(annotator)

    def test_parse_negative_payload_clears_preference_labels(self) -> None:
        """验证 negative 输出会被标准化为空偏好标签。"""

        parsed = parse_codex_annotation_payload(
            """{"primary_research_object":"LLM","preference_labels":["解码策略优化"],"negative_tier":"negative","evidence_spans":{"negative":["not relevant"]},"notes":"ok"}"""
        )

        self.assertEqual("negative", parsed["negative_tier"])
        self.assertEqual([], parsed["preference_labels"])


if __name__ == "__main__":
    unittest.main()
