from __future__ import annotations

import unittest

from paper_analysis_dataset.domain.benchmark import CandidatePaper
from paper_analysis_dataset.services.doubao_title_translator import (
    DoubaoTitleTranslator,
    build_doubao_title_translation_messages,
    parse_doubao_title_translation_payload,
)


class DoubaoTitleTranslatorTests(unittest.TestCase):
    def test_build_messages_requires_plain_chinese_output(self) -> None:
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
        )

        messages = build_doubao_title_translation_messages(candidate)

        self.assertIn("只输出中文标题正文", messages[0]["content"])
        self.assertEqual("title=Prompt Test", messages[1]["content"])

    def test_parse_payload_requires_chinese_content(self) -> None:
        self.assertEqual("提示测试", parse_doubao_title_translation_payload("提示测试"))
        with self.assertRaises(ValueError):
            parse_doubao_title_translation_payload("Translation: Prompt Test")

    def test_translate_uses_runner(self) -> None:
        candidate = CandidatePaper(
            paper_id="paper-2",
            title="Runner Test",
            abstract="About KV cache.",
            authors=["Alice"],
            venue="ICLR 2025",
            year=2025,
            source="conference",
            source_path="tests.json",
            primary_research_object="LLM",
        )

        translator = DoubaoTitleTranslator(
            runner=lambda messages: {"success": True, "content": "运行器测试", "error": None}
        )
        self.assertEqual("运行器测试", translator.submit_translate(candidate).result())


if __name__ == "__main__":
    unittest.main()
