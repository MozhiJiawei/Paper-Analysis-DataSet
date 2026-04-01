from __future__ import annotations

import unittest
from unittest.mock import patch

from paper_analysis_dataset.domain.benchmark import CandidatePaper
from paper_analysis_dataset.services.codex_title_translator import (
    CodexTitleTranslator,
    build_codex_title_translation_prompt,
    parse_codex_title_translation_payload,
)
from paper_analysis_dataset.shared.clients.codex_cli_client import DEFAULT_CODEX_CLI_MODEL


class CodexTitleTranslatorTests(unittest.TestCase):
    def test_build_prompt_requires_plain_chinese_output(self) -> None:
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

        prompt = build_codex_title_translation_prompt(candidate)

        self.assertIn("只输出中文标题正文", prompt)
        self.assertIn("title=Prompt Test", prompt)

    def test_parse_payload_strips_prefixed_output(self) -> None:
        parsed = parse_codex_title_translation_payload("中文标题：提示测试")
        self.assertEqual("提示测试", parsed)

    def test_parse_payload_accepts_json_wrapped_output(self) -> None:
        parsed = parse_codex_title_translation_payload('{"title_zh":"提示测试"}')
        self.assertEqual("提示测试", parsed)

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

        translator = CodexTitleTranslator(runner=lambda _: "运行器测试")
        self.assertEqual("运行器测试", translator.submit_translate(candidate).result())

    def test_translator_locks_model_to_codex_mini(self) -> None:
        runner = lambda prompt: prompt

        with patch("paper_analysis_dataset.services.codex_title_translator.CodexCliClient") as client_cls:
            translator = CodexTitleTranslator(runner=runner, concurrency=3)

        client_cls.assert_called_once_with(runner=runner, model=DEFAULT_CODEX_CLI_MODEL, concurrency=3)
        self.assertEqual(DEFAULT_CODEX_CLI_MODEL, translator.model)


if __name__ == "__main__":
    unittest.main()
