from __future__ import annotations

import unittest
from unittest.mock import patch

from paper_analysis_dataset.domain.benchmark import CandidatePaper
from paper_analysis_dataset.services.codex_abstract_translator import (
    CodexAbstractTranslator,
    build_codex_abstract_translation_prompt,
    parse_codex_abstract_translation_payload,
)
from paper_analysis_dataset.shared.clients.codex_cli_client import (
    CodexCliClient,
    DEFAULT_CODEX_CLI_MODEL,
)


class CodexAbstractTranslatorTests(unittest.TestCase):
    def test_build_prompt_requires_plain_chinese_output(self) -> None:
        candidate = CandidatePaper(
            paper_id="paper-1",
            title="Prompt Test",
            abstract="About speculative decoding.\nWith extra detail.",
            authors=["Alice"],
            venue="ICLR 2025",
            year=2025,
            source="conference",
            source_path="tests.json",
            primary_research_object="LLM",
        )

        prompt = build_codex_abstract_translation_prompt(candidate)

        self.assertIn("只输出中文摘要正文", prompt)
        self.assertIn("title=Prompt Test;", prompt)
        self.assertIn("abstract=About speculative decoding. With extra detail.", prompt)

    def test_build_prompt_for_long_abstract_requires_complete_translation(self) -> None:
        candidate = CandidatePaper(
            paper_id="paper-long",
            title="Long Prompt Test",
            abstract="A" * 450,
            authors=["Alice"],
            venue="ICLR 2025",
            year=2025,
            source="conference",
            source_path="tests.json",
            primary_research_object="LLM",
        )

        prompt = build_codex_abstract_translation_prompt(
            candidate,
            force_plain_output=True,
            force_complete_translation=True,
        )

        self.assertIn("必须完整覆盖原摘要全部信息", prompt)
        self.assertIn("如果译文少于120个中文字符，视为不合格", prompt)

    def test_parse_payload_returns_clean_translation(self) -> None:
        parsed = parse_codex_abstract_translation_payload("这是一段忠实的中文摘要。")
        self.assertEqual("这是一段忠实的中文摘要。", parsed)

    def test_parse_payload_strips_prefixed_output(self) -> None:
        parsed = parse_codex_abstract_translation_payload("中文翻译：这是一段摘要。")
        self.assertEqual("这是一段摘要。", parsed)

    def test_parse_payload_accepts_json_wrapped_output(self) -> None:
        parsed = parse_codex_abstract_translation_payload('{"abstract_zh":"这是一段摘要。"}')
        self.assertEqual("这是一段摘要。", parsed)

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

        translator = CodexAbstractTranslator(runner=lambda _: "这是中文摘要。")
        self.assertEqual("这是中文摘要。", translator.submit_translate(candidate).result())

    def test_translate_retries_when_long_translation_is_suspiciously_short(self) -> None:
        candidate = CandidatePaper(
            paper_id="paper-2b",
            title="Runner Long Test",
            abstract="A" * 500,
            authors=["Alice"],
            venue="ICLR 2025",
            year=2025,
            source="conference",
            source_path="tests.json",
            primary_research_object="LLM",
        )
        outputs = iter(
            [
                "过短摘要。",
                "这是一段完整的中文摘要。" * 12,
            ]
        )
        translator = CodexAbstractTranslator(runner=lambda _: next(outputs))

        result = translator.submit_translate(candidate).result()

        self.assertIn("完整的中文摘要", result)

    def test_translator_accepts_shared_client(self) -> None:
        candidate = CandidatePaper(
            paper_id="paper-3",
            title="Client Test",
            abstract="About serving.",
            authors=["Alice"],
            venue="ICLR 2025",
            year=2025,
            source="conference",
            source_path="tests.json",
            primary_research_object="LLM",
        )
        client = CodexCliClient(runner=lambda _: "这是中文摘要。")

        translator = CodexAbstractTranslator(client=client)

        self.assertEqual("这是中文摘要。", translator.submit_translate(candidate).result())

    def test_runner_is_forwarded_to_codex_cli_client(self) -> None:
        runner = lambda prompt: prompt

        with patch("paper_analysis_dataset.services.codex_abstract_translator.CodexCliClient") as client_cls:
            translator = CodexAbstractTranslator(runner=runner, concurrency=3)

        client_cls.assert_called_once_with(runner=runner, model=DEFAULT_CODEX_CLI_MODEL, concurrency=3)
        self.assertIsNotNone(translator)


if __name__ == "__main__":
    unittest.main()
