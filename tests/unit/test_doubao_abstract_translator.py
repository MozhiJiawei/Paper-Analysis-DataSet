from __future__ import annotations

import os
import sys
import tempfile
import unittest
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from paper_analysis_dataset.domain.benchmark import CandidatePaper
from paper_analysis_dataset.services.doubao_abstract_translator import (
    DoubaoAbstractTranslator,
    build_doubao_abstract_translation_messages,
    parse_doubao_abstract_translation_payload,
)
from paper_analysis.utils.doubao_client import DoubaoClient


class DoubaoAbstractTranslatorTests(unittest.TestCase):
    def test_build_messages_require_plain_chinese_output(self) -> None:
        candidate = CandidatePaper(
            paper_id="paper-1",
            title="Prompt Test",
            abstract="About speculative decoding.",
            abstract_zh="",
            authors=["Alice"],
            venue="ICLR 2025",
            year=2025,
            source="conference",
            source_path="tests.json",
            primary_research_object="LLM",
            candidate_preference_labels=["解码策略优化"],
            candidate_negative_tier="positive",
        )

        messages = build_doubao_abstract_translation_messages(candidate)

        self.assertEqual("system", messages[0]["role"])
        self.assertIn("只输出中文摘要正文", messages[0]["content"])
        self.assertIn("abstract=About speculative decoding.", messages[1]["content"])

    def test_parse_payload_returns_clean_translation(self) -> None:
        parsed = parse_doubao_abstract_translation_payload("这是一段忠实的中文摘要。")
        self.assertEqual("这是一段忠实的中文摘要。", parsed)

    def test_parse_payload_rejects_prefixed_output(self) -> None:
        with self.assertRaises(ValueError):
            parse_doubao_abstract_translation_payload("中文翻译：这是一段摘要。")

    def test_translate_uses_runner(self) -> None:
        candidate = CandidatePaper(
            paper_id="paper-2",
            title="Runner Test",
            abstract="About KV cache.",
            abstract_zh="",
            authors=["Alice"],
            venue="ICLR 2025",
            year=2025,
            source="conference",
            source_path="tests.json",
            primary_research_object="LLM",
            candidate_preference_labels=["上下文与缓存优化"],
            candidate_negative_tier="positive",
        )

        translator = DoubaoAbstractTranslator(
            runner=lambda _: {"success": True, "content": "这是中文摘要。"},
            config_path=Path("missing.yaml"),
        )
        self.assertEqual("这是中文摘要。", translator.submit_translate(candidate).result())

    def test_runner_failure_raises_runtime_error(self) -> None:
        candidate = CandidatePaper(
            paper_id="paper-3",
            title="Runner Test",
            abstract="About serving.",
            abstract_zh="",
            authors=["Alice"],
            venue="ICLR 2025",
            year=2025,
            source="conference",
            source_path="tests.json",
            primary_research_object="LLM",
            candidate_preference_labels=["系统与调度优化"],
            candidate_negative_tier="positive",
        )

        translator = DoubaoAbstractTranslator(
            runner=lambda _: {"success": False, "error": "boom", "content": None},
            config_path=Path("missing.yaml"),
        )
        with self.assertRaises(RuntimeError):
            translator.submit_translate(candidate).result()

    def test_translator_does_not_depend_on_example_config(self) -> None:
        candidate = CandidatePaper(
            paper_id="paper-4",
            title="No Example Dependency",
            abstract="About serving.",
            abstract_zh="",
            authors=["Alice"],
            venue="ICLR 2025",
            year=2025,
            source="conference",
            source_path="tests.json",
            primary_research_object="LLM",
            candidate_preference_labels=["系统与调度优化"],
            candidate_negative_tier="positive",
        )

        translator = DoubaoAbstractTranslator(
            runner=lambda _: {"success": True, "content": "这是中文摘要。"},
            config_path=Path("definitely-not-example.yaml"),
        )
        self.assertEqual("这是中文摘要。", translator.submit_translate(candidate).result())

    def test_translator_passes_config_to_client(self) -> None:
        with patch("paper_analysis_dataset.services.doubao_abstract_translator.DoubaoClient") as client_cls:
            translator = DoubaoAbstractTranslator(
                runner=lambda _: {"success": True, "content": "这是中文摘要。"},
                api_key="key",
                base_url="https://example.test",
                model="model-x",
                config_path=Path("custom.yaml"),
            )

        client_cls.assert_called_once_with(
            runner=translator.runner,
            api_key="key",
            base_url="https://example.test",
            model="model-x",
            config_path=Path("custom.yaml"),
            concurrency=1,
        )


class DoubaoClientTests(unittest.TestCase):
    def test_invalid_concurrency_raises_value_error(self) -> None:
        for value in (0, -1, 11):
            with self.assertRaises(ValueError):
                DoubaoClient(concurrency=value, config_path=Path("missing.yaml"))

    def test_explicit_config_has_highest_priority(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "doubao.yaml"
            config_path.write_text(
                "doubao:\n  api_key: yaml-key\n  base_url: https://yaml.test\n  model: yaml-model\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"ARK_API_KEY": "env-key"}, clear=False):
                client = DoubaoClient(
                    api_key="explicit-key",
                    base_url="https://explicit.test",
                    model="explicit-model",
                    config_path=config_path,
                )

        self.assertEqual("explicit-key", client.resolved_api_key)
        self.assertEqual("https://explicit.test", client.resolved_base_url)
        self.assertEqual("explicit-model", client.resolved_model)

    def test_environment_key_overrides_yaml(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "doubao.yaml"
            config_path.write_text(
                "doubao:\n  api_key: yaml-key\n  base_url: https://yaml.test\n  model: yaml-model\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"ARK_API_KEY": "env-key"}, clear=False):
                client = DoubaoClient(config_path=config_path)

        self.assertEqual("env-key", client.resolved_api_key)
        self.assertEqual("https://yaml.test", client.resolved_base_url)
        self.assertEqual("yaml-model", client.resolved_model)

    def test_missing_config_keeps_defaults(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            client = DoubaoClient(config_path=Path("missing.yaml"))

        self.assertIsNone(client.resolved_api_key)
        self.assertEqual("https://ark.cn-beijing.volces.com/api/v3", client.resolved_base_url)
        self.assertEqual("doubao-seed-2-0-pro-260215", client.resolved_model)

    def test_default_config_reads_from_user_private_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_root = Path(temp_dir) / ".paper-analysis"
            config_root.mkdir(parents=True, exist_ok=True)
            (config_root / "doubao.yaml").write_text(
                "doubao:\n  api_key: private-key\n  base_url: https://private.test\n  model: private-model\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"PAPER_ANALYSIS_HOME": str(config_root)}, clear=False):
                client = DoubaoClient()

        self.assertEqual("private-key", client.resolved_api_key)
        self.assertEqual("https://private.test", client.resolved_base_url)
        self.assertEqual("private-model", client.resolved_model)

    def test_missing_key_error_points_to_safe_config_location(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            client = DoubaoClient(config_path=Path("missing.yaml"))

        with self.assertRaises(ValueError) as context:
            client.submit([{"role": "user", "content": "hi"}]).result()

        self.assertIn("ARK_API_KEY", str(context.exception))
        self.assertIn(".paper-analysis", str(context.exception))
        self.assertIn("doubao.template.yaml", str(context.exception))

    def test_runner_short_circuits_sdk_creation(self) -> None:
        client = DoubaoClient(
            runner=lambda _: {"success": True, "content": "ok", "usage": None},
            config_path=Path("missing.yaml"),
        )

        with patch.object(DoubaoClient, "_get_client", side_effect=AssertionError("should not create sdk client")):
            result = client.submit([{"role": "user", "content": "hi"}]).result()

        self.assertTrue(result["success"])
        self.assertEqual("ok", result["content"])

    def test_sdk_response_is_normalized(self) -> None:
        response = SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="translated"))],
            usage=SimpleNamespace(prompt_tokens=1, completion_tokens=2, total_tokens=3),
        )
        fake_sdk_client = SimpleNamespace(
            chat=SimpleNamespace(
                completions=SimpleNamespace(
                    create=lambda **_: response,
                )
            )
        )
        client = DoubaoClient(api_key="key", config_path=Path("missing.yaml"))

        with patch.object(DoubaoClient, "_get_client", return_value=fake_sdk_client):
            result = client.submit([{"role": "user", "content": "hi"}]).result()

        self.assertEqual(
            {
                "success": True,
                "content": "translated",
                "error": None,
                "usage": {
                    "prompt_tokens": 1,
                    "completion_tokens": 2,
                    "total_tokens": 3,
                },
            },
            result,
        )

    def test_sdk_exception_returns_failed_response(self) -> None:
        fake_sdk_client = SimpleNamespace(
            chat=SimpleNamespace(
                completions=SimpleNamespace(
                    create=lambda **_: (_ for _ in ()).throw(RuntimeError("boom")),
                )
            )
        )
        client = DoubaoClient(api_key="key", config_path=Path("missing.yaml"))

        with patch.object(DoubaoClient, "_get_client", return_value=fake_sdk_client):
            result = client.submit([{"role": "user", "content": "hi"}]).result()

        self.assertFalse(result["success"])
        self.assertEqual("boom", result["error"])
        self.assertIsNone(result["content"])

    def test_thread_local_client_is_reused(self) -> None:
        client = DoubaoClient(api_key="key", config_path=Path("missing.yaml"))
        fake_sdk_client = object()
        fake_module = SimpleNamespace(Ark=lambda **_: fake_sdk_client)

        with patch.dict(sys.modules, {"volcenginesdkarkruntime": fake_module}):
            first = client._get_client()
            second = client._get_client()

        self.assertIs(first, second)

    def test_runner_call_writes_audit_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            audit_path = Path(temp_dir) / "artifacts" / "audit" / "doubao-api.jsonl"
            client = DoubaoClient(
                runner=lambda _: {"success": True, "content": "ok", "usage": None},
                config_path=Path("missing.yaml"),
                audit_log_path=audit_path,
            )

            result = client.submit([{"role": "user", "content": "hi"}]).result()

            self.assertTrue(result["success"])
            lines = audit_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(1, len(lines))
            payload = json.loads(lines[0])
            self.assertEqual("doubao", payload["provider"])
            self.assertEqual("runner", payload["source"])
            self.assertTrue(payload["success"])
            self.assertEqual(["user"], payload["message_roles"])
            self.assertEqual(2, payload["response_chars"])
            prompt_path = Path(payload["prompt_path"])
            response_path = Path(payload["response_path"])
            self.assertTrue(prompt_path.exists())
            self.assertTrue(response_path.exists())
            self.assertIn('"content": "hi"', prompt_path.read_text(encoding="utf-8"))
            self.assertEqual("ok", response_path.read_text(encoding="utf-8"))

    def test_failed_sdk_call_writes_audit_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            audit_path = Path(temp_dir) / "artifacts" / "audit" / "doubao-api.jsonl"
            fake_sdk_client = SimpleNamespace(
                chat=SimpleNamespace(
                    completions=SimpleNamespace(
                        create=lambda **_: (_ for _ in ()).throw(RuntimeError("boom")),
                    )
                )
            )
            client = DoubaoClient(
                api_key="key",
                config_path=Path("missing.yaml"),
                audit_log_path=audit_path,
            )

            with patch.object(DoubaoClient, "_get_client", return_value=fake_sdk_client):
                result = client.submit([{"role": "user", "content": "hi"}]).result()

            self.assertFalse(result["success"])
            lines = audit_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(1, len(lines))
            payload = json.loads(lines[0])
            self.assertEqual("ark_sdk", payload["source"])
            self.assertFalse(payload["success"])
            self.assertEqual("boom", payload["error"])
            prompt_path = Path(payload["prompt_path"])
            response_path = Path(payload["response_path"])
            error_path = Path(payload["error_path"])
            self.assertTrue(prompt_path.exists())
            self.assertTrue(response_path.exists())
            self.assertTrue(error_path.exists())
            self.assertIn('"content": "hi"', prompt_path.read_text(encoding="utf-8"))
            self.assertEqual("", response_path.read_text(encoding="utf-8"))
            self.assertEqual("boom", error_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
