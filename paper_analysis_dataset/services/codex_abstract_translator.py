from __future__ import annotations

import json
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Callable

from paper_analysis_dataset.domain.benchmark import CandidatePaper
from paper_analysis_dataset.shared.clients.codex_cli_client import (
    CodexCliClient,
    DEFAULT_CODEX_CLI_MODEL,
)


Runner = Callable[[str], str]
MIN_ABSTRACT_LENGTH_FOR_COMPLETENESS_CHECK = 400
MIN_TRANSLATION_LENGTH_FOR_LONG_ABSTRACT = 120
MIN_TRANSLATION_TO_ABSTRACT_RATIO = 0.12


@dataclass(slots=True)
class CodexAbstractTranslator:
    client: CodexCliClient | None = None
    runner: Runner | None = None
    model: str | None = None
    concurrency: int = 1
    _client: CodexCliClient = field(init=False, repr=False)

    def __post_init__(self) -> None:
        resolved_model = self.model or DEFAULT_CODEX_CLI_MODEL
        self._client = self.client or CodexCliClient(
            runner=self.runner,
            model=resolved_model,
            concurrency=self.concurrency,
        )
        self.model = resolved_model

    def submit_translate(self, candidate: CandidatePaper) -> Future[str]:
        outer_future: Future[str] = Future()
        abstract = candidate.abstract.strip()
        if not abstract:
            outer_future.set_result("")
            return outer_future
        prompts = [
            build_codex_abstract_translation_prompt(candidate),
            build_codex_abstract_translation_prompt(candidate, force_plain_output=True),
            build_codex_abstract_translation_prompt(
                candidate,
                force_plain_output=True,
                force_complete_translation=True,
            ),
        ]
        self._submit_attempt(candidate, prompts, 0, outer_future)
        return outer_future

    def _submit_attempt(
        self,
        candidate: CandidatePaper,
        prompts: list[str],
        index: int,
        outer_future: Future[str],
    ) -> None:
        inner_future = self._client.submit(prompts[index])
        inner_future.add_done_callback(
            lambda done: self._handle_translation_result(candidate, prompts, index, done, outer_future)
        )

    def _handle_translation_result(
        self,
        candidate: CandidatePaper,
        prompts: list[str],
        index: int,
        inner_future: Future[str],
        outer_future: Future[str],
    ) -> None:
        if outer_future.done():
            return
        try:
            translation = parse_codex_abstract_translation_payload(inner_future.result())
            _validate_translation_completeness(candidate, translation)
            outer_future.set_result(translation)
        except ValueError as exc:
            if index + 1 < len(prompts):
                self._submit_attempt(candidate, prompts, index + 1, outer_future)
                return
            outer_future.set_exception(exc)
        except Exception as exc:
            outer_future.set_exception(exc)


def build_codex_abstract_translation_prompt(
    candidate: CandidatePaper,
    *,
    force_plain_output: bool = False,
    force_complete_translation: bool = False,
) -> str:
    retry_guard = (
        "上一次输出不合规。"
        "这一次严禁输出“中文摘要：”“translation:”等任何前缀，严禁输出解释、JSON、代码块或额外句子。"
        if force_plain_output
        else ""
    )
    completeness_guard = (
        "上一次译文过短或疑似只翻译了开头。"
        "这一次必须完整覆盖原摘要全部信息，逐句忠实翻译，不得压缩成一句话概述，不得省略实验结果、方法细节、数据集名称或结论。"
        "如果原摘要较长，译文也必须是完整长摘要。"
        if force_complete_translation
        else ""
    )
    length_guard = (
        "原摘要较长，译文不能只翻译第一句或前两句；"
        "如果译文少于120个中文字符，视为不合格。"
        if len(candidate.abstract.strip()) >= MIN_ABSTRACT_LENGTH_FOR_COMPLETENESS_CHECK
        else ""
    )
    return " ".join(
        [
            "你是论文摘要翻译助手。",
            "请把给定英文摘要忠实翻译成简体中文。",
            "只输出中文摘要正文，不要输出标题、前后缀、解释、引号、项目符号、Markdown、代码块或 JSON。",
            "不要补充原文没有的信息，不要改写成提纲，不要省略关键技术细节。",
            retry_guard,
            completeness_guard,
            length_guard,
            f"title={_normalize_prompt_text(candidate.title)};",
            f"abstract={_normalize_prompt_text(candidate.abstract)}",
        ]
    )


def parse_codex_abstract_translation_payload(payload: str) -> str:
    text = payload.strip()
    if not text:
        raise ValueError("Codex CLI 未返回中文摘要")
    if "\n" in text:
        event_payload = _extract_text_from_event_stream(text)
        if event_payload is not None:
            text = event_payload
    text = _extract_translation_text(text)
    if text.startswith("```") and text.endswith("```"):
        text = text.strip("`").strip()
    lines = []
    for raw_line in text.splitlines():
        line = _strip_known_prefix(raw_line.strip())
        if not line:
            continue
        lowered = line.lower()
        if lowered in {
            "chinese translation",
            "translation",
            "translated abstract",
            "中文摘要",
            "摘要翻译",
        }:
            continue
        if lowered.startswith("here is") and not _contains_cjk(line):
            continue
        lines.append(line)
    if not lines:
        raise ValueError("Codex CLI 未返回中文摘要")
    normalized = "\n".join(lines)
    if not _contains_cjk(normalized):
        raise ValueError("Codex CLI 中文摘要缺少中文内容")
    return normalized


def _extract_text_from_event_stream(payload: str) -> str | None:
    for line in reversed(payload.splitlines()):
        stripped = line.strip()
        if not stripped.startswith("{"):
            continue
        try:
            import json

            event = json.loads(stripped)
        except Exception:
            continue
        if not isinstance(event, dict):
            continue
        item = event.get("item")
        if isinstance(item, dict) and item.get("type") == "agent_message":
            text = item.get("text")
            if isinstance(text, str) and text.strip():
                return text.strip()
    return None


def _extract_translation_text(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            return stripped
        if isinstance(payload, dict):
            for key in ("abstract_zh", "translation", "content", "text"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
    return stripped


def _strip_known_prefix(line: str) -> str:
    stripped = line.strip().lstrip("-*").strip()
    prefixes = (
        "中文摘要：",
        "中文翻译：",
        "摘要翻译：",
        "翻译：",
        "translation:",
        "translated abstract:",
        "chinese translation:",
        "以下是中文翻译：",
        "以下是中文摘要：",
    )
    lowered = stripped.lower()
    for prefix in prefixes:
        if lowered.startswith(prefix.lower()):
            return stripped[len(prefix):].strip()
    return stripped


def _contains_cjk(value: str) -> bool:
    return any("\u4e00" <= char <= "\u9fff" for char in value)


def _normalize_prompt_text(value: str) -> str:
    return " ".join(part.strip() for part in value.splitlines() if part.strip())


def _validate_translation_completeness(candidate: CandidatePaper, translation: str) -> None:
    abstract = candidate.abstract.strip()
    if len(abstract) < MIN_ABSTRACT_LENGTH_FOR_COMPLETENESS_CHECK:
        return
    if len(translation) >= MIN_TRANSLATION_LENGTH_FOR_LONG_ABSTRACT:
        if (len(translation) / max(len(abstract), 1)) >= MIN_TRANSLATION_TO_ABSTRACT_RATIO:
            return
    raise ValueError(
        "Codex CLI 中文摘要疑似截断或过短："
        f"abstract_chars={len(abstract)}, abstract_zh_chars={len(translation)}"
    )
