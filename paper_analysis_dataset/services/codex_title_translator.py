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


@dataclass(slots=True)
class CodexTitleTranslator:
    client: CodexCliClient | None = None
    runner: Runner | None = None
    concurrency: int = 1
    _client: CodexCliClient = field(init=False, repr=False)
    model: str = field(init=False)

    def __post_init__(self) -> None:
        self.model = DEFAULT_CODEX_CLI_MODEL
        self._client = self.client or CodexCliClient(
            runner=self.runner,
            model=self.model,
            concurrency=self.concurrency,
        )

    def submit_translate(self, candidate: CandidatePaper) -> Future[str]:
        outer_future: Future[str] = Future()
        title = candidate.title.strip()
        if not title:
            outer_future.set_result("")
            return outer_future
        inner_future = self._client.submit(build_codex_title_translation_prompt(candidate))
        inner_future.add_done_callback(lambda done: self._handle_result(done, outer_future))
        return outer_future

    def _handle_result(self, inner_future: Future[str], outer_future: Future[str]) -> None:
        if outer_future.done():
            return
        try:
            outer_future.set_result(parse_codex_title_translation_payload(inner_future.result()))
        except Exception as exc:
            outer_future.set_exception(exc)


def build_codex_title_translation_prompt(candidate: CandidatePaper) -> str:
    return " ".join(
        [
            "你是论文标题翻译助手。",
            "请把给定英文论文标题忠实翻译成简体中文。",
            "只输出中文标题正文，不要输出前后缀、解释、引号、项目符号、Markdown、代码块或 JSON。",
            "如果标题里包含模型名、方法名、缩写或数据集名，请尽量保留原始专有名词，并用自然中文组织句子。",
            f"title={_normalize_prompt_text(candidate.title)}",
        ]
    )


def parse_codex_title_translation_payload(payload: str) -> str:
    text = payload.strip()
    if not text:
        raise ValueError("Codex CLI 未返回中文标题")
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
        if lowered in {"translation", "中文标题", "标题翻译", "chinese title"}:
            continue
        if lowered.startswith("here is") and not _contains_cjk(line):
            continue
        lines.append(line)
    if not lines:
        raise ValueError("Codex CLI 未返回中文标题")
    normalized = " ".join(lines).strip()
    if not _contains_cjk(normalized):
        raise ValueError("Codex CLI 中文标题缺少中文内容")
    return normalized


def _extract_text_from_event_stream(payload: str) -> str | None:
    for line in reversed(payload.splitlines()):
        stripped = line.strip()
        if not stripped.startswith("{"):
            continue
        try:
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
            for key in ("title_zh", "translation", "content", "text"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
    return stripped


def _strip_known_prefix(line: str) -> str:
    stripped = line.strip().lstrip("-*").strip()
    prefixes = (
        "中文标题：",
        "标题翻译：",
        "翻译：",
        "translation:",
        "translated title:",
        "chinese title:",
        "以下是中文标题：",
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
