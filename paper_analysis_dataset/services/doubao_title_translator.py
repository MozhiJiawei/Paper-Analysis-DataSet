from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from paper_analysis_dataset.domain.benchmark import CandidatePaper
from paper_analysis_dataset.shared.clients.doubao_client import DoubaoClient


Runner = Callable[[list[dict[str, Any]]], dict[str, Any]]


@dataclass(slots=True)
class DoubaoTitleTranslator:
    runner: Runner | None = None
    api_key: str | None = None
    base_url: str | None = None
    model: str | None = None
    config_path: Path | None = None
    concurrency: int = 1
    _client: DoubaoClient = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._client = DoubaoClient(
            runner=self.runner,
            api_key=self.api_key,
            base_url=self.base_url,
            model=self.model,
            config_path=self.config_path,
            concurrency=self.concurrency,
        )

    def submit_translate(self, candidate: CandidatePaper) -> Future[str]:
        outer_future: Future[str] = Future()
        title = candidate.title.strip()
        if not title:
            outer_future.set_result("")
            return outer_future
        messages = build_doubao_title_translation_messages(candidate)
        inner_future = self._client.submit(messages, stream=False)
        inner_future.add_done_callback(lambda done: self._handle_translation_result(done, outer_future))
        return outer_future

    def _handle_translation_result(
        self,
        inner_future: Future[dict[str, Any]],
        outer_future: Future[str],
    ) -> None:
        if outer_future.done():
            return
        try:
            result = inner_future.result()
            if not result.get("success"):
                raise RuntimeError(f"Doubao 中文标题生成失败：{result.get('error', '未知错误')}")
            outer_future.set_result(parse_doubao_title_translation_payload(str(result.get("content", ""))))
        except Exception as exc:
            outer_future.set_exception(exc)


def build_doubao_title_translation_messages(candidate: CandidatePaper) -> list[dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "你是论文标题翻译助手。"
                "你的任务是把英文论文标题忠实翻译成简体中文。"
                "只输出中文标题正文，不要输出前后缀、解释、引号、项目符号、Markdown、代码块或 JSON。"
            ),
        },
        {
            "role": "user",
            "content": f"title={candidate.title}",
        },
    ]


def parse_doubao_title_translation_payload(payload: str) -> str:
    text = payload.strip()
    if not text:
        raise ValueError("Doubao 未返回中文标题")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        raise ValueError("Doubao 未返回中文标题")
    normalized = " ".join(lines).strip()
    lowered = normalized.lower()
    if "```" in normalized:
        raise ValueError("Doubao 中文标题输出格式非法")
    if any(fragment in lowered for fragment in ("translation:", "here is")):
        raise ValueError("Doubao 中文标题包含附加说明")
    if any(fragment in normalized for fragment in ("中文标题：", "以下是", "标题翻译：")):
        raise ValueError("Doubao 中文标题包含附加说明")
    if not _contains_cjk(normalized):
        raise ValueError("Doubao 中文标题缺少中文内容")
    return normalized


def _contains_cjk(value: str) -> bool:
    return any("\u4e00" <= char <= "\u9fff" for char in value)
