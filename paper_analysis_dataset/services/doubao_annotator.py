from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from paper_analysis_dataset.domain.benchmark import AnnotationRecord, CandidatePaper
from paper_analysis_dataset.services.codex_annotator import (
    build_codex_annotation_prompt,
    parse_codex_annotation_payload,
)
from paper_analysis.utils.doubao_client import DoubaoClient


Runner = Callable[[list[dict[str, object]]], dict[str, object]]


@dataclass(slots=True)
class DoubaoAnnotator:
    runner: Runner | None = None
    api_key: str | None = None
    base_url: str | None = None
    model: str | None = None
    config_path: Path | None = None
    concurrency: int = 1
    labeler_id: str = "doubao"
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

    def submit_annotate(self, candidate: CandidatePaper) -> Future[AnnotationRecord]:
        outer_future: Future[AnnotationRecord] = Future()
        prompts = [
            build_doubao_annotation_messages(candidate),
            build_doubao_annotation_messages(candidate, force_decision=True),
        ]
        self._submit_attempt(candidate, prompts, 0, outer_future)
        return outer_future

    def _submit_attempt(
        self,
        candidate: CandidatePaper,
        prompts: list[list[dict[str, str]]],
        index: int,
        outer_future: Future[AnnotationRecord],
    ) -> None:
        inner_future = self._client.submit(prompts[index], stream=False)
        inner_future.add_done_callback(
            lambda done: self._handle_messages_result(candidate, prompts, index, outer_future, done)
        )

    def _handle_messages_result(
        self,
        candidate: CandidatePaper,
        prompts: list[list[dict[str, str]]],
        index: int,
        outer_future: Future[AnnotationRecord],
        inner_future: Future[dict[str, object]],
    ) -> None:
        if outer_future.done():
            return
        try:
            payload = self._extract_payload(inner_future.result())
            data = parse_codex_annotation_payload(payload)
            outer_future.set_result(self._build_annotation(candidate, data))
            return
        except (RuntimeError, ValueError) as exc:
            if index + 1 < len(prompts):
                self._submit_attempt(candidate, prompts, index + 1, outer_future)
                return
            outer_future.set_exception(exc)

    def _extract_payload(self, result: dict[str, object]) -> str:
        if not result.get("success"):
            raise RuntimeError(f"Doubao 预标失败：{result.get('error', '未知错误')}")
        content = str(result.get("content", "")).strip()
        if not content:
            raise ValueError("Doubao 未返回内容")
        return content

    def _build_annotation(
        self,
        candidate: CandidatePaper,
        data: dict[str, object],
    ) -> AnnotationRecord:
        return AnnotationRecord(
            paper_id=candidate.paper_id,
            labeler_id=self.labeler_id,
            primary_research_object=str(data["primary_research_object"]),
            preference_labels=[str(item) for item in data["preference_labels"]],
            negative_tier=str(data["negative_tier"]),
            evidence_spans={
                str(key): [str(item) for item in value]
                for key, value in dict(data.get("evidence_spans", {})).items()
            },
            notes=str(data.get("notes", "")),
            review_status="pending",
        )


def build_doubao_annotation_messages(
    candidate: CandidatePaper,
    *,
    force_decision: bool = False,
) -> list[dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "你是论文预标助手。"
                "你必须严格返回一个 JSON 对象，不要解释，不要提问，不要输出 Markdown。"
            ),
        },
        {
            "role": "user",
            "content": build_codex_annotation_prompt(candidate, force_decision=force_decision),
        },
    ]
