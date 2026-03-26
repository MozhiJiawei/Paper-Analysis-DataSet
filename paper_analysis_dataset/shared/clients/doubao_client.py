from __future__ import annotations

import json
import os
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import yaml

from paper_analysis_dataset.shared.paths import ARTIFACTS_DIR, DATASET_ROOT_DIR


DEFAULT_BASE_URL = "https://ark.cn-beijing.volces.com/api/v3"
DEFAULT_MODEL = "doubao-seed-2-0-pro-260215"
DEFAULT_CONFIG_DIR_NAME = ".paper-analysis-dataset"
DEFAULT_CONFIG_FILE_NAME = "doubao.yaml"
TEMPLATE_CONFIG_PATH = DATASET_ROOT_DIR / "config" / "doubao.template.yaml"
DEFAULT_AUDIT_LOG_PATH = ARTIFACTS_DIR / "audit" / "doubao-api.jsonl"
DEFAULT_AUDIT_REQUESTS_DIR = ARTIFACTS_DIR / "audit" / "doubao-api" / "requests"
Runner = Callable[[list[dict[str, Any]]], dict[str, Any]]
_AUDIT_LOG_LOCK = threading.Lock()


@dataclass(slots=True)
class DoubaoConfig:
    api_key: str | None = None
    base_url: str = DEFAULT_BASE_URL
    model: str = DEFAULT_MODEL


@dataclass(slots=True)
class DoubaoUsage:
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None


@dataclass(slots=True)
class DoubaoResponse:
    success: bool
    content: str | None
    error: str | None = None
    usage: DoubaoUsage | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        if self.usage is None:
            payload["usage"] = None
        return payload


@dataclass(slots=True)
class DoubaoClient:
    runner: Runner | None = None
    api_key: str | None = None
    base_url: str | None = None
    model: str | None = None
    config_path: Path | None = None
    audit_log_path: Path | None = None
    concurrency: int = 1
    _thread_local: threading.local = field(init=False, repr=False)
    _config: DoubaoConfig = field(init=False, repr=False)
    _executor: ThreadPoolExecutor | None = field(init=False, default=None, repr=False)
    _executor_lock: threading.Lock = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.concurrency = _validate_concurrency(self.concurrency)
        config_payload = self._load_config()
        self._config = DoubaoConfig(
            api_key=self.api_key or os.getenv("ARK_API_KEY") or config_payload.get("api_key"),
            base_url=self.base_url or config_payload.get("base_url", DEFAULT_BASE_URL),
            model=self.model or config_payload.get("model", DEFAULT_MODEL),
        )
        self._thread_local = threading.local()
        self._executor_lock = threading.Lock()

    @property
    def resolved_api_key(self) -> str | None:
        return self._config.api_key

    @property
    def resolved_base_url(self) -> str:
        return self._config.base_url

    @property
    def resolved_model(self) -> str:
        return self._config.model

    def submit(
        self,
        messages: list[dict[str, Any]],
        *,
        stream: bool = False,
    ) -> Future[dict[str, Any]]:
        return self._get_executor().submit(self._run_chat_sync, messages, stream=stream)

    def _run_chat_sync(self, messages: list[dict[str, Any]], *, stream: bool = False) -> dict[str, Any]:
        started_at = time.perf_counter()
        request_id = str(uuid.uuid4())
        source = "runner" if self.runner is not None else "ark_sdk"
        if self.runner is not None:
            result = self.runner(messages)
            self._write_audit_log(
                request_id=request_id,
                messages=messages,
                response=result,
                stream=stream,
                duration_ms=_duration_ms(started_at),
                source=source,
            )
            return result
        if not self.resolved_api_key:
            raise ValueError(
                "Doubao API 密钥未设置，请检查 ARK_API_KEY，或在用户私有配置中创建 "
                f"{_default_config_path()}（可参考模板 {TEMPLATE_CONFIG_PATH}）。"
            )
        client = self._get_client()
        try:
            response = client.chat.completions.create(
                model=self.resolved_model,
                messages=messages,
                stream=stream,
            )
            normalized = self._normalize_response(response, stream=stream)
        except Exception as exc:
            normalized = DoubaoResponse(success=False, content=None, error=str(exc))
        result = normalized.to_dict()
        self._write_audit_log(
            request_id=request_id,
            messages=messages,
            response=result,
            stream=stream,
            duration_ms=_duration_ms(started_at),
            source=source,
        )
        return result

    def _get_executor(self) -> ThreadPoolExecutor:
        if self._executor is not None:
            return self._executor
        with self._executor_lock:
            if self._executor is None:
                self._executor = ThreadPoolExecutor(max_workers=self.concurrency)
        return self._executor

    def _get_client(self) -> Any:
        existing = getattr(self._thread_local, "client", None)
        if existing is not None:
            return existing
        from volcenginesdkarkruntime import Ark

        client = Ark(
            base_url=self.resolved_base_url,
            timeout=1800,
            api_key=self.resolved_api_key,
        )
        self._thread_local.client = client
        return client

    def _load_config(self) -> dict[str, Any]:
        config_path = self.config_path or _default_config_path()
        if not config_path.exists():
            return {}
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        doubao_config = payload.get("doubao")
        return dict(doubao_config) if isinstance(doubao_config, dict) else {}

    def _normalize_response(self, response: Any, *, stream: bool) -> DoubaoResponse:
        if stream:
            return self._normalize_streaming_response(response)
        content = ""
        if response.choices and response.choices[0].message is not None:
            content = response.choices[0].message.content or ""
        return DoubaoResponse(
            success=True,
            content=content,
            usage=_extract_usage(response),
        )

    def _normalize_streaming_response(self, response: Any) -> DoubaoResponse:
        chunks: list[str] = []
        usage: DoubaoUsage | None = None
        with response:
            for chunk in response:
                if chunk.choices and chunk.choices[0].delta.content is not None:
                    chunks.append(chunk.choices[0].delta.content)
                usage = _extract_usage(chunk) or usage
        if usage is None:
            usage = _extract_usage(response)
        return DoubaoResponse(
            success=True,
            content="".join(chunks),
            usage=usage,
        )

    def _write_audit_log(
        self,
        *,
        request_id: str,
        messages: list[dict[str, Any]],
        response: dict[str, Any],
        stream: bool,
        duration_ms: int,
        source: str,
    ) -> None:
        audit_path = self.audit_log_path or DEFAULT_AUDIT_LOG_PATH
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        request_dir = self._resolve_request_dir(audit_path, request_id)
        prompt_path = request_dir / "prompt.md"
        response_path = request_dir / "response.md"
        error_path = request_dir / "error.txt"
        prompt_path.write_text(_render_prompt_markdown(messages), encoding="utf-8")
        response_content = str(response.get("content", "") or "")
        response_path.write_text(response_content, encoding="utf-8")
        error_value = response.get("error")
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "request_id": request_id,
            "provider": "doubao",
            "source": source,
            "model": self.resolved_model,
            "base_url": self.resolved_base_url,
            "stream": stream,
            "message_count": len(messages),
            "message_roles": [str(message.get("role", "")) for message in messages],
            "success": bool(response.get("success")),
            "error": response.get("error"),
            "duration_ms": duration_ms,
            "response_chars": len(response_content),
            "prompt_path": str(prompt_path),
            "response_path": str(response_path),
            "usage": response.get("usage"),
        }
        if error_value:
            error_path.write_text(str(error_value), encoding="utf-8")
            payload["error_path"] = str(error_path)
        line = json.dumps(payload, ensure_ascii=False)
        with _AUDIT_LOG_LOCK:
            with audit_path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")

    def _resolve_request_dir(self, audit_path: Path, request_id: str) -> Path:
        if self.audit_log_path is not None:
            requests_root = audit_path.parent / "doubao-api" / "requests"
        else:
            requests_root = DEFAULT_AUDIT_REQUESTS_DIR
        request_dir = requests_root / request_id
        request_dir.mkdir(parents=True, exist_ok=True)
        return request_dir


def _extract_usage(response: Any) -> DoubaoUsage | None:
    usage = getattr(response, "usage", None)
    if usage is None:
        return None
    return DoubaoUsage(
        prompt_tokens=getattr(usage, "prompt_tokens", None),
        completion_tokens=getattr(usage, "completion_tokens", None),
        total_tokens=getattr(usage, "total_tokens", None),
    )


def _duration_ms(started_at: float) -> int:
    return int((time.perf_counter() - started_at) * 1000)


def _default_config_path() -> Path:
    config_root = os.getenv("PAPER_ANALYSIS_DATASET_HOME")
    if config_root:
        return Path(config_root) / DEFAULT_CONFIG_FILE_NAME
    return Path.home() / DEFAULT_CONFIG_DIR_NAME / DEFAULT_CONFIG_FILE_NAME


def _render_prompt_markdown(messages: list[dict[str, Any]]) -> str:
    lines = ["# Prompt", ""]
    for index, message in enumerate(messages, start=1):
        role = str(message.get("role", ""))
        lines.append(f"## Message {index}")
        lines.append(f"- role: {role}")
        lines.append("")
        lines.append("```json")
        lines.append(json.dumps(message, ensure_ascii=False, indent=2))
        lines.append("```")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _validate_concurrency(value: int) -> int:
    if 1 <= value <= 10:
        return value
    raise ValueError(f"Doubao 并发非法：{value}；允许范围：1~10")
