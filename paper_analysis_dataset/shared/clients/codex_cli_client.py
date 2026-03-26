from __future__ import annotations

import os
import subprocess
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable


Runner = Callable[[str], str]


@dataclass(slots=True)
class CodexCliClient:
    """轻量 Codex CLI 异步调用器，可被多个线程共享复用。"""

    runner: Runner | None = None
    cwd: Path | None = None
    timeout: int | None = None
    model: str | None = None
    json_mode: bool = False
    ephemeral: bool = False
    concurrency: int = 1
    _executor: ThreadPoolExecutor | None = field(init=False, default=None, repr=False)
    _executor_lock: threading.Lock = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.concurrency = _validate_concurrency(self.concurrency)
        self._executor_lock = threading.Lock()

    def submit(self, prompt: str) -> Future[str]:
        return self._get_executor().submit(self._run_prompt_sync, prompt)

    def _run_prompt_sync(self, prompt: str) -> str:
        if self.runner is not None:
            return self.runner(prompt)
        try:
            result = subprocess.run(
                self._build_command(prompt),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
                cwd=self.cwd,
                timeout=self.timeout,
            )
        except subprocess.TimeoutExpired as exc:
            model_detail = f"; model={self.model}" if self.model else ""
            raise RuntimeError(f"Codex CLI 调用超时：timeout={self.timeout}s{model_detail}") from exc
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "未知错误"
            model_detail = f"; model={self.model}" if self.model else ""
            raise RuntimeError(
                f"Codex CLI 调用失败：returncode={result.returncode}{model_detail}; {detail}"
            )
        return result.stdout.strip()

    def _get_executor(self) -> ThreadPoolExecutor:
        if self._executor is not None:
            return self._executor
        with self._executor_lock:
            if self._executor is None:
                self._executor = ThreadPoolExecutor(max_workers=self.concurrency)
        return self._executor

    def _build_command(self, prompt: str) -> list[str]:
        command = [
            "codex.cmd" if os.name == "nt" else "codex",
            "exec",
        ]
        if self.model:
            command.extend(["-m", self.model])
        command.append("--dangerously-bypass-approvals-and-sandbox")
        if self.json_mode:
            command.append("--json")
        if self.ephemeral:
            command.append("--ephemeral")
        command.append(prompt)
        return command


def _validate_concurrency(value: int) -> int:
    if 1 <= value <= 10:
        return value
    raise ValueError(f"Codex CLI 并发非法：{value}；允许范围：1~10")
