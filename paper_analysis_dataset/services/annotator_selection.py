from __future__ import annotations

import json
import os
from concurrent.futures import Future
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from paper_analysis_dataset.services.codex_annotator import CodexCliAnnotator
from paper_analysis_dataset.services.doubao_annotator import DoubaoAnnotator
from paper_analysis_dataset.shared.clients.doubao_client import DEFAULT_CONFIG_DIR_NAME


ANNOTATOR_BACKENDS = ("codex_cli", "doubao")
DEFAULT_ANNOTATOR_BACKEND = "codex_cli"
DEFAULT_CODEX_CLI_MODEL = "gpt-5.1-codex-mini"
ANNOTATOR_BACKEND_ENV = "PAPER_ANALYSIS_ANNOTATOR_BACKEND"
ANNOTATOR_SELECTION_FILE_NAME = "annotation_backend.json"


class AnnotatorBackend(Protocol):
    labeler_id: str

    def submit_annotate(self, candidate: object) -> Future[object]: ...


@dataclass(slots=True)
class AnnotatorSelection:
    selected_backend: str
    updated_at: str
    source: str
    sample_size: int | None = None
    seed: int | None = None
    report_path: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def build_annotator(
    backend: str | None = None,
    *,
    selection_config_path: Path | None = None,
    codex_runner: object | None = None,
    doubao_runner: object | None = None,
    concurrency: int = 1,
) -> AnnotatorBackend:
    resolved_backend = resolve_annotation_backend(
        backend=backend,
        selection_config_path=selection_config_path,
    )
    if resolved_backend == "codex_cli":
        return CodexCliAnnotator(
            runner=codex_runner,
            model=DEFAULT_CODEX_CLI_MODEL,
            concurrency=concurrency,
            labeler_id="codex_cli",
        )
    if resolved_backend == "doubao":
        return DoubaoAnnotator(
            runner=doubao_runner,
            concurrency=concurrency,
            labeler_id="doubao",
        )
    raise ValueError(f"不支持的预标后端：{resolved_backend}")


def resolve_annotation_backend(
    *,
    backend: str | None = None,
    selection_config_path: Path | None = None,
) -> str:
    candidate = backend or os.getenv(ANNOTATOR_BACKEND_ENV)
    if candidate:
        return _validate_backend(candidate)
    selection = read_annotator_selection(selection_config_path)
    if selection is not None:
        return selection.selected_backend
    return DEFAULT_ANNOTATOR_BACKEND


def read_annotator_selection(selection_config_path: Path | None = None) -> AnnotatorSelection | None:
    path = selection_config_path or default_annotator_selection_path()
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    selected_backend = _validate_backend(str(payload.get("selected_backend", "")))
    return AnnotatorSelection(
        selected_backend=selected_backend,
        updated_at=str(payload.get("updated_at", "")),
        source=str(payload.get("source", "")),
        sample_size=(int(payload["sample_size"]) if payload.get("sample_size") is not None else None),
        seed=(int(payload["seed"]) if payload.get("seed") is not None else None),
        report_path=(str(payload["report_path"]) if payload.get("report_path") is not None else None),
    )


def write_annotator_selection(
    selected_backend: str,
    *,
    selection_config_path: Path | None = None,
    source: str,
    sample_size: int | None = None,
    seed: int | None = None,
    report_path: Path | None = None,
) -> Path:
    path = selection_config_path or default_annotator_selection_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    selection = AnnotatorSelection(
        selected_backend=_validate_backend(selected_backend),
        updated_at=datetime.now(timezone.utc).isoformat(),
        source=source,
        sample_size=sample_size,
        seed=seed,
        report_path=str(report_path) if report_path is not None else None,
    )
    path.write_text(json.dumps(selection.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def default_annotator_selection_path() -> Path:
    config_root = os.getenv("PAPER_ANALYSIS_DATASET_HOME")
    if config_root:
        return Path(config_root) / ANNOTATOR_SELECTION_FILE_NAME
    return Path.home() / DEFAULT_CONFIG_DIR_NAME / ANNOTATOR_SELECTION_FILE_NAME


def _validate_backend(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in ANNOTATOR_BACKENDS:
        raise ValueError(f"预标后端非法：{value}；允许值：{', '.join(ANNOTATOR_BACKENDS)}")
    return normalized
