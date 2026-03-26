from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class Paper:
    """Normalized conference paper record used by dataset tools."""

    paper_id: str
    title: str
    abstract: str
    source: str
    venue: str
    authors: list[str]
    tags: list[str]
    organization: str
    published_at: str
    score: float = 0.0
    reasons: list[str] = field(default_factory=list)
    year: int | None = None
    acceptance_status: str = ""
    primary_area: str = ""
    topic: str = ""
    keywords: list[str] = field(default_factory=list)
    pdf_url: str = ""
    project_url: str = ""
    code_url: str = ""
    openreview_url: str = ""
    sampled_reason: str = ""
    source_path: str = ""
    raw_payload: dict[str, Any] = field(default_factory=dict)
