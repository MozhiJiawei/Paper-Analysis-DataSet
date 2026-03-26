from __future__ import annotations

import json
from pathlib import Path
from typing import Callable, TypeVar

from paper_analysis_dataset.domain.benchmark import (
    AnnotationRecord,
    BenchmarkRecord,
    CandidatePaper,
    ConflictRecord,
)
from paper_analysis_dataset.shared.paths import DATA_BENCHMARKS_DIR


T = TypeVar("T")
BENCHMARK_ROOT = DATA_BENCHMARKS_DIR / "paper-filter"


class AnnotationRepository:
    def __init__(self, root_dir: Path | None = None) -> None:
        self.root_dir = root_dir or BENCHMARK_ROOT

    @property
    def records_path(self) -> Path:
        return self.root_dir / "records.jsonl"

    @property
    def annotations_ai_path(self) -> Path:
        return self.root_dir / "annotations-ai.jsonl"

    @property
    def annotations_human_path(self) -> Path:
        return self.root_dir / "annotations-human.jsonl"

    @property
    def merged_path(self) -> Path:
        return self.root_dir / "merged.jsonl"

    @property
    def conflicts_path(self) -> Path:
        return self.root_dir / "conflicts.jsonl"

    @property
    def schema_path(self) -> Path:
        return self.root_dir / "schema.json"

    @property
    def stats_path(self) -> Path:
        return self.root_dir / "stats.json"

    def load_candidates(self) -> list[CandidatePaper]:
        return [record.to_candidate_paper() for record in self.load_records()]

    def write_candidates(self, candidates: list[CandidatePaper]) -> Path:
        records = [
            BenchmarkRecord(
                paper_id=candidate.paper_id,
                title=candidate.title,
                abstract=candidate.abstract,
                abstract_zh=candidate.abstract_zh,
                authors=candidate.authors,
                venue=candidate.venue,
                year=candidate.year,
                source=candidate.source,
                source_path=candidate.source_path,
                primary_research_object=candidate.primary_research_object,
                candidate_preference_labels=candidate.candidate_preference_labels,
                candidate_negative_tier=candidate.candidate_negative_tier,
                keywords=candidate.keywords,
                notes=candidate.notes,
            )
            for candidate in candidates
        ]
        return self.write_records(records)

    def load_record_map(self) -> dict[str, BenchmarkRecord]:
        return {record.paper_id: record for record in self.load_records()}

    def load_records(self, path: Path | None = None) -> list[BenchmarkRecord]:
        return self._read_jsonl(path or self.records_path, BenchmarkRecord.from_dict)

    def write_records(
        self,
        records: list[BenchmarkRecord],
        path: Path | None = None,
        *,
        include_final_annotations: bool = False,
    ) -> Path:
        sorted_records = sorted(records, key=lambda item: item.paper_id)
        return self._write_jsonl(
            path or self.records_path,
            sorted_records,
            include_final_annotations=include_final_annotations,
        )

    def upsert_record(
        self,
        record: BenchmarkRecord,
        path: Path | None = None,
        *,
        include_final_annotations: bool = False,
    ) -> Path:
        target_path = path or self.records_path
        existing = self.load_record_map() if target_path.exists() else {}
        existing[record.paper_id] = record
        return self.write_records(
            list(existing.values()),
            path,
            include_final_annotations=include_final_annotations,
        )

    def load_annotations(self, path: Path) -> list[AnnotationRecord]:
        return self._read_jsonl(path, AnnotationRecord.from_dict)

    def write_annotations(self, annotations: list[AnnotationRecord], path: Path) -> Path:
        return self._write_jsonl(path, sorted(annotations, key=lambda item: item.paper_id))

    def upsert_annotation(self, annotation: AnnotationRecord, path: Path) -> Path:
        existing = {item.paper_id: item for item in self.load_annotations(path)} if path.exists() else {}
        existing[annotation.paper_id] = annotation
        return self.write_annotations(list(existing.values()), path)

    def load_conflicts(self, path: Path) -> list[ConflictRecord]:
        return self._read_jsonl(path, ConflictRecord.from_dict)

    def write_conflicts(self, conflicts: list[ConflictRecord], path: Path) -> Path:
        return self._write_jsonl(path, sorted(conflicts, key=lambda item: item.paper_id))

    def write_json(self, payload: dict[str, object], path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def read_json(self, path: Path) -> dict[str, object]:
        return json.loads(path.read_text(encoding="utf-8"))

    def _read_jsonl(self, path: Path, loader: Callable[[dict[str, object]], T]) -> list[T]:
        if not path.exists():
            return []
        records: list[T] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError(f"JSONL 记录必须是对象：{path}")
            records.append(loader(payload))
        return records

    def _write_jsonl(
        self,
        path: Path,
        records: list[object],
        *,
        include_final_annotations: bool = True,
    ) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = []
        for record in records:
            if not hasattr(record, "to_dict"):
                raise ValueError(f"不支持写入的记录类型：{type(record)}")
            if isinstance(record, BenchmarkRecord):
                payload = record.to_dict(include_final_annotations=include_final_annotations)
            else:
                payload = record.to_dict()
            lines.append(json.dumps(payload, ensure_ascii=False))
        path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
        return path
