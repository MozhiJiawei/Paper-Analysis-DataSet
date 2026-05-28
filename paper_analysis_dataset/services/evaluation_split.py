from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
from typing import Any

from paper_analysis_dataset.services.annotation_repository import AnnotationRepository


SPLIT_DEV = "dev"
SPLIT_DEV_VALIDATION = "dev_validation"
SPLIT_TEST = "test"
SPLIT_NAMES = (SPLIT_DEV, SPLIT_DEV_VALIDATION, SPLIT_TEST)
DEFAULT_SPLIT_SEED = 42
DEFAULT_SPLIT_RATIOS = {
    SPLIT_DEV: 0.70,
    SPLIT_DEV_VALIDATION: 0.15,
    SPLIT_TEST: 0.15,
}


@dataclass(slots=True)
class SplitAssignmentSummary:
    assigned_count: int
    eligible_count: int
    already_assigned_count: int
    skipped_without_record_count: int
    counts_by_split: dict[str, int]
    manifest_path: Path

    def to_dict(self) -> dict[str, object]:
        return {
            "assigned_count": self.assigned_count,
            "eligible_count": self.eligible_count,
            "already_assigned_count": self.already_assigned_count,
            "skipped_without_record_count": self.skipped_without_record_count,
            "counts_by_split": self.counts_by_split,
            "manifest_path": str(self.manifest_path),
        }


def assign_new_merged_papers_to_splits(
    repository: AnnotationRepository,
    *,
    seed: int = DEFAULT_SPLIT_SEED,
) -> SplitAssignmentSummary:
    manifest = load_split_manifest(repository)
    splits = _manifest_splits(manifest)
    assigned_ids = {
        paper_id
        for split_name in SPLIT_NAMES
        for paper_id in splits[split_name]
    }
    record_ids = set(repository.load_record_map())
    merged_ids = {
        annotation.paper_id
        for annotation in repository.load_annotations(repository.merged_path)
    }
    eligible_ids = sorted(merged_ids & record_ids)
    new_ids = [paper_id for paper_id in eligible_ids if paper_id not in assigned_ids]
    skipped_without_record_count = len(merged_ids - record_ids)

    for paper_id in new_ids:
        splits[_choose_split(paper_id, seed=seed)].append(paper_id)

    for split_name in SPLIT_NAMES:
        splits[split_name] = sorted(set(splits[split_name]))

    next_manifest = {
        "version": 1,
        "seed": seed,
        "ratios": DEFAULT_SPLIT_RATIOS,
        "splits": splits,
    }
    repository.write_json(next_manifest, repository.split_manifest_path)
    return SplitAssignmentSummary(
        assigned_count=len(new_ids),
        eligible_count=len(eligible_ids),
        already_assigned_count=len(eligible_ids) - len(new_ids),
        skipped_without_record_count=skipped_without_record_count,
        counts_by_split={split_name: len(splits[split_name]) for split_name in SPLIT_NAMES},
        manifest_path=repository.split_manifest_path,
    )


def load_split_manifest(repository: AnnotationRepository) -> dict[str, object]:
    if not repository.split_manifest_path.exists():
        return {
            "version": 1,
            "seed": DEFAULT_SPLIT_SEED,
            "ratios": DEFAULT_SPLIT_RATIOS,
            "splits": {split_name: [] for split_name in SPLIT_NAMES},
        }
    payload = repository.read_json(repository.split_manifest_path)
    return {
        "version": int(payload.get("version", 1)),
        "seed": int(payload.get("seed", DEFAULT_SPLIT_SEED)),
        "ratios": dict(payload.get("ratios", DEFAULT_SPLIT_RATIOS)),
        "splits": _manifest_splits(payload),
    }


def paper_split_map(repository: AnnotationRepository) -> dict[str, str]:
    manifest = load_split_manifest(repository)
    splits = _manifest_splits(manifest)
    return {
        paper_id: split_name
        for split_name in SPLIT_NAMES
        for paper_id in splits[split_name]
    }


def split_counts(repository: AnnotationRepository) -> dict[str, int]:
    manifest = load_split_manifest(repository)
    splits = _manifest_splits(manifest)
    return {split_name: len(splits[split_name]) for split_name in SPLIT_NAMES}


def pending_split_assignment_count(repository: AnnotationRepository) -> int:
    assigned_ids = set(paper_split_map(repository))
    record_ids = set(repository.load_record_map())
    merged_ids = {
        annotation.paper_id
        for annotation in repository.load_annotations(repository.merged_path)
    }
    return len((merged_ids & record_ids) - assigned_ids)


def _manifest_splits(payload: dict[str, Any]) -> dict[str, list[str]]:
    raw_splits = payload.get("splits", {})
    if not isinstance(raw_splits, dict):
        raw_splits = {}
    return {
        split_name: sorted(
            {
                str(paper_id).strip()
                for paper_id in raw_splits.get(split_name, [])
                if str(paper_id).strip()
            }
        )
        for split_name in SPLIT_NAMES
    }


def _choose_split(paper_id: str, *, seed: int) -> str:
    digest = hashlib.sha256(f"{seed}:{paper_id}".encode("utf-8")).hexdigest()
    bucket = int(digest[:12], 16) / float(0xFFFFFFFFFFFF)
    if bucket < DEFAULT_SPLIT_RATIOS[SPLIT_DEV]:
        return SPLIT_DEV
    if bucket < DEFAULT_SPLIT_RATIOS[SPLIT_DEV] + DEFAULT_SPLIT_RATIOS[SPLIT_DEV_VALIDATION]:
        return SPLIT_DEV_VALIDATION
    return SPLIT_TEST
