from __future__ import annotations

from dataclasses import dataclass


TARGET_LABEL = "系统与调度优化"
DEFAULT_TARGET_POSITIVE_COUNT = 100


@dataclass(slots=True)
class SchedulingAugmentationPlan:
    target_label: str
    current_positive_count: int
    target_positive_count: int
    gap: int
    venue_priority: tuple[str, ...]

    @property
    def completed(self) -> bool:
        return self.gap <= 0

    def to_dict(self) -> dict[str, object]:
        return {
            "target_positive_counts": {self.target_label: self.target_positive_count},
            "current_positive_counts": {self.target_label: self.current_positive_count},
            "gap_by_label": {self.target_label: self.gap},
            "venue_priority": list(self.venue_priority),
        }


def build_scheduling_augmentation_plan(
    stats: dict[str, object],
    *,
    target_positive_count: int = DEFAULT_TARGET_POSITIVE_COUNT,
    venue_priority: tuple[str, ...],
) -> SchedulingAugmentationPlan:
    current_positive_count = _read_positive_count(stats)
    gap = max(0, target_positive_count - current_positive_count)
    return SchedulingAugmentationPlan(
        target_label=TARGET_LABEL,
        current_positive_count=current_positive_count,
        target_positive_count=target_positive_count,
        gap=gap,
        venue_priority=venue_priority,
    )


def _read_positive_count(stats: dict[str, object]) -> int:
    merged_count = _read_nested_positive_count(stats, "by_layer", "merged", "by_preference_label", TARGET_LABEL)
    if merged_count > 0:
        return merged_count
    return _read_nested_positive_count(stats, "by_preference_label", TARGET_LABEL)


def _read_nested_positive_count(stats: dict[str, object], *keys: str) -> int:
    current: object = stats
    for key in keys:
        if not isinstance(current, dict):
            return 0
        current = current.get(key, {})
    if not isinstance(current, dict):
        return 0
    return int(current.get("positive", 0))
