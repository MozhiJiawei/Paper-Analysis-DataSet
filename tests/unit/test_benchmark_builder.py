from __future__ import annotations

from concurrent.futures import Future
import unittest
from pathlib import Path

from paper_analysis_dataset.services.benchmark_builder import BenchmarkBuilder


ROOT_DIR = Path(__file__).resolve().parents[2]
FIXTURE_PAPERLISTS_ROOT = ROOT_DIR / "tests" / "fixtures" / "paperlists_repo"



class _FakeTranslator:
    def submit_translate(self, candidate: object) -> Future[str]:
        future: Future[str] = Future()
        future.set_result("中文摘要：" + str(candidate.title))
        return future


class BenchmarkBuilderTests(unittest.TestCase):
    def test_build_candidates_from_paperlists_fixture(self) -> None:
        """验证 benchmark builder 能从指定会议构建候选样本。"""

        builder = BenchmarkBuilder(FIXTURE_PAPERLISTS_ROOT)
        candidates = builder.build_candidates((("iclr", 2025),), limit_per_venue=2)

        self.assertEqual(2, len(candidates))
        self.assertEqual("conference", candidates[0].source)
        self.assertEqual("ICLR 2025", candidates[0].venue)
        self.assertTrue(candidates[0].primary_research_object)
        self.assertEqual(["iclr25-001", "iclr25-002"], [candidate.paper_id for candidate in candidates])

    def test_validate_release_dataset_flags_no_duplicates(self) -> None:
        """验证单版本 records 校验能返回正样本与负样本统计。"""

        builder = BenchmarkBuilder(FIXTURE_PAPERLISTS_ROOT)
        candidates = builder.build_candidates((("iclr", 2025),), limit_per_venue=2)
        records = builder.build_records(candidates, abstract_translator=_FakeTranslator())
        summary = builder.validate_release_dataset(records)

        self.assertEqual(2, summary.total_records)
        self.assertEqual([], summary.duplicate_paper_ids)
        self.assertTrue(all(count >= 0 for count in summary.label_positive_counts.values()))
        self.assertTrue(all(count >= 0 for count in summary.label_negative_counts.values()))
        self.assertTrue(all(record.abstract_zh.startswith("中文摘要：") for record in records))

    def test_build_inference_acceleration_candidates_supports_fixture_dataset(self) -> None:
        """验证推理加速 benchmark 候选构建可基于子仓 fixture 独立运行。"""

        builder = BenchmarkBuilder(FIXTURE_PAPERLISTS_ROOT)
        candidates = builder.build_inference_acceleration_candidates(
            quota_by_venue={
                ("aaai", 2025): 1,
                ("iclr", 2025): 1,
                ("iclr", 2026): 1,
                ("icml", 2025): 1,
                ("nips", 2025): 1,
            },
            minimum_score=6,
        )

        self.assertEqual(5, len(candidates))
        venues = {candidate.venue for candidate in candidates}
        self.assertEqual(
            {"AAAI 2025", "ICLR 2025", "ICLR 2026", "ICML 2025", "NIPS 2025"},
            venues,
        )
        self.assertTrue(any(candidate.candidate_negative_tier == "positive" for candidate in candidates))
        self.assertTrue(all(len(candidate.candidate_preference_labels) <= 1 for candidate in candidates))

    def test_build_candidates_only_keeps_accepted_records(self) -> None:
        """验证 withdrawn 等非 accepted 记录不会进入候选池。"""

        builder = BenchmarkBuilder(FIXTURE_PAPERLISTS_ROOT)
        candidates = builder.build_candidates((("iclr", 2025),))

        self.assertEqual(["iclr25-001", "iclr25-002"], [candidate.paper_id for candidate in candidates])

    def test_build_scheduling_positive_candidates_uses_targeted_scoring(self) -> None:
        """验证调度专项候选会命中系统与调度优化单标签。"""

        builder = BenchmarkBuilder(FIXTURE_PAPERLISTS_ROOT)
        candidates = builder.build_scheduling_positive_candidates(
            venue_targets=(("icml", 2025),),
            minimum_score=1,
        )

        self.assertEqual(1, len(candidates))
        self.assertEqual(["系统与调度优化"], candidates[0].candidate.candidate_preference_labels)
        self.assertGreaterEqual(candidates[0].score, 1)


if __name__ == "__main__":
    unittest.main()
