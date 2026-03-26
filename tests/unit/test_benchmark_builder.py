from __future__ import annotations

from concurrent.futures import Future
import unittest

from paper_analysis_dataset.services.benchmark_builder import BenchmarkBuilder
from paper_analysis_dataset.shared.paths import SUPERPROJECT_ROOT_DIR



class _FakeTranslator:
    def submit_translate(self, candidate: object) -> Future[str]:
        future: Future[str] = Future()
        future.set_result("中文摘要：" + str(candidate.title))
        return future


class BenchmarkBuilderTests(unittest.TestCase):
    def test_build_candidates_from_paperlists_fixture(self) -> None:
        """验证 benchmark builder 能从指定会议构建候选样本。"""

        builder = BenchmarkBuilder(SUPERPROJECT_ROOT_DIR / "tests" / "fixtures" / "paperlists_repo")
        candidates = builder.build_candidates((("iclr", 2025),), limit_per_venue=2)

        self.assertEqual(2, len(candidates))
        self.assertEqual("conference", candidates[0].source)
        self.assertEqual("ICLR 2025", candidates[0].venue)
        self.assertTrue(candidates[0].primary_research_object)

    def test_validate_release_dataset_flags_no_duplicates(self) -> None:
        """验证单版本 records 校验能返回正样本与负样本统计。"""

        builder = BenchmarkBuilder(SUPERPROJECT_ROOT_DIR / "third_party" / "paperlists")
        candidates = builder.build_candidates(limit_per_venue=10)
        records = builder.build_records(candidates[:30], abstract_translator=_FakeTranslator())
        summary = builder.validate_release_dataset(records)

        self.assertEqual(30, summary.total_records)
        self.assertEqual([], summary.duplicate_paper_ids)
        self.assertTrue(all(count >= 0 for count in summary.label_positive_counts.values()))
        self.assertTrue(all(count >= 0 for count in summary.label_negative_counts.values()))
        self.assertTrue(all(record.abstract_zh.startswith("中文摘要：") for record in records))

    def test_build_inference_acceleration_candidates_returns_balanced_release_size(self) -> None:
        """验证推理加速 benchmark 候选构建会稳定返回约 200 篇平衡样本。"""

        builder = BenchmarkBuilder(SUPERPROJECT_ROOT_DIR / "third_party" / "paperlists")
        candidates = builder.build_inference_acceleration_candidates()

        self.assertEqual(200, len(candidates))
        venues = {candidate.venue for candidate in candidates}
        self.assertEqual(
            {"AAAI 2025", "ICLR 2025", "ICLR 2026", "ICML 2025", "NIPS 2025"},
            venues,
        )
        self.assertTrue(any(candidate.candidate_negative_tier == "positive" for candidate in candidates))
        self.assertTrue(any(candidate.candidate_negative_tier == "negative" for candidate in candidates))
        self.assertTrue(all(len(candidate.candidate_preference_labels) <= 1 for candidate in candidates))


if __name__ == "__main__":
    unittest.main()
