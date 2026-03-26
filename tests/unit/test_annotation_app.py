from __future__ import annotations

import shutil
import unittest
from io import BytesIO
from pathlib import Path

from paper_analysis_dataset.domain.benchmark import AnnotationRecord, BenchmarkRecord, CandidatePaper
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.web.annotation_app import AnnotationApplication


ROOT_DIR = Path(__file__).resolve().parents[2]


def _candidate(*, paper_id: str, title: str) -> CandidatePaper:
    return CandidatePaper(
        paper_id=paper_id,
        title=title,
        abstract=f"{title} abstract.",
        abstract_zh=f"{title} 中文摘要。",
        authors=["Alice"],
        venue="ICLR 2025",
        year=2025,
        source="conference",
        source_path="tests.json",
        primary_research_object="LLM",
        candidate_preference_labels=["解码策略优化"],
        candidate_negative_tier="positive",
    )


def _ai_annotation(*, paper_id: str, primary_research_object: str = "LLM") -> AnnotationRecord:
    return AnnotationRecord(
        paper_id=paper_id,
        labeler_id="codex_cli",
        primary_research_object=primary_research_object,
        preference_labels=["解码策略优化"],
        negative_tier="positive",
        evidence_spans={"解码策略优化": ["evidence"]},
        review_status="pending",
    )


class AnnotationApplicationTests(unittest.TestCase):
    maxDiff = None

    def test_papers_route_renders_candidate_list(self) -> None:
        """验证标注网页可以渲染候选池列表。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_candidates([_candidate(paper_id="paper-1", title="Annotation App Test")])
        repository.write_annotations([_ai_annotation(paper_id="paper-1")], repository.annotations_ai_path)

        app = AnnotationApplication(repository)
        headers: list[tuple[str, str]] = []

        def start_response(status: str, response_headers: list[tuple[str, str]]) -> None:
            headers.extend(response_headers)
            self.assertIn("200", status)

        response = app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": "/papers",
                "wsgi.input": BytesIO(b""),
                "CONTENT_LENGTH": "0",
            },
            start_response,
        )

        html = b"".join(response).decode("utf-8")
        self.assertIn("候选池列表", html)
        self.assertIn("Annotation App Test", html)
        self.assertIn("状态筛选", html)
        self.assertTrue(any(header[0] == "Content-Type" for header in headers))

    def test_detail_route_shows_ai_annotation_summary(self) -> None:
        """验证单论文页会展示默认折叠的 AI 预标摘要。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-detail"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_candidates([_candidate(paper_id="paper-2", title="Detail Test")])
        repository.write_annotations([_ai_annotation(paper_id="paper-2")], repository.annotations_ai_path)

        app = AnnotationApplication(repository)
        response = app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": "/papers/paper-2",
                "wsgi.input": BytesIO(b""),
                "CONTENT_LENGTH": "0",
            },
            lambda status, response_headers: self.assertIn("200", status),
        )

        html = b"".join(response).decode("utf-8")
        self.assertIn("AI 预标", html)
        self.assertIn("<details class=\"collapsible-panel\">", html)
        self.assertNotIn("<details class=\"collapsible-panel\" open>", html)
        self.assertIn("默认折叠，点击展开", html)
        self.assertIn("解码策略优化", html)
        self.assertIn("人工复标", html)
        self.assertIn("detail-layout", html)
        self.assertIn("data-detail-layout", html)
        self.assertIn("data-abstract-content", html)
        self.assertIn("data-abstract-text", html)
        self.assertIn("中文摘要", html)
        self.assertIn("Detail Test 中文摘要。", html)
        self.assertIn("查看英文摘要", html)
        self.assertIn("默认只展示中文摘要，减少首屏干扰。", html)
        self.assertIn("fitAbstract", html)
        self.assertNotIn("当前选择", html)

    def test_detail_route_shows_fallback_when_chinese_abstract_missing(self) -> None:
        """验证旧记录缺少中文摘要时页面会展示降级文案。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-detail-no-zh"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_candidates(
            [
                CandidatePaper(
                    paper_id="paper-2b",
                    title="Detail Missing Zh",
                    abstract="Only English abstract.",
                    abstract_zh="",
                    authors=["Alice"],
                    venue="ICLR 2025",
                    year=2025,
                    source="conference",
                    source_path="tests.json",
                    primary_research_object="LLM",
                    candidate_preference_labels=["解码策略优化"],
                    candidate_negative_tier="positive",
                )
            ]
        )
        repository.write_annotations([_ai_annotation(paper_id="paper-2b")], repository.annotations_ai_path)

        app = AnnotationApplication(repository)
        response = app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": "/papers/paper-2b",
                "wsgi.input": BytesIO(b""),
                "CONTENT_LENGTH": "0",
            },
            lambda status, response_headers: self.assertIn("200", status),
        )

        html = b"".join(response).decode("utf-8")
        self.assertIn("暂无中文摘要", html)

    def test_detail_route_inherits_ai_preannotation_defaults(self) -> None:
        """验证表单会真实继承 AI 预标的主对象、极性与偏好标签。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-ai-seed-split"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_candidates([_candidate(paper_id="paper-ai-seed", title="AI Seed Split")])
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="paper-ai-seed",
                    labeler_id="codex_cli",
                    primary_research_object="AI 系统 / 基础设施",
                    preference_labels=["模型压缩"],
                    negative_tier="positive",
                    evidence_spans={"general": ["ai evidence"]},
                    notes="ai notes",
                    review_status="pending",
                )
            ],
            repository.annotations_ai_path,
        )

        app = AnnotationApplication(repository)
        response = app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": "/papers/paper-ai-seed",
                "wsgi.input": BytesIO(b""),
                "CONTENT_LENGTH": "0",
            },
            lambda status, response_headers: self.assertIn("200", status),
        )

        html = b"".join(response).decode("utf-8")
        self.assertIn('<option value="AI 系统 / 基础设施" selected>', html)
        self.assertIn('<option value="positive" selected>', html)
        self.assertIn('<input type="radio" name="preference_labels" value="模型压缩" checked>', html)
        self.assertNotIn('name="target_preference_labels"', html)
        self.assertIn('<textarea name="evidence_1" rows="3"></textarea>', html)
        self.assertIn('<textarea name="notes" rows="3"></textarea>', html)

    def test_detail_route_prefers_final_then_human_for_non_core_fields(self) -> None:
        """验证表单按区块优先使用 final，其次 human。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-final-priority"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_records(
            [
                BenchmarkRecord(
                    paper_id="paper-final-seed",
                    title="Final Seed Priority",
                    abstract="Final Seed Priority abstract.",
                    abstract_zh="Final Seed Priority 中文摘要。",
                    authors=["Alice"],
                    venue="ICLR 2025",
                    year=2025,
                    source="conference",
                    source_path="tests.json",
                    primary_research_object="LLM",
                    candidate_preference_labels=["解码策略优化"],
                    candidate_negative_tier="positive",
                )
            ]
        )
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="paper-final-seed",
                    labeler_id="codex_cli",
                    primary_research_object="LLM",
                    preference_labels=["解码策略优化"],
                    negative_tier="positive",
                    evidence_spans={"general": ["ai evidence"]},
                    notes="ai notes",
                    review_status="pending",
                )
            ],
            repository.annotations_ai_path,
        )
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="paper-final-seed",
                    labeler_id="human_reviewer",
                    primary_research_object="多模态 / VLM",
                    preference_labels=["系统与调度优化"],
                    negative_tier="negative",
                    evidence_spans={"general": ["human evidence"]},
                    notes="human notes",
                    review_status="pending",
                )
            ],
            repository.annotations_human_path,
        )
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="paper-final-seed",
                    labeler_id="merged",
                    primary_research_object="评测 / Benchmark / 数据集",
                    preference_labels=["模型压缩"],
                    negative_tier="positive",
                    evidence_spans={"general": ["final evidence"]},
                    notes="final notes",
                    review_status="final",
                )
            ],
            repository.merged_path,
        )

        app = AnnotationApplication(repository)
        response = app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": "/papers/paper-final-seed",
                "wsgi.input": BytesIO(b""),
                "CONTENT_LENGTH": "0",
            },
            lambda status, response_headers: self.assertIn("200", status),
        )

        html = b"".join(response).decode("utf-8")
        self.assertIn('<option value="评测 / Benchmark / 数据集" selected>', html)
        self.assertIn('<option value="positive" selected>', html)
        self.assertIn('<input type="radio" name="preference_labels" value="模型压缩" checked>', html)
        self.assertNotIn('name="target_preference_labels"', html)
        self.assertIn('<textarea name="evidence_1" rows="3">final evidence</textarea>', html)
        self.assertIn('<textarea name="notes" rows="3">final notes</textarea>', html)

    def test_post_annotation_requires_existing_ai_annotation(self) -> None:
        """验证只允许对已有 AI 预标的论文提交人工复标。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-post-requires-ai"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_candidates([_candidate(paper_id="paper-3", title="Needs AI First")])
        app = AnnotationApplication(repository)
        body = (
            "primary_research_object=LLM&"
            "preference_labels=%E8%A7%A3%E7%A0%81%E7%AD%96%E7%95%A5%E4%BC%98%E5%8C%96&"
            "negative_tier=positive"
        ).encode("utf-8")

        statuses: list[str] = []
        response = app(
            {
                "REQUEST_METHOD": "POST",
                "PATH_INFO": "/papers/paper-3",
                "wsgi.input": BytesIO(body),
                "CONTENT_LENGTH": str(len(body)),
            },
            lambda status, response_headers: statuses.append(status),
        )

        self.assertTrue(any(status.startswith("400") for status in statuses))
        self.assertIn("未找到 AI 预标", b"".join(response).decode("utf-8"))

    def test_post_annotation_refreshes_merged_outputs(self) -> None:
        """验证保存人工复标会刷新 merged、conflicts 和 stats。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-post"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_candidates(
            [
                _candidate(paper_id="paper-keep-pending", title="Pending Candidate"),
                _candidate(paper_id="paper-save-now", title="Save Candidate"),
            ]
        )
        repository.write_annotations(
            [
                _ai_annotation(paper_id="paper-keep-pending"),
                _ai_annotation(paper_id="paper-save-now"),
            ],
            repository.annotations_ai_path,
        )

        app = AnnotationApplication(repository)
        statuses: list[str] = []
        headers: list[tuple[str, str]] = []
        body = (
            "primary_research_object=LLM&"
            "preference_labels=%E8%A7%A3%E7%A0%81%E7%AD%96%E7%95%A5%E4%BC%98%E5%8C%96&"
            "negative_tier=positive&"
            "evidence_1=speculative+decoding&"
            "notes=saved"
        ).encode("utf-8")

        response = app(
            {
                "REQUEST_METHOD": "POST",
                "PATH_INFO": "/papers/paper-save-now",
                "wsgi.input": BytesIO(body),
                "CONTENT_LENGTH": str(len(body)),
            },
            lambda status, response_headers: (statuses.append(status), headers.extend(response_headers)),
        )

        self.assertEqual([b""], response)
        self.assertTrue(any(status.startswith("302") for status in statuses))
        self.assertIn(("Location", "/papers/paper-keep-pending"), headers)
        self.assertEqual([], repository.load_conflicts(repository.conflicts_path))
        self.assertEqual(["paper-save-now"], [item.paper_id for item in repository.load_annotations(repository.merged_path)])
        self.assertTrue(repository.stats_path.exists())

    def test_post_negative_annotation_clears_preference_labels(self) -> None:
        """验证 negative 样本保存时会自动清空偏好标签。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-post-negative"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_candidates([_candidate(paper_id="paper-negative", title="Negative Candidate")])
        repository.write_annotations([_ai_annotation(paper_id="paper-negative")], repository.annotations_ai_path)

        app = AnnotationApplication(repository)
        body = (
            "primary_research_object=LLM&"
            "preference_labels=%E8%A7%A3%E7%A0%81%E7%AD%96%E7%95%A5%E4%BC%98%E5%8C%96&"
            "negative_tier=negative"
        ).encode("utf-8")

        app(
            {
                "REQUEST_METHOD": "POST",
                "PATH_INFO": "/papers/paper-negative",
                "wsgi.input": BytesIO(body),
                "CONTENT_LENGTH": str(len(body)),
            },
            lambda status, response_headers: None,
        )

        human = repository.load_annotations(repository.annotations_human_path)
        self.assertEqual(1, len(human))
        self.assertEqual("negative", human[0].negative_tier)
        self.assertEqual([], human[0].preference_labels)

    def test_post_positive_annotation_requires_exactly_one_preference_label(self) -> None:
        """验证 positive 样本必须且只能提交一个子偏好标签。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-post-positive-single-select"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_candidates([_candidate(paper_id="paper-positive-single", title="Positive Single Select")])
        repository.write_annotations([_ai_annotation(paper_id="paper-positive-single")], repository.annotations_ai_path)

        app = AnnotationApplication(repository)
        body = (
            "primary_research_object=LLM&"
            "preference_labels=%E8%A7%A3%E7%A0%81%E7%AD%96%E7%95%A5%E4%BC%98%E5%8C%96&"
            "preference_labels=%E6%A8%A1%E5%9E%8B%E5%8E%8B%E7%BC%A9&"
            "negative_tier=positive"
        ).encode("utf-8")

        statuses: list[str] = []
        response = app(
            {
                "REQUEST_METHOD": "POST",
                "PATH_INFO": "/papers/paper-positive-single",
                "wsgi.input": BytesIO(body),
                "CONTENT_LENGTH": str(len(body)),
            },
            lambda status, response_headers: statuses.append(status),
        )

        self.assertTrue(any(status.startswith("400") for status in statuses))
        self.assertIn("子偏好标签必须单选", b"".join(response).decode("utf-8"))

    def test_conflict_resolution_marks_conflict_resolved_and_emits_merged(self) -> None:
        """验证冲突页可以在线仲裁，并在仲裁后产出 merged 结果。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-conflict"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_candidates([_candidate(paper_id="paper-conflict", title="Conflict Candidate")])
        repository.write_annotations([_ai_annotation(paper_id="paper-conflict")], repository.annotations_ai_path)
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="paper-conflict",
                    labeler_id="human_reviewer",
                    primary_research_object="AI 系统 / 基础设施",
                    preference_labels=["解码策略优化"],
                    negative_tier="positive",
                    evidence_spans={"general": ["human evidence"]},
                    review_status="pending",
                )
            ],
            repository.annotations_human_path,
        )

        app = AnnotationApplication(repository)
        app._refresh_merge_outputs()
        body = "winner=human".encode("utf-8")
        statuses: list[str] = []
        headers: list[tuple[str, str]] = []

        response = app(
            {
                "REQUEST_METHOD": "POST",
                "PATH_INFO": "/conflicts/paper-conflict/resolve",
                "wsgi.input": BytesIO(body),
                "CONTENT_LENGTH": str(len(body)),
            },
            lambda status, response_headers: (statuses.append(status), headers.extend(response_headers)),
        )

        self.assertEqual([b""], response)
        self.assertTrue(any(status.startswith("302") for status in statuses))
        self.assertIn(("Location", "/conflicts"), headers)
        conflicts = repository.load_conflicts(repository.conflicts_path)
        self.assertEqual(1, len(conflicts))
        self.assertTrue(conflicts[0].is_resolved)
        self.assertEqual(["paper-conflict"], [item.paper_id for item in repository.load_annotations(repository.merged_path)])


if __name__ == "__main__":
    unittest.main()
