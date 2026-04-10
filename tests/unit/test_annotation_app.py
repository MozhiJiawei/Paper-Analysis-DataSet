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
        title_zh=f"{title} 中文标题",
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
        self.assertIn("Annotation App Test 中文标题", html)
        self.assertIn("Annotation App Test", html)
        self.assertIn("状态筛选", html)
        self.assertIn("论文筛选", html)
        self.assertIn('name="preference_label"', html)
        self.assertIn('name="negative_tier"', html)
        self.assertIn('name="research_object"', html)
        self.assertIn("待抽检", html)
        self.assertIn("已完成", html)
        self.assertIn("负标签", html)
        self.assertIn("正样本", html)
        self.assertTrue(any(header[0] == "Content-Type" for header in headers))

    def test_papers_route_supports_dropdown_filters(self) -> None:
        """验证候选池支持子标签、正负样本和研究对象下拉筛选。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-dropdown-filters"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_candidates(
            [
                _candidate(paper_id="paper-cache", title="Cache Candidate"),
                CandidatePaper(
                    paper_id="paper-vision-negative",
                    title="Vision Negative Candidate",
                    title_zh="视觉负样本",
                    abstract="Vision negative abstract.",
                    abstract_zh="视觉负样本摘要。",
                    authors=["Bob"],
                    venue="ICLR 2025",
                    year=2025,
                    source="conference",
                    source_path="tests.json",
                    primary_research_object="计算机视觉",
                    candidate_preference_labels=[],
                    candidate_negative_tier="negative",
                ),
                CandidatePaper(
                    paper_id="paper-systems",
                    title="Systems Candidate",
                    title_zh="系统样本",
                    abstract="Systems abstract.",
                    abstract_zh="系统样本摘要。",
                    authors=["Carol"],
                    venue="ICLR 2025",
                    year=2025,
                    source="conference",
                    source_path="tests.json",
                    primary_research_object="AI 系统 / 基础设施",
                    candidate_preference_labels=["系统与调度优化"],
                    candidate_negative_tier="positive",
                ),
            ]
        )
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="paper-cache",
                    labeler_id="codex_cli",
                    primary_research_object="LLM",
                    preference_labels=["上下文与缓存优化"],
                    negative_tier="positive",
                    evidence_spans={"general": ["cache"]},
                    review_status="pending",
                ),
                AnnotationRecord(
                    paper_id="paper-vision-negative",
                    labeler_id="codex_cli",
                    primary_research_object="计算机视觉",
                    preference_labels=[],
                    negative_tier="negative",
                    evidence_spans={"general": ["vision"]},
                    review_status="pending",
                ),
                AnnotationRecord(
                    paper_id="paper-systems",
                    labeler_id="codex_cli",
                    primary_research_object="AI 系统 / 基础设施",
                    preference_labels=["系统与调度优化"],
                    negative_tier="positive",
                    evidence_spans={"general": ["systems"]},
                    review_status="pending",
                ),
            ],
            repository.annotations_ai_path,
        )

        app = AnnotationApplication(repository)
        response = app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": "/papers",
                "QUERY_STRING": (
                    "status=all&preference_label=%E4%B8%8A%E4%B8%8B%E6%96%87%E4%B8%8E%E7%BC%93%E5%AD%98%E4%BC%98%E5%8C%96"
                    "&negative_tier=positive&research_object=LLM"
                ),
                "wsgi.input": BytesIO(b""),
                "CONTENT_LENGTH": "0",
            },
            lambda status, response_headers: self.assertIn("200", status),
        )

        html = b"".join(response).decode("utf-8")
        self.assertIn("Cache Candidate 中文标题", html)
        self.assertIn("Cache Candidate", html)
        self.assertNotIn("Vision Negative Candidate", html)
        self.assertNotIn("Systems Candidate", html)

    def test_papers_route_supports_negative_status_filter(self) -> None:
        """验证候选池一级分类支持负样本待抽检筛选。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-negative-filter"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_candidates(
            [
                _candidate(paper_id="paper-positive", title="Positive Candidate"),
                CandidatePaper(
                    paper_id="paper-negative",
                    title="Negative Candidate",
                    title_zh="负样本候选论文",
                    abstract="Negative abstract.",
                    abstract_zh="Negative 中文摘要。",
                    authors=["Alice"],
                    venue="ICLR 2025",
                    year=2025,
                    source="conference",
                    source_path="tests.json",
                    primary_research_object="LLM",
                    candidate_preference_labels=[],
                    candidate_negative_tier="negative",
                ),
            ]
        )
        repository.write_annotations(
            [
                _ai_annotation(paper_id="paper-positive"),
                AnnotationRecord(
                    paper_id="paper-negative",
                    labeler_id="codex_cli",
                    primary_research_object="LLM",
                    preference_labels=[],
                    negative_tier="negative",
                    evidence_spans={"negative": ["not relevant"]},
                    review_status="pending",
                ),
            ],
            repository.annotations_ai_path,
        )

        app = AnnotationApplication(repository)
        response = app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": "/papers",
                "QUERY_STRING": "status=negative",
                "wsgi.input": BytesIO(b""),
                "CONTENT_LENGTH": "0",
            },
            lambda status, response_headers: self.assertIn("200", status),
        )

        html = b"".join(response).decode("utf-8")
        self.assertIn("负样本候选论文", html)
        self.assertIn("Negative Candidate", html)
        self.assertNotIn("Positive Candidate", html)
        self.assertIn("负样本", html)
        self.assertIn("一键完成全部待抽检", html)
        self.assertNotIn(">一键完成<", html)

    def test_papers_route_prefers_ai_research_object_over_candidate_seed(self) -> None:
        """验证列表页主研究对象优先显示 AI 预标，而不是候选种子。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-ai-object-priority"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_candidates(
            [
                CandidatePaper(
                    paper_id="paper-vision",
                    title="E-MoFlow",
                    title_zh="E-MoFlow 中文标题",
                    abstract="Optical flow and ego-motion from event data.",
                    abstract_zh="中文摘要。",
                    authors=["Alice"],
                    venue="NIPS 2025",
                    year=2025,
                    source="conference",
                    source_path="tests.json",
                    primary_research_object="LLM",
                    candidate_preference_labels=[],
                    candidate_negative_tier="negative",
                )
            ]
        )
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="paper-vision",
                    labeler_id="codex_cli",
                    primary_research_object="计算机视觉",
                    preference_labels=[],
                    negative_tier="negative",
                    evidence_spans={"general": ["optical flow"]},
                    review_status="pending",
                )
            ],
            repository.annotations_ai_path,
        )

        app = AnnotationApplication(repository)
        response = app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": "/papers",
                "QUERY_STRING": "status=negative",
                "wsgi.input": BytesIO(b""),
                "CONTENT_LENGTH": "0",
            },
            lambda status, response_headers: self.assertIn("200", status),
        )

        html = b"".join(response).decode("utf-8")
        self.assertIn("计算机视觉", html)
        self.assertNotIn(">LLM<", html)

    def test_negative_sample_after_human_spot_check_moves_to_completed(self) -> None:
        """验证负样本抽检后离开待抽检列表并进入已完成。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-negative-completed"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_candidates(
            [
                CandidatePaper(
                    paper_id="paper-negative-done",
                    title="Negative Done Candidate",
                    title_zh="负样本已复标论文",
                    abstract="Negative done abstract.",
                    abstract_zh="Negative done 中文摘要。",
                    authors=["Alice"],
                    venue="ICLR 2025",
                    year=2025,
                    source="conference",
                    source_path="tests.json",
                    primary_research_object="LLM",
                    candidate_preference_labels=[],
                    candidate_negative_tier="negative",
                )
            ]
        )
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="paper-negative-done",
                    labeler_id="codex_cli",
                    primary_research_object="LLM",
                    preference_labels=[],
                    negative_tier="negative",
                    evidence_spans={"negative": ["ai spot check"]},
                    review_status="pending",
                )
            ],
            repository.annotations_ai_path,
        )
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="paper-negative-done",
                    labeler_id="human_reviewer",
                    primary_research_object="LLM",
                    preference_labels=[],
                    negative_tier="negative",
                    evidence_spans={"negative": ["human checked"]},
                    review_status="pending",
                )
            ],
            repository.annotations_human_path,
        )

        app = AnnotationApplication(repository)
        negative_response = app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": "/papers",
                "QUERY_STRING": "status=negative",
                "wsgi.input": BytesIO(b""),
                "CONTENT_LENGTH": "0",
            },
            lambda status, response_headers: self.assertIn("200", status),
        )
        negative_html = b"".join(negative_response).decode("utf-8")
        self.assertNotIn("Negative Done Candidate", negative_html)

        completed_response = app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": "/papers",
                "QUERY_STRING": "status=completed",
                "wsgi.input": BytesIO(b""),
                "CONTENT_LENGTH": "0",
            },
            lambda status, response_headers: self.assertIn("200", status),
        )
        completed_html = b"".join(completed_response).decode("utf-8")
        self.assertIn("Negative Done Candidate", completed_html)

    def test_status_counts_use_candidate_negative_tier_and_include_conflict_in_all(self) -> None:
        """验证顶部计数使用候选负样本层，且全部包含冲突。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-status-counts"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_candidates(
            [
                CandidatePaper(
                    paper_id="paper-negative-pending",
                    title="Negative Pending",
                    title_zh="负样本待抽检论文",
                    abstract="Negative pending abstract.",
                    abstract_zh="Negative pending 中文摘要。",
                    authors=["Alice"],
                    venue="ICLR 2025",
                    year=2025,
                    source="conference",
                    source_path="tests.json",
                    primary_research_object="LLM",
                    candidate_preference_labels=[],
                    candidate_negative_tier="negative",
                ),
                _candidate(paper_id="paper-positive-pending", title="Positive Pending"),
                _candidate(paper_id="paper-positive-completed", title="Positive Completed"),
                _candidate(paper_id="paper-positive-conflict", title="Positive Conflict"),
            ]
        )
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="paper-negative-pending",
                    labeler_id="codex_cli",
                    primary_research_object="LLM",
                    preference_labels=["解码策略优化"],
                    negative_tier="positive",
                    evidence_spans={"general": ["ai mismatch but should not affect queue count"]},
                    review_status="pending",
                ),
                _ai_annotation(paper_id="paper-positive-pending"),
                _ai_annotation(paper_id="paper-positive-completed"),
                _ai_annotation(paper_id="paper-positive-conflict"),
            ],
            repository.annotations_ai_path,
        )
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="paper-positive-completed",
                    labeler_id="human_reviewer",
                    primary_research_object="LLM",
                    preference_labels=["解码策略优化"],
                    negative_tier="positive",
                    evidence_spans={"general": ["human done"]},
                    review_status="pending",
                ),
                AnnotationRecord(
                    paper_id="paper-positive-conflict",
                    labeler_id="human_reviewer",
                    primary_research_object="AI 系统 / 基础设施",
                    preference_labels=["模型压缩"],
                    negative_tier="positive",
                    evidence_spans={"general": ["human conflict"]},
                    review_status="pending",
                ),
            ],
            repository.annotations_human_path,
        )

        app = AnnotationApplication(repository)
        app._refresh_merge_outputs()

        counts = app.state.list_status_counts()
        self.assertEqual(1, counts["negative"])
        self.assertEqual(1, counts["pending"])
        self.assertEqual(1, counts["completed"])
        self.assertEqual(1, counts["conflict"])
        self.assertEqual(4, counts["all"])

    def test_negative_tab_supports_bulk_complete_and_merges(self) -> None:
        """验证待抽检页支持批量一键完成，并将结果合入 merged。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-one-click-complete"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_candidates(
            [
                CandidatePaper(
                    paper_id="paper-one-click",
                    title="One Click Negative",
                    title_zh="一键完成负样本",
                    abstract="Negative abstract.",
                    abstract_zh="负样本中文摘要。",
                    authors=["Alice"],
                    venue="NIPS 2025",
                    year=2025,
                    source="conference",
                    source_path="tests.json",
                    primary_research_object="LLM",
                    candidate_preference_labels=[],
                    candidate_negative_tier="negative",
                ),
                CandidatePaper(
                    paper_id="paper-one-click-2",
                    title="Another Negative",
                    title_zh="另一个待抽检负样本",
                    abstract="Another negative abstract.",
                    abstract_zh="另一个负样本中文摘要。",
                    authors=["Bob"],
                    venue="NIPS 2025",
                    year=2025,
                    source="conference",
                    source_path="tests.json",
                    primary_research_object="LLM",
                    candidate_preference_labels=[],
                    candidate_negative_tier="negative",
                ),
            ]
        )
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="paper-one-click",
                    labeler_id="codex_cli",
                    primary_research_object="计算机视觉",
                    preference_labels=[],
                    negative_tier="negative",
                    evidence_spans={"general": ["optical flow"]},
                    notes="ai negative",
                    review_status="pending",
                ),
                AnnotationRecord(
                    paper_id="paper-one-click-2",
                    labeler_id="codex_cli",
                    primary_research_object="通用机器学习",
                    preference_labels=[],
                    negative_tier="negative",
                    evidence_spans={"general": ["distribution models"]},
                    notes="ai negative 2",
                    review_status="pending",
                ),
            ],
            repository.annotations_ai_path,
        )

        app = AnnotationApplication(repository)
        list_response = app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": "/papers",
                "QUERY_STRING": "status=negative",
                "wsgi.input": BytesIO(b""),
                "CONTENT_LENGTH": "0",
            },
            lambda status, response_headers: self.assertIn("200", status),
        )
        list_html = b"".join(list_response).decode("utf-8")
        self.assertIn("一键完成全部待抽检", list_html)
        self.assertNotIn('action="/papers/paper-one-click/complete"', list_html)

        statuses: list[str] = []
        headers: list[tuple[str, str]] = []
        response = app(
            {
                "REQUEST_METHOD": "POST",
                "PATH_INFO": "/papers/complete-negative",
                "wsgi.input": BytesIO(b""),
                "CONTENT_LENGTH": "0",
            },
            lambda status, response_headers: (statuses.append(status), headers.extend(response_headers)),
        )

        self.assertEqual([b""], response)
        self.assertTrue(any(status.startswith("302") for status in statuses))
        self.assertIn(("Location", "/papers?status=negative&completed=2"), headers)

        human = repository.load_annotations(repository.annotations_human_path)
        self.assertEqual(2, len(human))
        self.assertEqual({"paper-one-click", "paper-one-click-2"}, {item.paper_id for item in human})

        merged = repository.load_annotations(repository.merged_path)
        self.assertEqual(2, len(merged))
        self.assertEqual({"paper-one-click", "paper-one-click-2"}, {item.paper_id for item in merged})

        completed_response = app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": "/papers",
                "QUERY_STRING": "status=completed",
                "wsgi.input": BytesIO(b""),
                "CONTENT_LENGTH": "0",
            },
            lambda status, response_headers: self.assertIn("200", status),
        )
        completed_html = b"".join(completed_response).decode("utf-8")
        self.assertIn("一键完成负样本", completed_html)
        self.assertIn("另一个待抽检负样本", completed_html)

    def test_post_negative_spot_check_change_merges_without_conflict(self) -> None:
        """验证负样本抽检改判后直接写入 merged，不进入冲突。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-negative-spot-check"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_candidates(
            [
                CandidatePaper(
                    paper_id="paper-negative-review",
                    title="Negative Review Candidate",
                    title_zh="负样本抽检论文",
                    abstract="Negative review abstract.",
                    abstract_zh="Negative review 中文摘要。",
                    authors=["Alice"],
                    venue="ICLR 2025",
                    year=2025,
                    source="conference",
                    source_path="tests.json",
                    primary_research_object="LLM",
                    candidate_preference_labels=[],
                    candidate_negative_tier="negative",
                )
            ]
        )
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="paper-negative-review",
                    labeler_id="codex_cli",
                    primary_research_object="LLM",
                    preference_labels=[],
                    negative_tier="negative",
                    evidence_spans={"negative": ["ai spot check"]},
                    review_status="pending",
                )
            ],
            repository.annotations_ai_path,
        )

        app = AnnotationApplication(repository)
        body = (
            "primary_research_object=AI+%E7%B3%BB%E7%BB%9F+%2F+%E5%9F%BA%E7%A1%80%E8%AE%BE%E6%96%BD&"
            "preference_labels=%E6%A8%A1%E5%9E%8B%E5%8E%8B%E7%BC%A9&"
            "negative_tier=positive&"
            "evidence_1=human+fixed&"
            "notes=spot+check+updated"
        ).encode("utf-8")

        app(
            {
                "REQUEST_METHOD": "POST",
                "PATH_INFO": "/papers/paper-negative-review",
                "wsgi.input": BytesIO(body),
                "CONTENT_LENGTH": str(len(body)),
            },
            lambda status, response_headers: None,
        )

        self.assertEqual([], repository.load_conflicts(repository.conflicts_path))
        merged = repository.load_annotations(repository.merged_path)
        self.assertEqual(1, len(merged))
        self.assertEqual("paper-negative-review", merged[0].paper_id)
        self.assertEqual("positive", merged[0].negative_tier)
        self.assertEqual(["模型压缩"], merged[0].preference_labels)

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
        self.assertIn("Detail Test 中文标题", html)
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
                    title_zh="缺少中文摘要的详情页",
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

    def test_detail_route_uses_blank_supplement_fields_when_only_ai_exists(self) -> None:
        """验证只有 AI 预标时，补充信息字段保持为空。"""

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

    def test_detail_route_uses_human_supplement_fields_when_merged_missing(self) -> None:
        """验证没有 merged 时，补充信息字段继承人工复标。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-human-supplement-priority"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_records(
            [
                BenchmarkRecord(
                    paper_id="paper-human-seed",
                    title="Human Seed Priority",
                    title_zh="人工种子优先级",
                    abstract="Human Seed Priority abstract.",
                    abstract_zh="Human Seed Priority 中文摘要。",
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
                    paper_id="paper-human-seed",
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
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="paper-human-seed",
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

        app = AnnotationApplication(repository)
        response = app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": "/papers/paper-human-seed",
                "wsgi.input": BytesIO(b""),
                "CONTENT_LENGTH": "0",
            },
            lambda status, response_headers: self.assertIn("200", status),
        )

        html = b"".join(response).decode("utf-8")
        self.assertIn('<textarea name="evidence_1" rows="3">human evidence</textarea>', html)
        self.assertIn('<textarea name="notes" rows="3">human notes</textarea>', html)

    def test_detail_route_prefers_final_then_human_for_non_core_fields(self) -> None:
        """验证补充信息字段优先使用 merged，其次 human。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-final-priority"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_records(
            [
                BenchmarkRecord(
                    paper_id="paper-final-seed",
                    title="Final Seed Priority",
                    title_zh="最终种子优先级",
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

    def test_stats_route_renders_preference_label_distribution(self) -> None:
        """验证数据概览页展示子标签统计。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-stats-dashboard"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_records(
            [
                BenchmarkRecord(
                    paper_id="paper-stats-1",
                    title="Stats Candidate One",
                    title_zh="Stats Candidate One 中文标题",
                    abstract="Stats Candidate One abstract.",
                    abstract_zh="Stats Candidate One 中文摘要。",
                    authors=["Alice"],
                    venue="ICLR 2025",
                    year=2025,
                    source="conference",
                    source_path="tests.json",
                    primary_research_object="LLM",
                    candidate_preference_labels=["解码策略优化"],
                    candidate_negative_tier="positive",
                    final_primary_research_object="LLM",
                    final_preference_labels=["解码策略优化"],
                    final_negative_tier="positive",
                    final_labeler_ids=["merged"],
                    final_review_status="final",
                ),
                BenchmarkRecord(
                    paper_id="paper-stats-2",
                    title="Stats Candidate Two",
                    title_zh="Stats Candidate Two 中文标题",
                    abstract="Stats Candidate Two abstract.",
                    abstract_zh="Stats Candidate Two 中文摘要。",
                    authors=["Bob"],
                    venue="ICLR 2025",
                    year=2025,
                    source="conference",
                    source_path="tests.json",
                    primary_research_object="LLM",
                    candidate_preference_labels=["模型压缩"],
                    candidate_negative_tier="positive",
                    final_primary_research_object="LLM",
                    final_preference_labels=["模型压缩"],
                    final_negative_tier="positive",
                    final_labeler_ids=["merged"],
                    final_review_status="final",
                ),
            ],
            include_final_annotations=True,
        )

        app = AnnotationApplication(repository)
        response = app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": "/stats",
                "wsgi.input": BytesIO(b""),
                "CONTENT_LENGTH": "0",
            },
            lambda status, response_headers: self.assertIn("200", status),
        )

        html = b"".join(response).decode("utf-8")
        self.assertIn("数据概览", html)
        self.assertIn("子标签分布", html)
        self.assertIn("解码策略优化：1", html)
        self.assertIn("模型压缩：1", html)

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

    def test_conflict_page_defaults_to_human_choice(self) -> None:
        """验证冲突页默认选中以 Human 为准。"""

        temp_root = ROOT_DIR / "artifacts" / "test-output" / "annotation-app-conflict-default-human"
        if temp_root.exists():
            shutil.rmtree(temp_root)

        repository = AnnotationRepository(temp_root)
        repository.write_candidates([_candidate(paper_id="paper-conflict-default", title="Conflict Default")])
        repository.write_annotations([_ai_annotation(paper_id="paper-conflict-default")], repository.annotations_ai_path)
        repository.write_annotations(
            [
                AnnotationRecord(
                    paper_id="paper-conflict-default",
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
        response = app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": "/conflicts",
                "wsgi.input": BytesIO(b""),
                "CONTENT_LENGTH": "0",
            },
            lambda status, response_headers: self.assertIn("200", status),
        )

        html = b"".join(response).decode("utf-8")
        self.assertIn('name="winner" value="human" checked', html)
        self.assertIn("Conflict Default 中文摘要。", html)
        self.assertIn("Conflict Default 中文标题", html)


if __name__ == "__main__":
    unittest.main()
