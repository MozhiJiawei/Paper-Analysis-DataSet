from __future__ import annotations

from http import HTTPStatus
from pathlib import Path
from urllib.parse import parse_qs
from wsgiref.simple_server import make_server

from jinja2 import Environment, FileSystemLoader

from paper_analysis_dataset.domain.benchmark import AnnotationRecord
from paper_analysis_dataset.services.annotation_merge import merge_annotations
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.benchmark_reporter import build_distribution_report
from paper_analysis_dataset.web.view_models import AnnotationAppState


TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"
STATIC_DIR = Path(__file__).resolve().parent / "static"


class AnnotationApplication:
    def __init__(self, repository: AnnotationRepository | None = None) -> None:
        self.repository = repository or AnnotationRepository()
        self.state = AnnotationAppState(self.repository)
        self.env = Environment(
            loader=FileSystemLoader(TEMPLATE_DIR),
            autoescape=True,
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def __call__(self, environ: dict[str, object], start_response: callable) -> list[bytes]:
        method = str(environ.get("REQUEST_METHOD", "GET"))
        path = str(environ.get("PATH_INFO", "/"))

        try:
            if path == "/":
                return self._redirect(start_response, "/papers")
            if path == "/static/annotation.css":
                return self._serve_css(start_response)
            if path == "/papers" and method == "GET":
                query = parse_qs(str(environ.get("QUERY_STRING", "")))
                status_filter = query.get("status", ["all"])[0]
                if status_filter not in {"all", "pending", "completed", "conflict"}:
                    status_filter = "all"
                return self._html_response(
                    start_response,
                    "annotation_list.html.j2",
                    {
                        "title": "候选池列表",
                        "rows": self.state.list_papers(status_filter=status_filter),
                        "status_filter": status_filter,
                        "counts": self.state.list_paper_counts(),
                    },
                )
            if path.startswith("/papers/") and method == "GET":
                paper_id = path.split("/", 2)[2]
                return self._html_response(
                    start_response,
                    "annotation_detail.html.j2",
                    {"title": "单论文标注", **self.state.paper_detail(paper_id)},
                )
            if path.startswith("/papers/") and method == "POST":
                paper_id = path.split("/", 2)[2]
                self._save_human_annotation(environ, paper_id)
                self._refresh_merge_outputs()
                next_paper_id = self.state.next_pending_paper_id(paper_id)
                if next_paper_id is not None:
                    return self._redirect(start_response, f"/papers/{next_paper_id}")
                return self._redirect(start_response, f"/papers/{paper_id}")
            if path == "/conflicts" and method == "GET":
                return self._html_response(
                    start_response,
                    "annotation_conflicts.html.j2",
                    {"title": "冲突审阅", "rows": self.state.conflicts()},
                )
            if path.startswith("/conflicts/") and path.endswith("/resolve") and method == "POST":
                paper_id = path.split("/", 3)[2]
                self._resolve_conflict(environ, paper_id)
                self._refresh_merge_outputs()
                next_conflict = self._next_unresolved_conflict_id(current_paper_id=paper_id)
                if next_conflict is not None:
                    return self._redirect(start_response, f"/conflicts#{next_conflict}")
                return self._redirect(start_response, "/conflicts")
            if path == "/stats" and method == "GET":
                return self._html_response(
                    start_response,
                    "annotation_dashboard.html.j2",
                    {"title": "数据概览", **self.state.dashboard()},
                )
        except KeyError:
            return self._text_response(start_response, HTTPStatus.NOT_FOUND, "未找到论文")
        except ValueError as exc:
            return self._text_response(start_response, HTTPStatus.BAD_REQUEST, str(exc))

        return self._text_response(start_response, HTTPStatus.NOT_FOUND, "未找到页面")

    def _save_human_annotation(self, environ: dict[str, object], paper_id: str) -> None:
        body = self._read_form(environ)
        evidence_text = body.get("evidence_1", [""])[0].strip()
        negative_tier = body.get("negative_tier", ["negative"])[0]
        preference_labels = body.get("preference_labels", [])
        if negative_tier == "negative":
            preference_labels = []
        elif len(preference_labels) != 1:
            raise ValueError("positive 样本的子偏好标签必须单选。")
        annotation = AnnotationRecord(
            paper_id=paper_id,
            labeler_id="human_reviewer",
            primary_research_object=body.get("primary_research_object", [""])[0],
            preference_labels=preference_labels,
            negative_tier=negative_tier,
            evidence_spans=({"general": [evidence_text]} if evidence_text else {}),
            notes=body.get("notes", [""])[0],
            review_status="pending",
        )
        if not any(item.paper_id == paper_id for item in self.repository.load_annotations(self.repository.annotations_ai_path)):
            raise ValueError(f"未找到 AI 预标，禁止直接人工补标：{paper_id}")
        self.repository.upsert_annotation(annotation, self.repository.annotations_human_path)

    def _refresh_merge_outputs(self) -> None:
        records = self.repository.load_records()
        ai = self.repository.load_annotations(self.repository.annotations_ai_path)
        human = self.repository.load_annotations(self.repository.annotations_human_path)
        existing_conflicts = self.repository.load_conflicts(self.repository.conflicts_path)
        paired_ids = {
            item.paper_id for item in ai
        } & {
            item.paper_id for item in human
        }
        arbitrations = [
            item.resolved_annotation
            for item in existing_conflicts
            if item.resolved_annotation is not None and item.paper_id in paired_ids
        ]
        result = merge_annotations(
            [item for item in records if item.paper_id in paired_ids],
            [item for item in ai if item.paper_id in paired_ids],
            [item for item in human if item.paper_id in paired_ids],
            arbitrations,
        )
        merged_by_id = {item.paper_id: item for item in result.records}
        next_records = [merged_by_id.get(item.paper_id, item) for item in records]
        self.repository.write_records(next_records)
        self.repository.write_conflicts(result.conflicts, self.repository.conflicts_path)
        self.repository.write_annotations(result.merged_annotations, self.repository.merged_path)
        self.repository.write_json(build_distribution_report(next_records), self.repository.stats_path)

    def _resolve_conflict(self, environ: dict[str, object], paper_id: str) -> None:
        body = self._read_form(environ)
        winner = body.get("winner", [""])[0]
        if winner not in {"codex", "human"}:
            raise ValueError("仲裁结果必须选择 codex 或 human")

        conflicts = self.repository.load_conflicts(self.repository.conflicts_path)
        next_conflicts = []
        found = False
        for item in conflicts:
            if item.paper_id != paper_id:
                next_conflicts.append(item)
                continue
            found = True
            source_annotation = item.codex_annotation if winner == "codex" else item.human_annotation
            resolved_annotation = AnnotationRecord(
                paper_id=source_annotation.paper_id,
                labeler_id="arbiter",
                primary_research_object=source_annotation.primary_research_object,
                preference_labels=source_annotation.preference_labels,
                negative_tier=source_annotation.negative_tier,
                evidence_spans=source_annotation.evidence_spans,
                notes=f"仲裁采纳：{winner}",
                review_status="final",
            )
            item.resolved_annotation = resolved_annotation
            next_conflicts.append(item)
        if not found:
            raise ValueError(f"未找到冲突样本：{paper_id}")
        self.repository.write_conflicts(next_conflicts, self.repository.conflicts_path)

    def _next_unresolved_conflict_id(self, current_paper_id: str | None = None) -> str | None:
        pending_conflicts = [
            item["paper_id"]
            for item in self.state.conflicts()
            if not item["resolved"]
        ]
        if not pending_conflicts:
            return None
        if current_paper_id not in pending_conflicts:
            return pending_conflicts[0]
        current_index = pending_conflicts.index(current_paper_id)
        if current_index + 1 < len(pending_conflicts):
            return pending_conflicts[current_index + 1]
        if current_index > 0:
            return pending_conflicts[0]
        return None

    def _serve_css(self, start_response: callable) -> list[bytes]:
        data = (STATIC_DIR / "annotation.css").read_bytes()
        start_response(
            f"{HTTPStatus.OK.value} {HTTPStatus.OK.phrase}",
            [("Content-Type", "text/css; charset=utf-8")],
        )
        return [data]

    def _html_response(
        self,
        start_response: callable,
        template_name: str,
        context: dict[str, object],
    ) -> list[bytes]:
        template = self.env.get_template(template_name)
        data = template.render(**context).encode("utf-8")
        start_response(
            f"{HTTPStatus.OK.value} {HTTPStatus.OK.phrase}",
            [("Content-Type", "text/html; charset=utf-8")],
        )
        return [data]

    def _text_response(
        self,
        start_response: callable,
        status: HTTPStatus,
        text: str,
    ) -> list[bytes]:
        start_response(
            f"{status.value} {status.phrase}",
            [("Content-Type", "text/plain; charset=utf-8")],
        )
        return [text.encode("utf-8")]

    def _redirect(self, start_response: callable, location: str) -> list[bytes]:
        start_response(
            f"{HTTPStatus.FOUND.value} {HTTPStatus.FOUND.phrase}",
            [("Location", location)],
        )
        return [b""]

    def _read_form(self, environ: dict[str, object]) -> dict[str, list[str]]:
        content_length = int(str(environ.get("CONTENT_LENGTH", "0") or "0"))
        raw_body = environ["wsgi.input"].read(content_length).decode("utf-8")
        return parse_qs(raw_body)


def main() -> None:
    app = AnnotationApplication()
    with make_server("127.0.0.1", 8042, app) as httpd:
        print("标注工具已启动：http://127.0.0.1:8042")
        httpd.serve_forever()

if __name__ == "__main__":
    main()
