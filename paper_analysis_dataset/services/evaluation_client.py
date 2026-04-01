from __future__ import annotations

from dataclasses import dataclass
import json
from urllib import error, request

from paper_analysis_dataset.domain.benchmark import AnnotationRecord, CandidatePaper


class EvaluationApiError(RuntimeError):
    """Raised when the remote evaluation API request fails."""


class EvaluationProtocolError(RuntimeError):
    """Raised when the remote evaluation API responds with an invalid payload."""


@dataclass(slots=True)
class EvaluationApiClient:
    base_url: str
    timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")

    def annotate(self, candidate: CandidatePaper, *, request_id: str) -> AnnotationRecord:
        payload = {
            "request_id": request_id,
            "paper": {
                "paper_id": candidate.paper_id,
                "title": candidate.title,
                "abstract": candidate.abstract,
                "abstract_zh": candidate.abstract_zh,
                "authors": candidate.authors,
                "venue": candidate.venue,
                "year": candidate.year,
                "source": candidate.source,
                "source_path": candidate.source_path,
                "keywords": candidate.keywords,
            },
        }
        http_request = request.Request(
            f"{self.base_url}/v1/evaluation/annotate",
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        try:
            with request.urlopen(http_request, timeout=self.timeout_seconds) as response:
                body = response.read().decode("utf-8")
        except error.HTTPError as exc:
            response_body = exc.read().decode("utf-8", errors="replace")
            raise EvaluationApiError(
                f"评测接口返回 HTTP {exc.code}：{response_body}"
            ) from exc
        except error.URLError as exc:
            raise EvaluationApiError(f"无法连接评测接口：{exc.reason}") from exc

        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            raise EvaluationProtocolError("评测接口响应不是合法 JSON") from exc
        return self._parse_annotation(payload)

    def _parse_annotation(self, payload: object) -> AnnotationRecord:
        if not isinstance(payload, dict):
            raise EvaluationProtocolError("评测接口响应必须是对象")
        request_id = str(payload.get("request_id", "")).strip()
        prediction = payload.get("prediction")
        model_info = payload.get("model_info")
        if not request_id:
            raise EvaluationProtocolError("评测接口响应缺少 request_id")
        if not isinstance(prediction, dict):
            raise EvaluationProtocolError("评测接口响应缺少 prediction 对象")
        if not isinstance(model_info, dict) or not str(
            model_info.get("algorithm_version", "")
        ).strip():
            raise EvaluationProtocolError("评测接口响应缺少 model_info.algorithm_version")

        disallowed_fields = {
            "expected_label",
            "ground_truth",
            "split",
            "target_preference_labels",
        }
        leaked = disallowed_fields & set(prediction)
        if leaked:
            raise EvaluationProtocolError(
                f"评测接口响应包含不允许的评测泄露字段：{', '.join(sorted(leaked))}"
            )
        try:
            return AnnotationRecord(
                paper_id=request_id.split(":", 1)[-1],
                labeler_id="evaluation_api",
                primary_research_object=str(prediction.get("primary_research_object", "")),
                preference_labels=[
                    str(item) for item in prediction.get("preference_labels", [])
                ],
                negative_tier=str(prediction.get("negative_tier", "")),
                evidence_spans={
                    str(key): [str(item) for item in value]
                    for key, value in dict(prediction.get("evidence_spans", {})).items()
                },
                notes=str(prediction.get("notes", "")),
                review_status="pending",
            )
        except ValueError as exc:
            raise EvaluationProtocolError(f"评测接口响应不符合标签协议：{exc}") from exc
