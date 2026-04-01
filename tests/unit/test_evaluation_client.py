from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from paper_analysis_dataset.domain.benchmark import CandidatePaper
from paper_analysis_dataset.services.evaluation_client import (
    EvaluationApiClient,
    EvaluationProtocolError,
)


class EvaluationClientTests(unittest.TestCase):
    def _build_candidate(self, paper_id: str = "paper-1") -> CandidatePaper:
        return CandidatePaper(
            paper_id=paper_id,
            title="Speculative Decoding",
            abstract="About speculative decoding.",
            authors=["Alice"],
            venue="ICLR 2025",
            year=2025,
            source="conference",
            source_path="tests.json",
            primary_research_object="LLM",
        )

    def test_client_parses_valid_prediction(self) -> None:
        payload = {
            "responses": [
                {
                    "request_id": "benchmark:paper-1:1",
                    "prediction": {
                        "primary_research_object": "LLM",
                        "preference_labels": ["解码策略优化"],
                        "negative_tier": "positive",
                        "evidence_spans": {"解码策略优化": ["speculative decoding"]},
                        "notes": "ok",
                    },
                    "model_info": {"algorithm_version": "heuristic-v1"},
                }
            ]
        }

        class _FakeResponse:
            status = 200

            def __enter__(self) -> _FakeResponse:
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps(payload, ensure_ascii=False).encode("utf-8")

        with patch(
            "paper_analysis_dataset.services.evaluation_client.request.urlopen",
            return_value=_FakeResponse(),
        ):
            annotations = EvaluationApiClient("http://127.0.0.1:8765").annotate_many(
                [(self._build_candidate(), "benchmark:paper-1:1")],
            )

        self.assertEqual(1, len(annotations))
        self.assertEqual("positive", annotations[0].negative_tier)
        self.assertEqual(["解码策略优化"], annotations[0].preference_labels)

    def test_client_rejects_leaked_ground_truth_fields(self) -> None:
        payload = {
            "responses": [
                {
                    "request_id": "benchmark:paper-1:1",
                    "prediction": {
                        "primary_research_object": "LLM",
                        "preference_labels": [],
                        "negative_tier": "negative",
                        "evidence_spans": {"negative": ["benchmark only"]},
                        "notes": "ok",
                        "split": "test",
                    },
                    "model_info": {"algorithm_version": "heuristic-v1"},
                }
            ]
        }

        class _FakeResponse:
            status = 200

            def __enter__(self) -> _FakeResponse:
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps(payload, ensure_ascii=False).encode("utf-8")

        with patch(
            "paper_analysis_dataset.services.evaluation_client.request.urlopen",
            return_value=_FakeResponse(),
        ):
            with self.assertRaises(EvaluationProtocolError):
                EvaluationApiClient("http://127.0.0.1:8765").annotate_many(
                    [(self._build_candidate(), "benchmark:paper-1:1")],
                )

    def test_client_rejects_response_count_mismatch(self) -> None:
        payload = {"responses": []}

        class _FakeResponse:
            status = 200

            def __enter__(self) -> _FakeResponse:
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps(payload, ensure_ascii=False).encode("utf-8")

        with patch(
            "paper_analysis_dataset.services.evaluation_client.request.urlopen",
            return_value=_FakeResponse(),
        ):
            with self.assertRaises(EvaluationProtocolError):
                EvaluationApiClient("http://127.0.0.1:8765").annotate_many(
                    [(self._build_candidate(), "benchmark:paper-1:1")],
                )


if __name__ == "__main__":
    unittest.main()
