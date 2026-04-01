from __future__ import annotations

from argparse import ArgumentParser
import json
import random
import sys
from pathlib import Path

from paper_analysis_dataset.domain.benchmark import AnnotationRecord
from paper_analysis_dataset.services.annotation_repository import AnnotationRepository
from paper_analysis_dataset.services.evaluation_client import (
    EvaluationApiClient,
    EvaluationApiError,
    EvaluationProtocolError,
)
from paper_analysis_dataset.services.evaluation_reporter import (
    build_evaluation_report,
    write_evaluation_artifacts,
)
from paper_analysis_dataset.shared.paths import ARTIFACTS_DIR


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        prog="paper-analysis-dataset-evaluate",
        description="调用主仓评测 API，对 benchmark 执行脱敏评测并输出聚合报告。",
    )
    parser.add_argument("--base-url", required=True, help="主仓评测服务根地址，例如 http://127.0.0.1:8765")
    parser.add_argument("--limit", type=int, default=0, help="限制评测样本数，0 表示全部")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ARTIFACTS_DIR / "evaluation" / "latest",
        help="报告输出目录，默认 artifacts/evaluation/latest",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=10.0,
        help="单请求超时时间，默认 10 秒",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="遇到请求失败或协议错误时立即退出",
    )
    parser.add_argument(
        "--sample-seed",
        type=int,
        default=42,
        help="limit 生效时用于抽样的随机种子",
    )
    parser.add_argument(
        "--benchmark-root",
        type=Path,
        default=None,
        help="可选 benchmark 根目录，默认使用仓内 data/benchmarks/paper-filter",
    )
    return parser


def evaluate_benchmark(
    *,
    base_url: str,
    limit: int = 0,
    output_dir: Path,
    timeout_seconds: float = 10.0,
    fail_fast: bool = False,
    sample_seed: int = 42,
    benchmark_root: Path | None = None,
) -> dict[str, object]:
    repository = AnnotationRepository(benchmark_root)
    record_map = repository.load_record_map()
    truths_all = repository.load_annotations(repository.merged_path)
    selected_truths = _sample_truths(truths_all, limit=limit, sample_seed=sample_seed)
    client = EvaluationApiClient(base_url=base_url, timeout_seconds=timeout_seconds)
    print(f"[evaluate] start total={len(selected_truths)} base_url={base_url}")

    truths: list[AnnotationRecord] = []
    predictions: list[AnnotationRecord] = []
    request_error_count = 0
    protocol_error_count = 0

    for index, truth in enumerate(selected_truths, start=1):
        record = record_map.get(truth.paper_id)
        if record is None:
            raise ValueError(f"records.jsonl 中缺少 benchmark 样本：{truth.paper_id}")
        candidate = record.to_candidate_paper()
        request_id = f"benchmark:{candidate.paper_id}:{index}"
        try:
            prediction = client.annotate(candidate, request_id=request_id)
        except EvaluationApiError:
            request_error_count += 1
            print(
                f"[evaluate] {index}/{len(selected_truths)} errors={request_error_count} "
                f"protocol_errors={protocol_error_count} paper_id={truth.paper_id}"
            )
            if fail_fast:
                print(f"[evaluate] fail_fast paper_id={truth.paper_id}")
                raise
            continue
        except EvaluationProtocolError:
            protocol_error_count += 1
            print(
                f"[evaluate] {index}/{len(selected_truths)} errors={request_error_count} "
                f"protocol_errors={protocol_error_count} paper_id={truth.paper_id}"
            )
            if fail_fast:
                print(f"[evaluate] fail_fast paper_id={truth.paper_id}")
                raise
            continue
        prediction.paper_id = truth.paper_id
        truths.append(truth)
        predictions.append(prediction)
        print(
            f"[evaluate] {index}/{len(selected_truths)} errors={request_error_count} "
            f"protocol_errors={protocol_error_count} paper_id={truth.paper_id}"
        )

    report = build_evaluation_report(
        truths=truths,
        predictions=predictions,
        request_error_count=request_error_count,
        protocol_error_count=protocol_error_count,
    )
    artifacts = write_evaluation_artifacts(output_dir, report)
    summary = {
        "ok": request_error_count == 0 and protocol_error_count == 0,
        "artifacts": {key: str(path) for key, path in artifacts.items()},
        "report": report,
    }
    print(f"[evaluate] done output_dir={output_dir}")
    return summary


def _sample_truths(
    truths: list[AnnotationRecord],
    *,
    limit: int,
    sample_seed: int,
) -> list[AnnotationRecord]:
    if limit <= 0 or limit >= len(truths):
        return list(truths)
    randomizer = random.Random(sample_seed)
    sampled = randomizer.sample(truths, limit)
    sampled.sort(key=lambda item: item.paper_id)
    return sampled


def main() -> None:
    args = build_parser().parse_args()
    summary = evaluate_benchmark(
        base_url=args.base_url,
        limit=args.limit,
        output_dir=args.output_dir,
        timeout_seconds=args.timeout_seconds,
        fail_fast=args.fail_fast,
        sample_seed=args.sample_seed,
        benchmark_root=args.benchmark_root,
    )
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if not summary["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
