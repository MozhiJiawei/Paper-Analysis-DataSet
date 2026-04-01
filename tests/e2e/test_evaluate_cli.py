from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import unittest
from pathlib import Path
from urllib import request


ROOT_DIR = Path(__file__).resolve().parents[2]
MAIN_ROOT = Path(os.environ.get("PAPER_ANALYSIS_MAIN_ROOT", "")).resolve() if os.environ.get("PAPER_ANALYSIS_MAIN_ROOT") else None


def _find_free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


@unittest.skipUnless(MAIN_ROOT and MAIN_ROOT.exists(), "未提供 PAPER_ANALYSIS_MAIN_ROOT，跳过跨仓 e2e。")
class EvaluateCliE2ETests(unittest.TestCase):
    def test_evaluate_cli_runs_against_real_main_repo_service(self) -> None:
        port = _find_free_port()
        process = self._start_server(port)
        output_dir = ROOT_DIR / "artifacts" / "test-output" / "dataset-evaluate-e2e"
        if output_dir.exists():
            shutil.rmtree(output_dir)
        env = os.environ.copy()
        env["PYTHONUTF8"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "paper_analysis_dataset.tools.evaluate_paper_filter_benchmark",
                    "--base-url",
                    f"http://127.0.0.1:{port}",
                    "--limit",
                    "55",
                    "--output-dir",
                    str(output_dir),
                ],
                cwd=ROOT_DIR,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=env,
                check=False,
            )
            self.assertEqual(0, result.returncode, result.stdout + result.stderr)
            payload = json.loads((output_dir / "report.json").read_text(encoding="utf-8"))
            summary = (output_dir / "summary.md").read_text(encoding="utf-8")
            serialized = json.dumps(payload, ensure_ascii=False) + "\n" + summary
            self.assertEqual(55, payload["counts"]["evaluated_count"])
            self.assertNotIn("paper_id", serialized)
            self.assertNotIn("source_path", serialized)
        finally:
            self._stop_server(process)

    def _start_server(self, port: int) -> subprocess.Popen[str]:
        env = os.environ.copy()
        env["PYTHONUTF8"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "paper_analysis.api.evaluation_server",
                "--port",
                str(port),
                "--algorithm-version",
                "dataset-e2e-v1",
            ],
            cwd=MAIN_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                with request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=1) as response:
                    if response.status == 200:
                        return process
            except Exception:
                time.sleep(0.1)
        self._stop_server(process)
        self.fail("主仓评测服务未能在预期时间内启动。")

    def _stop_server(self, process: subprocess.Popen[str]) -> None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        if process.stdout is not None:
            process.stdout.close()
        if process.stderr is not None:
            process.stderr.close()


if __name__ == "__main__":
    unittest.main()
