# Repository Guidelines

## Project Structure & Module Organization
`paper_analysis_dataset/` contains the package code. Keep domain models in `domain/`, business logic in `services/`, shared path/client/conference helpers in `shared/`, CLI entrypoints in `tools/`, and the local annotation UI in `web/` with templates under `web/templates/` and static assets under `web/static/`.

Tests live in `tests/unit/`; reusable input data for isolated runs lives in `tests/fixtures/`. Repository-owned benchmark outputs and snapshots live under `data/benchmarks/`. Reference docs and labeling specs live in `docs/` and `docs/benchmarks/`. Config templates belong in `config/`.

## Build, Test, and Development Commands
Install in editable mode:

```powershell
py -m pip install -e .
```

Run the unit test entrypoint:

```powershell
paper-analysis-dataset-local-ci
```

Run tests directly with `unittest`:

```powershell
py -m unittest discover -s tests/unit -t .
```

Common CLI workflows:

```powershell
paper-analysis-dataset-rebuild --paperlists-root D:\path\to\paperlists
paper-analysis-dataset-annotate
paper-analysis-dataset-backfill --limit 20
paper-analysis-dataset-annotation-app
paper-analysis-dataset-evaluate --base-url http://127.0.0.1:8765 --limit 20
```

Cross-repo evaluation startup:

- Start the main-repo evaluation service first: `py -m paper_analysis.api.evaluation_server --port 8765`
- Then run dataset evaluation from this repo: `paper-analysis-dataset-evaluate --base-url http://127.0.0.1:8765 --limit 20`
- Module entry is also supported: `py -m paper_analysis_dataset.tools.evaluate_paper_filter_benchmark --base-url http://127.0.0.1:8765 --limit 20`
- The dataset repo talks to the main repo only through `POST /v1/evaluation/annotate`
- Formal evaluation reports must stay de-identified and output aggregate metrics only

## Coding Style & Naming Conventions
Target Python 3.11+ and follow existing style: 4-space indentation, explicit type hints, `from __future__ import annotations`, and small focused modules. Use `snake_case` for files, functions, and variables; `PascalCase` for classes; `UPPER_SNAKE_CASE` for module constants such as scoring rules or defaults.

Prefer standard library tools already used in the repo. Keep CLI modules thin and place reusable logic in `services/` or `shared/`.
Long-running tasks must print minimal progress logs to stdout, with at least `start`, in-progress updates, and `done`.

## Testing Guidelines
Use `unittest` with test files named `test_*.py` and test classes ending in `Tests`. Keep fixtures under `tests/fixtures/` so tests remain independently runnable without external datasets. Add or update unit tests for service logic, CLI contracts, and annotation/data merge behavior when changing those areas.

## Commit & Pull Request Guidelines
Recent history uses short, imperative commit subjects with a scope prefix, for example `docs: add benchmark spec documents`. Follow that pattern and keep each commit focused.

PRs should explain the user-visible or data-contract impact, list the commands you ran, and call out any dataset or config changes. Include screenshots only when changing the annotation web UI. Mention required inputs such as `--paperlists-root`, `ARK_API_KEY`, or local Doubao/Codex setup when relevant.

## Configuration & Data Notes
Do not assume access to an external superproject. Rebuild commands must receive an explicit `--paperlists-root`. Keep secrets out of the repo; use `ARK_API_KEY` or a private config file under `%USERPROFILE%\.paper-analysis-dataset\doubao.yaml`. `config/doubao.template.yaml` is the checked-in template only.
