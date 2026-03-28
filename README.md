# Paper Analysis Dataset

这是一个可单独 clone、单独安装、单独测试、单独运行的数据集与标注工具仓库。

包含能力：

- benchmark 数据集文件与重建工具
- annotation 仓储、合并逻辑与网页标注应用
- Codex / Doubao 预标工具
- 中文摘要回填工具
- 跨仓评测 CLI 与脱敏报告输出
- 子仓自己的单元测试与质量入口

## 文档

- benchmark 规范文档位于 `docs/benchmarks/`
- 总入口位于 `docs/README.md`

## 安装

```powershell
py -m pip install -e .
```

要求：

- Python `>=3.11`

## 常用入口

```powershell
paper-analysis-dataset-rebuild --paperlists-root D:\path\to\paperlists
paper-analysis-dataset-annotate
paper-analysis-dataset-backfill --limit 20
paper-analysis-dataset-annotation-app
paper-analysis-dataset-evaluate --base-url http://127.0.0.1:8765 --limit 20
paper-analysis-dataset-local-ci
```

也可以直接使用模块入口：

```powershell
py -m paper_analysis_dataset.tools.rebuild_paper_filter_benchmark --paperlists-root D:\path\to\paperlists
py -m paper_analysis_dataset.tools.evaluate_paper_filter_benchmark --base-url http://127.0.0.1:8765 --limit 20
py -m paper_analysis_dataset.tools.local_ci
```

评测约束：

- 主仓看不到子仓 benchmark 文件内容
- 子仓只通过 `POST /v1/evaluation/annotate` 与主仓交互
- 正式评测报告只输出聚合指标，不输出 `paper_id`、标题、摘要、作者、`source_path`

如需在重建时顺便生成中文摘要，显式开启：

```powershell
paper-analysis-dataset-rebuild --paperlists-root D:\path\to\paperlists --with-doubao-abstract-translation
```

## 测试

```powershell
paper-analysis-dataset-local-ci
```

或：

```powershell
py -m unittest discover -s tests/unit -t .
```

## 会议数据输入契约

- 本仓不会自动借用外部 superproject 的 `third_party/paperlists`
- 重建 benchmark 时必须显式提供 `--paperlists-root`
- 若目录不存在、不是目录，或缺少目标会议 JSON，工具会直接报错
- 仓内 `tests/fixtures/paperlists_repo` 仅用于单测，不是完整会议数据快照

## 配置

Doubao 默认优先读取环境变量 `ARK_API_KEY`。

如需本地私有配置文件，可放在：

```text
%USERPROFILE%\.paper-analysis-dataset\doubao.yaml
```

或通过环境变量 `PAPER_ANALYSIS_DATASET_HOME` 指向自定义私有目录。

模板文件位于：

```text
config/doubao.template.yaml
```

Codex 预标依赖本地可用的 `codex` CLI。
