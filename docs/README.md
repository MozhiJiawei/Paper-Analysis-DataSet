# 文档入口

当前子仓承载 benchmark 数据协议、双人标注流程、网页标注工作流与相关测试约束。

如需跨仓评测，可先在主仓启动：

```powershell
py -m paper_analysis.api.evaluation_server --port 8765
```

再在子仓执行：

```powershell
paper-analysis-dataset-evaluate --base-url http://127.0.0.1:8765 --limit 20
```

增量补录 accepted 论文并刷新 AI 层统计可执行：

```powershell
paper-analysis-dataset-rebalance --paperlists-root D:\path\to\paperlists --venues iclr:2026 nips:2025 --target-ai-positive-ratio 0.30 --batch-size 50 --seed 20260328
```

## benchmark 规范

- `docs/benchmarks/paper-filter-dataset-protocol.md`
- `docs/benchmarks/paper-filter-annotation-guidelines.md`
- `docs/benchmarks/paper-filter-label-spec.md`
- `docs/benchmarks/paper-filter-ui-workflow.md`
