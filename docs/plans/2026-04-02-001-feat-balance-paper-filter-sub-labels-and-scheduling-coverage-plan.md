---
title: feat: Balance paper-filter sub-labels and expand scheduling coverage
type: feat
status: completed
date: 2026-04-02
---

# feat: Balance paper-filter sub-labels and expand scheduling coverage

## Overview

当前 `paper-filter` 数据集已经具备完整的重建、增量补录、AI 预标和统计链路，但子标签分布明显失衡，尤其是 `系统与调度优化` 样本量过低，无法稳定支撑后续评测与专项分析。本计划收敛为单一目标：从 `d:\Git_Repo\Paper-Analysis-New\third_party\paperlists\` 的 2025/2026 最新 accepted 顶会论文中，定向补充 `系统与调度优化`，将该子类提升到 100 条，并确保新增文章全部进入 AI 标注与人工标注任务流。

## Problem Statement / Motivation

现有 `data/benchmarks/paper-filter/stats.json` 暴露出显著失衡：

- `系统与调度优化` 仅 19 条正样本
- 目标是把 `系统与调度优化` 补到 100 条
- 本轮不处理其他子类的平衡，只把任务范围限定在调度类增强

仓库当前已有两条数据构建路径，但都不能直接满足这次目标：

- `rebuild` 适合从零构建推理加速数据集，但它依赖统一阈值和 venue 配额，无法直接表达“只补调度类到 100 条”
- `rebalance` 当前默认只从启发式 `negative` 候选池补样，用于控制 AI 层 `positive_ratio`，也不覆盖“新增样本必须全部进入人工标注任务流”

本地可行性验证表明，外部数据源供给非常充足。扣除当前 benchmark 已有记录和指纹重复后，`paperlists` 中仍有约：

- 3,978 篇可新增的 `系统与调度优化` 正样本候选

其中 `iclr 2026`、`nips 2025`、`icml 2025`、`aaai 2025` 是调度类供给最强的来源，因此应作为专项增强主入口。

## Proposed Solution

将现有 pipeline 扩展为“调度类专项增强”流程，分三层推进：

### 1. 新增调度类目标规划阶段

新增一个分析与规划层，基于现有 `records.jsonl` / `annotations-ai.jsonl` / `merged.jsonl` 统计：

- 当前 `系统与调度优化` 的已确认正样本数
- 距离 100 条目标还差多少
- 调度类内部的 `primary_research_object` 分布

显式目标固定为：

- `系统与调度优化` 从当前 19 条补到 100 条
- 本轮不为其他子类设定补样目标
- 新增文章必须先完成 AI 标注，再进入人工标注任务流

该阶段输出应是一个结构化补样计划，例如：

```json
{
  "target_positive_counts": {
    "系统与调度优化": 100
  },
  "venue_priority": [
    "iclr:2026",
    "nips:2025",
    "icml:2025",
    "aaai:2025",
    "cvpr:2025",
    "iccv:2025"
  ]
}
```

### 2. 新增调度类候选检索与排序逻辑

在 `paper_analysis_dataset/services/benchmark_builder.py` 和新服务模块中，把现有通用启发式升级为：

- 支持只为 `系统与调度优化` 建立候选池
- 支持按 venue/year 优先级取样
- 支持按剩余缺口动态停止，达到 100 条即停止
- 支持对 `系统与调度优化` 注入更细粒度关键词和得分规则

对调度类应明确扩展命中词，不再只依赖通用 `scheduling / batching / serving / runtime`。基于相关论文标题与摘要，建议改成分组关键词：

- `LLM 服务化调度`：
  `llm serving`, `language model serving`, `inference serving`, `request scheduling`, `continuous batching`, `goodput`, `tail latency`, `SLO`, `QoS`, `admission control`, `load balancing`
- `多模型共卡 / 多租户服务`：
  `multiple llm serving`, `multiple model serving`, `multi-tenant serving`, `multi-tenant LoRA`, `spatial-temporal multiplexing`, `gpu multiplexing`, `co-location`, `shared GPU`, `spatial multiplexing`
- `PD 分离`：
  `prefill-decode disaggregation`, `prefill decoding disaggregation`, `disaggregated llm serving`, `disaggregated generative inference`, `decode rescheduling`, `KV cache transfer`, `heterogeneous GPUs`
- `EPD 分离`：
  `encode-prefill-decode`, `EPD disaggregation`, `multimodal serving`, `large multimodal models`, `stage-level disaggregation`
- `MoE 专家并行 / 调度`：
  `expert parallelism`, `expert parallel inference`, `moe serving`, `all-to-all communication`, `straggler effect`, `expert routing`, `sample placement`, `model-data collaborative scheduling`, `semantic parallelism`

同时建议加入“强相关标题短语”，提高命中精度：

- `MuxServe`
- `DistServe`
- `HexGen-2`
- `Adaptive Rescheduling in Prefill-Decode Disaggregated LLM Inference`
- `Efficiently Serving Large Multimodal Models Using EPD Disaggregation`
- `Sem-MoE`
- `Capacity-Aware Inference`
- `SpaceServe`

排除词也应同步收紧，减少把训练系统、泛化 MoE 算法论文或无关缩写误判成调度类：

- `training`, `pretraining`, `fine-tuning`, `RLHF`, `alignment`
- `mixture-of-experts` 但不含 `serving` / `inference` / `parallelism` / `routing` / `communication`
- `EPD` 但不含 `multimodal` / `encode` / `prefill` / `decode`
- `multi-tenant` 但不含 `LLM` / `LoRA` / `serving`
- `SLO` 单独出现时不计分，必须与 `serving` / `latency` / `throughput` / `goodput` 共现
- `routing` 单独出现时不计分，必须与 `expert` / `request` / `token` / `serving scheduler` 共现

建议在实现时采用“组命中 + 共现约束”而不是单关键词匹配。例如：

- `prefill` 与 `decode` 共现才可计入 PD 分离
- `expert parallel`、`all-to-all`、`straggler`、`routing` 至少二者共现才可计入 MoE 调度
- `multi-tenant` 需与 `LoRA`、`LLM`、`serving` 中至少一项共现才可计入多模型共卡

### 3. 新增增强 CLI，并串上 AI/人工标注任务流

不要直接把现有 `paper-analysis-dataset-rebalance` 改造成多目标工具。更稳妥的做法是新增独立入口，例如：

- `paper-analysis-dataset-augment --paperlists-root ... --venues ... --target-config ...`

该命令负责：

- 读取当前调度类分布
- 计算距离 100 条目标的缺口
- 从 2025/2026 accepted 论文中构建去重后的候选池
- 执行调度类专项增强
- 对新增记录执行 AI 预标
- 为新增记录生成或接入人工标注待办
- 刷新 `stats.json`，并输出增量报告

这样可以保留：

- `rebuild` 作为全量重建工具
- `rebalance` 作为正负比例控制工具
- `augment` 作为“调度类专项增强并推入 AI/人工标注流”的新工具

## Technical Considerations

- 仍需遵守现有协议：`records.jsonl` 是唯一外部元数据主表，新增论文不能直接写入 `merged.jsonl`
- 去重必须继续使用双保险：`paper_id` + 规范化 `title + abstract` 指纹
- 当前 schema 要求 `preference_labels` 单选，因此增强逻辑必须在多标签命中时做确定性排序，避免引入协议不兼容
- 调度类论文往往同时命中 `AI 系统 / 基础设施` 与 `LLM`，需要在主研究对象推断上保留当前规则但增加验证统计，避免全部被挤到单一对象类别
- 新流程应打印最小必要进度日志：`start`、阶段进展、`done`
- 需要对“候选供应不足”提供降级策略，但不能悄悄修改“100 条”目标，应明确报告当前达到的数量和阻塞原因
- 需要定义人工标注任务流的落点，例如写入专用待办文件、标注队列或现有人工标注入口可消费的清单

## System-Wide Impact

- **Interaction graph**: 新 `augment` 命令会复用 `paperlists_parser`、`BenchmarkBuilder`、`AnnotationRepository`、AI annotator 和 `refresh_benchmark_stats`，因此需要保持与现有 JSONL 协议完全兼容。
- **Error propagation**: 如果 venue 文件缺失、候选去重失败、写回前发现重复 `paper_id`，必须整批失败，不能部分提交；AI 预标失败时应允许记录已写入但明确输出断点信息，以便后续续跑。
- **State lifecycle risks**: 当前 `rebalance` 是先写 `records` 再补 AI 标注。新流程若沿用该顺序，需要定义恢复策略，避免“记录已新增但预标未完成”时无法识别本次批次。
- **API surface parity**: 除新增 CLI 外，还要同步补上服务层公共函数和可测试配置对象，避免把策略硬编码在工具脚本中。
- **Integration test scenarios**:
  - 调度类从 19 条向 100 条补样时，是否能在达到目标后正确停止
  - 候选论文多标签命中时，是否稳定落入调度类或被正确排除
  - 新增记录后 `stats.json` 是否反映 AI 层和 records 层的一致增量
  - 中途 AI 标注失败后重跑，是否不会重复写入记录或重复提交同一论文
  - 新增记录是否全部进入人工标注待处理任务流

## SpecFlow Analysis

从使用流程看，这个需求至少包含五条关键流：

1. **现状分析流**：读取现有统计，确认调度类当前只有 19 条并计算到 100 条的缺口
2. **候选发现流**：扫描 paperlists 的 2025/2026 accepted 论文，形成调度类候选池
3. **调度专项流**：从高优先级 venue 定向补充 `系统与调度优化`，直到达到 100 条
4. **AI 标注流**：新增记录进入 AI 预标，保证每条新论文都有机器标注结果
5. **人工标注流**：新增记录全部进入人工标注待处理任务流
6. **写回与验证流**：刷新 stats、输出报告并允许复跑

需要提前明确的边界与缺口：

- 当一篇论文同时命中多个子类时，优先级如何定义，才能稳定保留调度类专项样本
- “顶会”白名单是否只限 `aaai/iclr/icml/nips/cvpr/iccv/acl/emnlp/colm/www`，还是允许更广泛的 2025/2026 venue 集
- 调度类增强是否只面向 LLM/VLM serving，还是接受更广义 AI systems scheduling
- 人工标注任务流的具体载体是什么，以及如何保证新增论文不会遗漏
- 是否要为增强批次打上可追踪备注，例如 `notes += augment_batch=<date>-scheduling`，便于后续审计与复跑

## Acceptance Criteria

- [ ] 新增一条独立的“调度类专项增强”服务链路，不改变现有 `rebuild` 与 `rebalance` 的核心语义
- [ ] 增强流程支持读取 2025/2026 accepted 论文，并允许显式传入 `--paperlists-root`
- [ ] 流程能基于当前统计识别 `系统与调度优化` 缺口，并按 100 条目标补样
- [ ] 流程能对 `系统与调度优化` 应用更细粒度的检索与排序规则
- [ ] 去重仍满足 `paper_id` 与 `title + abstract` 指纹双门禁
- [ ] 新增记录全部完成 AI 标注，并全部进入人工标注任务流
- [ ] 新增记录不得绕过人工流程直接写入 `merged.jsonl`
- [ ] 运行完成后，`stats.json` 中 `系统与调度优化` 达到或尽可能接近 100 条，并有增量说明
- [ ] 至少补充覆盖 `iclr 2026`、`nips 2025`、`icml 2025`、`aaai 2025` 这四个调度类高价值来源
- [ ] 为服务层、CLI 解析、去重、停止条件和统计刷新补充单元测试

## Success Metrics

- `系统与调度优化` 从当前 19 条提升到 100 条，或在候选/流程受阻时明确报告未达成原因
- 新流程完成后无重复 `paper_id`
- `stats.json`、`records.jsonl` 与 `annotations-ai.jsonl` 的数量关系保持一致
- 本次新增的全部文章都能在人工标注待办中被追踪到
- 同一输入、同一随机种子下，抽样结果可复现

## Dependencies & Risks

- **数据源依赖**: 依赖 `d:\Git_Repo\Paper-Analysis-New\third_party\paperlists\` 中对应 venue/year JSON 可读取且 accepted 状态可解析
- **规则风险**: 纯关键词会把部分“训练系统”或“资源管理”论文误分为调度类，需要通过更强排除词和抽样审查来控制
- **协议风险**: 当前单标签 schema 会压缩多面向论文的信息，可能导致调度类与内核类边界样本被迫单归类
- **流程风险**: 先写 records 后打 AI 标注的顺序在异常时会留下“待补标”窗口，需要可恢复设计
- **人工流转风险**: 如果只补了 AI 标注而没有稳定写入人工标注待办，会出现“数据进库了但没人复核”的断层
- **规模风险**: 从 19 条补到 100 条意味着至少新增 81 条调度类正样本，人工复核负担会明显上升，需要按批次推进

## Suggested Implementation Shape

### Phase 1: Distribution planning

- 新增分布目标配置对象，例如 `paper_analysis_dataset/services/augmentation_plan.py`
- 基于当前 `stats.json` 计算调度类距离 100 条的缺口
- 产出结构化执行计划和批次摘要

### Phase 2: Candidate retrieval and scoring

- 扩展 `benchmark_builder.py` 中子标签匹配与调度类得分规则
- 新增按标签、venue、year 检索候选的服务函数
- 为调度类增加优先 venue 排序和排除词

### Phase 3: Incremental augmentation write path

- 新增 `paper_analysis_dataset/services/augment_benchmark.py`
- 在写回前执行去重和停止条件检查
- 分批写入 records、触发 AI 预标、写入人工标注待办、刷新 stats

### Phase 4: Tooling and tests

- 新增 CLI：`paper_analysis_dataset/tools/augment_paper_filter_benchmark.py`
- 增补 `tests/unit/` 中的服务层与 CLI 测试
- 产出增强报告，记录新增样本数、调度类进度、人工标注待办数量和来源 venue

## Sources & References

- Internal protocol: `docs/benchmarks/paper-filter-dataset-protocol.md`
- Label spec: `docs/benchmarks/paper-filter-label-spec.md`
- Current builder heuristics: `paper_analysis_dataset/services/benchmark_builder.py`
- Current incremental rebalance flow: `paper_analysis_dataset/services/rebalance_benchmark.py`
- CLI entrypoints: `paper_analysis_dataset/tools/rebuild_paper_filter_benchmark.py`, `paper_analysis_dataset/tools/rebalance_paper_filter_benchmark.py`
- Current dataset distribution: `data/benchmarks/paper-filter/stats.json`
- Candidate source root: `d:\Git_Repo\Paper-Analysis-New\third_party\paperlists\`
- Related papers informing keyword design:
  - `MuxServe: Flexible Spatial-Temporal Multiplexing for Multiple LLM Serving`
  - `DistServe: Disaggregating Prefill and Decoding for Goodput-optimized Large Language Model Serving`
  - `HexGen-2: Disaggregated Generative Inference of LLMs in Heterogeneous Environment`
  - `Adaptive Rescheduling in Prefill-Decode Disaggregated LLM Inference`
  - `Efficiently Serving Large Multimodal Models Using EPD Disaggregation`
  - `EPD-Serve: A Flexible Multimodal EPD Disaggregation Inference Serving System On Ascend`
  - `Sem-MoE: Semantic-aware Model-Data Collaborative Scheduling for Efficient MoE Inference`
  - `Capacity-Aware Inference: Mitigating the Straggler Effect in Mixture of Experts`
  - `SpaceServe: Spatial Multiplexing of Complementary Encoders and Decoders for Multimodal LLMs`
