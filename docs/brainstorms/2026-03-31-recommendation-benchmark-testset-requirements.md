---
date: 2026-03-31
topic: recommendation-benchmark-testset
---

# Recommendation Benchmark Testset

## Problem Frame
当前 `paper-filter` 数据集更像二元筛选 benchmark：`positive/negative` 加单个 `preference_label`，适合评估“是否命中目标主题”。如果推荐算法要继续演进到支持 `positive/negative` 分类、子标签标记，并把目标放在高召回 `>90%`、准确率 `>50%`，现有测试集的信息密度和评测约束都不够。

这个数据集的定位应是高准确性的独立测试集，不参与推荐算法研发，不为模型提供训练信号，而是稳定、严格地衡量不同算法版本是否真的更好，尤其要能发现漏召回、6 元子类标签误分，以及边界负样本误判。

## Requirements
- R1. 测试集必须支持两层核心真值：`gold_decision`（positive / negative）与 `gold_preference_labels`（保持现有 6 元标签体系，允许未来按需扩展到多标签，但当前阶段不要求更细 `subtags`）。
- R2. 每个 `positive` 样本必须至少附带 1 条可核验的标注证据，优先来自标题、摘要或关键词中的证据片段，保证评测结论可复查。
- R3. `negative` 样本不能只表示“不是目标”；需要进一步区分少量低成本的负样本类型，至少覆盖“主题无关”与“非推理优化但表述模糊、容易被误判为 6 类推理优化之一的边界负样本”。
- R4. 数据集需要显式覆盖召回风险最高的样本层，包括边界正样本、易混淆负样本、长尾标签样本、不同研究对象下的正样本。
- R5. 6 个 `preference_labels` 都需要达到最低支持数，避免出现当前这类极低频标签无法稳定评估的情况；特别是长尾类必须优先补齐。
- R6. 评测时必须同时输出整体指标和分层指标，至少包括：positive recall、positive precision、各偏好标签 recall / precision、边界样本召回、硬负样本准确率。
- R7. 测试集需要按用途拆分成不同评测切片，至少包括真实分布切片、子类均衡切片和硬负样本切片；正式对比所用核心测试子集仍需冻结版本并保持盲测属性。
- R8. 数据集构建流程必须优先保障真值准确性而不是构建速度，要求双人复核、冲突仲裁、抽样审计和周期性回查。
- R9. 人工标注成本必须被严格控制：默认全量走 AI 预标，人工优先投入到长尾正样本、边界正样本、硬负样本和冲突样本，而不是对普通负样本做全量重标。

## Success Criteria
- 正式测试集可以稳定评估 `positive/negative + 6 元 preference_labels` 输出。
- 用该测试集评估时，能明确判断算法是否满足 `positive recall > 90%` 与 `precision > 50%` 的目标，而不是只得到单一 accuracy。
- 每个 `preference_label` 都有足够样本支持，评测结果不会被头部类分布或偶然样本波动主导。
- 标注准确性可追溯，抽查时可以复原每个 gold label 的判定依据。
- 算法团队无法通过反复查看全量测试真值来“学会测试集”。
- `hard negative` 上的误报风险可以被单独观测，不会被普通负样本稀释。

## Scope Boundaries
- 不把该数据集用于推荐模型训练、规则学习或阈值拟合。
- 不在本阶段定义具体模型结构、召回策略、在线实验或特征工程。
- 不引入额外细粒度子标签体系，除非后续评测明确证明 6 元标签不足以区分算法能力。
- 不追求让正式测试集完全服从自然论文分布；当评估稳定性需要时，允许通过切片与补样做受控分布设计。

## Key Decisions
- 高准确性优先于规模扩张：测试集是裁判，不是语料池。
- 当前以 6 元 `preference_labels` 作为推荐算法分类终点：避免在测试集侧引入不必要的标注复杂度。
- 将 `negative` 细分为少量、低成本、带语义的错误类型：所有真正属于推理优化的论文都应落入现有 6 元标签之一，负样本细分只服务于识别“无关样本”和“易混淆但并非推理优化的边界样本”。
- 引入 challenge slices 而不只看 overall：高召回目标下，最重要的是边界样本和长尾样本表现。
- 保留冻结盲测集并与可见分析集分层：否则测试集会逐渐失去评估价值。
- 6 元子类分布不均衡本身就是测试集缺陷：必须通过定向补样与均衡切片修复，不能只接受现状。
- 人工成本优先投向高价值样本：长尾正类、边界正样本、`hard negative`、冲突样本。

## Dependencies / Assumptions
- 假设推荐算法未来输出至少包含：正负判断、6 元 `preference_labels`。
- 假设当前仓库允许扩展标注 schema 和评测报告，但正式测试集仍保持脱敏和聚合报告输出。
- 假设人工标注资源有限，因此增强应按“尽量复用 AI 预标 + 只把人工投入到高价值样本与冲突样本”的原则分批完成，而不是一次性重做全量数据。

## Recommended Direction
建议把测试集增强拆成两条主线并行推进。

### 主线 A：修复 6 元子类分布
- 保留一个 `core positive set`，大体反映真实论文分布，用于观察整体效果。
- 新增一个 `balanced positive set`，专门为 6 元子类评估服务，对长尾类做定向补样。
- 每个 `preference_label` 先达到最低支持数，务实目标为每类至少 `40` 个高质量正样本；若人工预算允许，可提升到 `50-60`。
- 头部类 `上下文与缓存优化`、`模型压缩` 暂时不继续优先扩张，优先补 `算子与内核优化`、`系统与调度优化`、`解码策略优化`。
- 补样不能靠随机抽样，应通过定向候选召回完成，优先从标题、摘要、关键词和 venue 特征中搜集长尾类候选，再做 AI 预标与人工复核。

### 主线 B：补低成本 `hard negative`
- `negative` 不引入复杂多分类，只保留一种专项评测概念：`hard negative`。
- `hard negative` 指“不是推理优化，但非常容易被误判为 6 类推理优化之一”的论文，例如泛训练优化、通用系统论文、评测论文、数据集论文、与在线推理无关的效率论文。
- 对普通无关负样本不做重投入；对 `hard negative` 单独建切片，用于评估高召回目标下的误报控制能力。
- `hard negative` 候选优先通过启发式规则和 AI 预标筛出，人工只复核被机器判为高混淆风险的子集。

### 人工成本控制策略
- 全量候选先做 AI 预标。
- 人工优先复核 4 类样本：长尾正类候选、边界正样本、`hard negative` 候选、AI 与规则冲突样本。
- 普通负样本只做抽检，不做全量人工重标。
- 仲裁资源优先用于长尾类与 `hard negative`，而不是头部类简单样本。

### 建议的测试集切片
- `core_set`：接近真实分布，主要看整体 recall / precision。
- `balanced_positive_set`：6 元子类都有最低支持数，主要看各类 recall / precision 和 macro 指标。
- `hard_negative_set`：只放高混淆负样本，主要看误报率和 precision。

### 第一阶段务实目标
- 将 `算子与内核优化`、`系统与调度优化`、`解码策略优化` 补到每类至少 `40+` 高质量正样本。
- 新增一批 `hard negative`，初始规模建议 `40-60`。
- 头部类暂不继续明显扩张。
- 正式报告除 overall 外，必须强制输出每个 `preference_label` 的 recall / precision、macro recall，以及 `hard negative` 的误报率。

## Outstanding Questions

### Deferred to Planning
- [Affects R5][Needs research] 每个 `preference_label` 的最低支持数应设为 `40`、`50` 还是更高，才能在当前评测口径下获得足够稳定的 recall / precision？
- [Affects R7][Technical] `core_set`、`balanced_positive_set`、`hard_negative_set` 之间的样本是否允许重叠，还是应完全独立维护？
- [Affects R9][Technical] `hard negative` 候选的启发式筛选规则应如何定义，才能最大化混淆性同时最小化人工复核量？
- [Affects R7][Technical] 可见分析集与隐藏盲测集的比例如何划分更合适？

## Next Steps
→ /prompts:ce-plan for structured implementation planning
