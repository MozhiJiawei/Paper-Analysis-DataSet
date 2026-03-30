---
status: pending
priority: p2
issue_id: "002"
tags: [code-review, quality, benchmark, rebalance]
dependencies: []
---

# 让补样结果更贴近目标比例

## Problem Statement

新的 rebalance 流程只有在整批数据都追加完成之后，才会检查 AI 正样本比例是否已经 `<= target_ai_positive_ratio`。这意味着当前实现表达的是“把比例压到不高于 30%”，而不是“调整到约 30% 范围”。在默认 `batch_size=50` 的情况下，对于中小规模数据集，一次批量补样就可能把最终比例压得明显低于目标值。

## Findings

- [paper_analysis_dataset/services/rebalance_benchmark.py](D:\Git_Repo\Paper-Analysis-New\third_party\paper_analysis_dataset\paper_analysis_dataset\services\rebalance_benchmark.py) 第 116-118 行的停止判断，只会在开始下一批之前执行。
- [paper_analysis_dataset/services/rebalance_benchmark.py](D:\Git_Repo\Paper-Analysis-New\third_party\paper_analysis_dataset\paper_analysis_dataset\services\rebalance_benchmark.py) 第 134-149 行总是整批追加 `batch_candidates`，然后才在批次结束后重新判断比例。
- 默认批大小在 [paper_analysis_dataset/services/rebalance_benchmark.py](D:\Git_Repo\Paper-Analysis-New\third_party\paper_analysis_dataset\paper_analysis_dataset\services\rebalance_benchmark.py) 第 25 行定义为 50，这会放大比例“过冲”的风险。
- 当前测试只在 [tests/unit/test_rebalance_paper_filter_benchmark.py](D:\Git_Repo\Paper-Analysis-New\third_party\paper_analysis_dataset\tests\unit\test_rebalance_paper_filter_benchmark.py) 第 172-184 行用 `batch_size=1` 验证了精确命中目标的情况；并没有测试在更真实的批大小下，最终比例是否仍落在一个可接受的 30% 附近区间。

## Proposed Solutions

### Option 1: 对最后一批做动态缩容

**Approach:** 先估算为了达到目标比例还需要补多少负样本，再把当前批次大小裁剪到这个数量附近。

**Pros:**
- 最直接地贴合“约 30%”这个目标。
- 对外使用方式几乎不用变化。

**Cons:**
- 需要增加一段“比例反推数量”的计算逻辑。
- 仍然依赖实际 AI 标注结果与“预期为负样本”的假设大体一致。

**Effort:** 2-3 小时

**Risk:** 低

---

### Option 2: 在接近阈值时切换为逐条补样

**Approach:** 当比例接近目标值时，不再整批追加，而是逐条补样与标注，直到进入约定好的容差区间。

**Pros:**
- 对最终比例的控制最精确。
- 测试上也更容易表达与验证。

**Cons:**
- 最后阶段 I/O 会变多。
- 控制流会比现在复杂一些。

**Effort:** 3-5 小时

**Risk:** 中

---

### Option 3: 明确定义并文档化容差区间

**Approach:** 保持现有批处理逻辑，但显式规定一个可接受区间，例如 `0.25-0.35`，并据此编写测试。

**Pros:**
- 实现成本最低。
- 能让使用者明确知道工具承诺的行为边界。

**Cons:**
- 本身并不会提升精度。
- 如果用户期望更接近 30%，仍然可能觉得不够符合预期。

**Effort:** 1-2 小时

**Risk:** 低

## Recommended Action

**To be filled during triage.**

## Technical Details

**Affected files:**
- `paper_analysis_dataset/services/rebalance_benchmark.py`
- `paper_analysis_dataset/tools/rebalance_paper_filter_benchmark.py`
- `tests/unit/test_rebalance_paper_filter_benchmark.py`

**Related components:**
- AI 层分布统计
- batch size 默认值
- CLI 面向操作者的语义表达

**Database changes (if any):**
- 无

## Resources

- **Review context:** 当前分支关于“将正样本分布调整到约30%范围”的代码审查
- **Relevant code:** `paper_analysis_dataset/services/rebalance_benchmark.py:116`
- **Relevant code:** `tests/unit/test_rebalance_paper_filter_benchmark.py:172`

## Acceptance Criteria

- [ ] rebalance 算法明确说明目标语义究竟是 `<= target`，还是“落入某个容差区间”。
- [ ] 对代表性数据集，最终 AI 正样本比例能稳定落在约定区间内。
- [ ] 测试覆盖真实批大小场景，而不只是 `batch_size=1`。
- [ ] CLI help 文案与实际停止语义保持一致。

## Work Log

### 2026-03-30 - 创建审查问题

**By:** Codex

**Actions:**
- 审查了 target ratio 的停止条件和默认批大小配置。
- 对照“约 30%”的产品预期核对了当前实现语义。
- 确认现有测试只覆盖了 `batch_size=1` 下的精确命中场景。

**Learnings:**
- 当前功能是可复现、可测试的，但停止语义只保证单边约束。
- 默认批大小会让中小 benchmark 更容易偏离“约 30%”的直觉预期。

## Notes

- 这个问题很重要，因为即便所有测试都通过，它仍然会影响功能是否真正符合产品预期。
