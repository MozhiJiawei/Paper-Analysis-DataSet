---
status: pending
priority: p1
issue_id: "001"
tags: [code-review, data-integrity, benchmark, rebalance]
dependencies: []
---

# 让补样批次写入具备原子性

## Problem Statement

`rebalance_benchmark()` 目前会在同一批次的 AI 标注完全成功之前，先把新增 benchmark 记录写入磁盘。如果标注过程中途崩溃或抛错，数据集就可能停留在“半完成”状态：`records.jsonl` 已经包含新论文，但 `annotations-ai.jsonl` 和 `stats.json` 还不完整。这样会破坏“每条补入记录都应伴随 AI 层标签”的预期，也会导致后续重跑时跳过这些实际上没有被完整处理的论文。

## Findings

- 在 [paper_analysis_dataset/services/rebalance_benchmark.py](D:\Git_Repo\Paper-Analysis-New\third_party\paper_analysis_dataset\paper_analysis_dataset\services\rebalance_benchmark.py) 第 134-149 行，代码先写入 `next_records`，随后才触发标注副作用。
- `annotate_missing_candidates()` 会在 [paper_analysis_dataset/services/rebalance_benchmark.py](D:\Git_Repo\Paper-Analysis-New\third_party\paper_analysis_dataset\paper_analysis_dataset\services\rebalance_benchmark.py) 第 310-323 行按完成顺序逐条持久化 annotation，因此只要某个 future 失败，就可能留下一个只写入了部分 annotation 的批次。
- 下一次运行时，`blocked_paper_ids` 会从已有 records 构造，见 [paper_analysis_dataset/services/rebalance_benchmark.py](D:\Git_Repo\Paper-Analysis-New\third_party\paper_analysis_dataset\paper_analysis_dataset\services\rebalance_benchmark.py) 第 84-90 行。因此那些已经写进 `records.jsonl`、但没有完整标注成功的论文会被候选池排除，无法自动重试。
- 当前测试覆盖了成功路径、候选池耗尽、去重和幂等性，但没有模拟“records 已写入后 annotator 在批次中途失败”的场景。

## Proposed Solutions

### Option 1: 先在内存中完成整批标注，再统一提交

**Approach:** 先把这一批的 annotation 结果全部收集到内存里，只有整批都成功后，才一次性写入 `records.jsonl`、`annotations-ai.jsonl` 和 `stats.json`。

**Pros:**
- 可以避免半批次持久化。
- 能保证 benchmark 各文件之间的一致性。

**Cons:**
- 每批会多占用一些内存。
- 需要调整当前 annotation helper 的实现方式。

**Effort:** 2-4 小时

**Risk:** 低

---

### Option 2: 批次失败时做回滚

**Approach:** 在处理批次前先保存当前文件状态，一旦任意 annotation future 失败，就把文件恢复到批次开始前的状态。

**Pros:**
- 对现有控制流改动较小。
- 可以保留当前“边完成边处理”的 annotation 循环。

**Cons:**
- 文件系统层面的复杂度更高。
- 容易漏掉某个需要回滚的产物文件。

**Effort:** 3-5 小时

**Risk:** 中

---

### Option 3: 引入批次清单与恢复逻辑

**Approach:** 持久化记录“进行中的批次”，并在启动时自动检查并修复未完成批次，决定继续执行还是回滚。

**Pros:**
- 具备更强的运维可观测性。
- 不仅能处理异常，也能覆盖进程崩溃场景。

**Cons:**
- 实现最复杂。
- 对当前工具来说可能有些过度设计。

**Effort:** 1-2 天

**Risk:** 中

## Recommended Action

**To be filled during triage.**

## Technical Details

**Affected files:**
- `paper_analysis_dataset/services/rebalance_benchmark.py`
- `tests/unit/test_rebalance_paper_filter_benchmark.py`

**Related components:**
- `AnnotationRepository`
- 增量 annotator 后端
- `stats.json` 生成逻辑

**Database changes (if any):**
- 无

## Resources

- **Review context:** 当前分支关于“将正样本分布调整到约30%范围”的代码审查
- **Relevant code:** `paper_analysis_dataset/services/rebalance_benchmark.py:134`
- **Relevant code:** `paper_analysis_dataset/services/rebalance_benchmark.py:310`

## Acceptance Criteria

- [ ] 批次失败时，不能留下“新增 record 已写入，但 AI annotation 未完整写入”的状态。
- [ ] annotator 失败后重新运行工具时，不会悄悄跳过之前半写入的论文。
- [ ] 单元测试至少覆盖一个批次中途失败的场景。
- [ ] `stats.json` 只反映成功提交的批次结果。

## Work Log

### 2026-03-30 - 创建审查问题

**By:** Codex

**Actions:**
- 审查了新的 rebalance service 和 CLI。
- 梳理了 records、AI annotations 和 stats 的写入顺序。
- 确认现有测试没有覆盖批次中途 annotation 失败的情况。

**Learnings:**
- 当前 happy path 是正确的，但还不具备 crash-safe 能力。
- 现有重跑幂等性依赖 `records.jsonl`，这让“部分写入”问题尤其危险。

## Notes

- 这个问题应视为阻塞合并，因为它可能在真实数据上破坏 benchmark 状态。
