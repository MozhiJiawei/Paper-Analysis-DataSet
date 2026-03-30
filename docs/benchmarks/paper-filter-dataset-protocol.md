# 数据集协议

## 目录

- `data/benchmarks/paper-filter/records.jsonl`
- `data/benchmarks/paper-filter/annotations-ai.jsonl`
- `data/benchmarks/paper-filter/annotations-human.jsonl`
- `data/benchmarks/paper-filter/merged.jsonl`
- `data/benchmarks/paper-filter/conflicts.jsonl`
- `data/benchmarks/paper-filter/schema.json`
- `data/benchmarks/paper-filter/stats.json`

## 核心文件

- `records.jsonl`：benchmark 唯一论文主表，也是唯一允许保存外部元数据的位置
- `annotations-ai.jsonl`：AI 预标快照，只保存 `paper_id` 和标注字段
- `annotations-human.jsonl`：人工复标，只保存 `paper_id` 和标注字段
- `merged.jsonl`：合并后的最终标注结果，只保存 `paper_id` 和最终标注字段
- `conflicts.jsonl`：冲突与仲裁状态，只保存 `paper_id` 与冲突标注内容
- `schema.json`：单版本协议的机器可读说明
- `stats.json`：当前数据集整体分布摘要

## 增量补录

- 当前支持通过 `paper-analysis-dataset-rebalance` 对 benchmark 做非破坏性的 accepted 论文增量补录
- 增量补录只会从显式指定的 venue/year 中读取 accepted 论文，并默认只抽样启发式规则判为 `negative` 的候选
- 抽样前必须先排除已出现在 `records.jsonl`、`annotations-ai.jsonl`、`annotations-human.jsonl`、`merged.jsonl`、`conflicts.jsonl` 中的 `paper_id`
- 候选池内部必须同时做 `paper_id` 去重与规范化 `title + abstract` 指纹去重；若新候选与现有主表论文指纹相同，则优先保留现有主表记录
- 每次批量写回前都必须再次校验根表 `paper_id` 全局唯一；若发现冲突，整批失败，不允许部分写入
- `records.jsonl` 仍是唯一元数据主表；新增论文不会自动进入 `annotations-human.jsonl`、`merged.jsonl`
- 增量流程只能对缺失 AI 标注的新增论文执行预标，既有 `annotations-ai.jsonl` 内容不得先清空再重写
- 本轮分布控制与停止条件以 `stats.json -> by_layer.annotations_ai` 为准

## 门禁

- 根 `records.jsonl` 中每条记录都必须是 UTF-8 JSONL
- `paper_id` 在根主表中必须全局唯一
- 规范化 `title + abstract` 指纹在根主表中也必须保持唯一
- 论文标题、英文摘要、中文摘要、作者、venue、source 等外部元数据只能出现在根 `records.jsonl`
- 根 `records.jsonl` 不得保存 `final_*` 聚合标注字段
- `annotations-ai.jsonl`、`annotations-human.jsonl`、`merged.jsonl`、`conflicts.jsonl` 不得重复携带外部元数据
- `merged.jsonl` 中同一 `paper_id` 必须全局唯一
- 未解决冲突的样本不得进入 `merged.jsonl`
- 本协议不再引入 `calibration`、`v1`、`release`、`split` 等目录或字段

## 标注字段约束

- `preference_labels` 使用数组存储，但当前协议要求长度只能为 `0..1`
- `negative_tier=positive` 时必须且只能选择 1 个 `preference_label`
- `negative_tier=negative` 时必须保持 `preference_labels=[]`

## 统计口径

- `stats.json` 顶层字段继续保留，用于兼容既有消费者
- `stats.json.by_layer.annotations_ai.total_records` 表示 AI 标注层样本量
- `stats.json.by_layer.annotations_ai.by_negative_tier` 表示 AI 标注层正负分布
- `stats.json.by_layer.annotations_ai.positive_ratio` 是增量补录流程的目标控制口径
