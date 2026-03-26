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

## 门禁

- 根 `records.jsonl` 中每条记录都必须是 UTF-8 JSONL
- `paper_id` 在根主表中必须全局唯一
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
