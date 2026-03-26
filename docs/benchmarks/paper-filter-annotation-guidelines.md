# 双人标注手册

## 角色

- 标注员 A：`AI`
- 标注员 B：人类网页标注员
- 仲裁者：仅处理冲突样本

## 工作流

1. 维护者先生成 `records.jsonl`，作为唯一论文主表；主表中的 `abstract_zh` 由 Doubao 翻译生成
2. 对全部进入数据集的 `paper_id` 统一触发 AI 预标，写入 `annotations-ai.jsonl`
3. 人类网页只对已有 AI 预标的 `paper_id` 录入复标，写入 `annotations-human.jsonl`
4. 系统自动生成 `conflicts.jsonl`
5. 无冲突样本直接进入 `merged.jsonl`
6. 仲裁完成后把最终结果写入 `merged.jsonl`，不要回写 `records.jsonl`

## 标注要求

- 每条记录必须填写一个 `primary_research_object`
- `negative_tier` 只允许 `positive` 或 `negative`
- `preference_labels` 为单选：`positive` 时必须且只能有 1 个标签，`negative` 时必须为空
- `negative` 样本必须保持 `preference_labels=[]`
- 人类复标阶段不要直接覆盖 AI 结果，应保存为独立文件
- 标注文件与冲突文件不得重复保存标题、摘要、venue、source 等外部元数据
