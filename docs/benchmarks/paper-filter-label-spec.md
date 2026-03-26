# 论文筛选标签规范

## A 轴：偏好标签

- `解码策略优化`
- `上下文与缓存优化`
- `系统与调度优化`
- `算子与内核优化`
- `模型结构侧推理优化`
- `模型压缩`

当前协议为单选：

- `negative_tier=positive` 时，必须且只能选择 1 个偏好标签
- `negative_tier=negative` 时，必须保持 `preference_labels=[]`

## B 轴：主研究对象

- `LLM`
- `多模态 / VLM`
- `Diffusion / 生成模型`
- `通用机器学习`
- `强化学习 / 序列决策`
- `检索 / 推荐 / 搜索`
- `计算机视觉`
- `语音 / 音频`
- `AI 系统 / 基础设施`
- `评测 / Benchmark / 数据集`

每篇论文必须且只能有一个主研究对象标签。

## 样本极性

- `positive`
- `negative`

`positive` 仅用于命中 A 轴标签的样本，必须填写且只能填写一个 `preference_label`。

`negative` 用于未命中目标偏好标签的样本，必须保持 `preference_labels=[]`。
