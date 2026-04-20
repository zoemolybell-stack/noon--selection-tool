# Architecture

这里放系统架构、仓库模块地图、运行模型、数据读写边界等知识。

## 内容类型

- repo 正式文档投影出的 managed notes
- 适合长期保留的架构说明
- 便于 Obsidian 检索的结构化知识卡片

## 规则

- 带 `generated` 注释的 managed notes 由 `scripts/sync_obsidian_context.py` 生成
- 若系统事实发生变化，先改 `noon-selection-tool/docs/`，再刷新这里
- 不要把运行时快照、数据库状态和临时排障文本直接沉积到这里
