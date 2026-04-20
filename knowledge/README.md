# Knowledge

这个目录是 Noon 项目的 Git 管理知识库主源。

## 目标

- 让源码、正式工程文档、知识笔记都能进入 Git / GitHub 协同
- 让 Obsidian 只做知识阅读与编写入口，不再承载完整代码工作副本
- 保证多机协同和多人协同时，只有一份可审计的事实源

## 结构

- `architecture/`
  系统架构、模块地图、当前开发上下文、由正式文档投影出的 managed notes
- `methods/`
  可复用方法论、交付模式、Codex 协作方式
- `requirements/`
  需求拆解、功能规划、需求笔记
- `project-ops/`
  项目驾驶舱、正式任务清单、调度台
- `dev-journal/`
  开发日记与阶段过程记录
- `reference/`
  长期参考资料，例如佣金、定价、平台规则笔记

## 规则

- `noon-selection-tool/docs/` 仍然承载正式工程文档与协作契约
- `knowledge/` 承载适合在 Obsidian 中阅读和沉淀的知识笔记
- 运行期数据库、日志、缓存、快照、浏览器配置不进入这里
- 任何会影响系统事实边界的内容，先改 repo 正式文档，再刷新这里的 managed notes

## Obsidian 接入

推荐两种方式：

1. 直接在 Obsidian 中打开仓库内的 `knowledge/`
2. 通过 `scripts/link_repo_knowledge_into_obsidian.ps1` 把对应目录链接进 Vault

不再推荐把整份 repo 复制到 Obsidian Vault 中维护。
