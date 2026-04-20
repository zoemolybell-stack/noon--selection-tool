# GitHub 与 Obsidian 知识库收敛方案

Last Updated: 2026-04-20

## Summary

当前项目已经具备 GitHub 私有仓库作为源码主仓的条件，但 Obsidian Vault 里仍混有完整代码副本、运行目录和手工笔记，事实源没有完全收敛。

本方案将项目固定为：

- Git / GitHub：代码与知识资产唯一权威源
- Obsidian：知识阅读、检索、编写入口
- 本地 / NAS：运行时数据库、日志、快照、浏览器配置

## Canonical Rules

1. `D:\claude noon v1` 是当前 Git 工作树，Codex 直接基于这份 Git 代码执行开发任务。
2. `noon-selection-tool/docs/` 负责正式工程文档、协作边界、运行契约。
3. `knowledge/` 负责知识笔记、方法论、项目推进面板、开发日记和参考资料。
4. Obsidian 不再承载完整代码仓库副本，不再作为第二事实源。
5. `runtime_data/`、`data/`、`logs/`、`tmp/`、`venv/`、数据库文件和密钥不进入 GitHub。

## Directory Mapping

| 当前 Obsidian 路径 | 目标 Git 路径 | 归属 | 处理方式 |
|---|---|---|---|
| `系统架构知识库/` | `knowledge/architecture/` | Git 管理 | 迁入 Git；managed notes 由脚本刷新 |
| `可复用方法论/` | `knowledge/methods/` | Git 管理 | 迁入 Git；generated + manual notes 共存 |
| `爬虫开发需求笔记/` | `knowledge/requirements/` | Git 管理 | 目录保留；成熟需求再迁入 |
| `项目推进与调度/` | `knowledge/project-ops/` | Git 管理 | 迁入 Git |
| `noon开发日记/` | `knowledge/dev-journal/` | Git 管理 | 迁入 Git |
| `平台佣金/` | `knowledge/reference/pricing/` | Git 管理 | 迁入 Git |
| Vault 根目录定价笔记 | `knowledge/reference/pricing/` | Git 管理 | 迁入 Git 并归到 pricing |
| `workspace/` | 无 | 本地保留 | 废弃为长期主路径；不再继续扩展 |
| Vault 根目录 `AGENTS.md` | 无 | 本地保留 | repo 根目录已有 canonical `AGENTS.md` |
| 个人临时草稿 | 无或后续 `knowledge/` | 本地优先 | 成熟后再迁入 Git |

## Script Roles After Refactor

### `scripts/sync_obsidian_context.py`

新定位：

- 从 `noon-selection-tool/docs/` 读取正式文档
- 生成 repo 内 `knowledge/architecture/` 和 `knowledge/methods/` 的 managed notes
- 默认仍可镜像到旧 Obsidian 目录，便于过渡

不再是：

- 把 repo 当成源，然后把 Obsidian 当作主知识库长期承载

### `scripts/run_obsidian_sync.ps1`

新定位：

- 作为知识刷新入口，驱动 `sync_obsidian_context.py`
- 先刷新 repo 内 `knowledge/`，再按参数决定是否镜像到旧 Vault 目录

### `scripts/migrate_repo_to_obsidian_workspace.ps1`

新定位：

- 已废弃
- 不再允许把整份 repo 迁入 Obsidian Vault

### `scripts/link_repo_knowledge_into_obsidian.ps1`

新定位：

- 为 Obsidian 创建“知识目录链接”
- 只把 `knowledge/` 的各子目录接入 Vault
- 不触碰代码、运行时目录和数据库

## What Moves Into Git Now

- `系统架构知识库/*.md`
- `可复用方法论/*.md`
- `项目推进与调度/*.md`
- `noon开发日记/*.md`
- `平台佣金/*.md`
- Vault 根目录下与定价/佣金相关的知识笔记

## What Stays Local Only

- `workspace/`
- 完整代码副本镜像
- `.env`
- `venv/`
- `runtime_data/`
- `data/`
- `logs/`
- `tmp/`
- 浏览器 profile、cookies、token、数据库和快照

## Execution Checklist

1. 在 Git 仓库内建立 `knowledge/` 主结构。
2. 将现有 Obsidian 知识笔记复制到对应的 `knowledge/` 子目录。
3. 将 `sync_obsidian_context.py` 改为先刷新 repo `knowledge/`。
4. 将旧 `migrate_repo_to_obsidian_workspace.ps1` 废弃，避免继续迁整仓到 Vault。
5. 需要继续使用现有 Vault 目录时，用镜像或目录链接接入 `knowledge/`。
6. 第二台机器只需要 `git clone` 仓库，再让 Obsidian 打开 `knowledge/` 或建立目录链接。
7. 运行期数据继续由本地 / NAS / 数据库承担，不进入 GitHub。

## Recommended Operating Mode

推荐优先级如下：

1. 最优：Obsidian 直接打开仓库内的 `knowledge/`
2. 次优：使用 `scripts/link_repo_knowledge_into_obsidian.ps1` 建立 Vault 目录链接
3. 过渡：继续使用旧 Vault 目录镜像，但不再维护完整 repo 副本

## Effect Boundary

本方案不修改：

- Web 读取统一数据库的方式
- 类目爬虫与关键词爬虫运行路径
- warehouse 稳定表名
- 生产数据库位置

本方案只收敛：

- 代码与知识资产的 Git 管理方式
- Obsidian 的角色边界
- 旧同步脚本与旧迁移脚本的职责
