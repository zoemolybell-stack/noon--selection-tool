# NAS 发布迁移回滚模式

<!-- generated: scripts/sync_obsidian_context.py -->

更新时间：`2026-04-11 00:39:10`

## 当前项目通用经验

- 本地 beta 是唯一开发基线
- NAS 只接受明确 release bundle
- 发布前先跑本地 stabilization/self-check
- retained-data 数据卷优先，不把本地数据直接导入生产
- rollback 先回代码，再按需要回数据源

## 参考

- [NAS_DEPLOYMENT_RUNBOOK.md](D:/claude%20noon%20v1/noon-selection-tool/docs/NAS_DEPLOYMENT_RUNBOOK.md)
- [NAS_RELEASE_ROLLBACK.md](D:/claude%20noon%20v1/noon-selection-tool/docs/NAS_RELEASE_ROLLBACK.md)
