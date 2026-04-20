# NAS 发布与运行治理

<!-- generated: scripts/sync_obsidian_context.py -->

更新时间：`2026-04-20 17:56:10`

## 当前阶段
- `single control plane + remote category node steady state`

## 当前 NAS 稳定版
- `huihaokang-nas-20260414-keyword-control-r68`

## 运行治理关注点
- category crawler gradual layering
- remote category steady-state hardening
- keyword protection and runtime observability no-regression
- doc/knowledge-base alignment

## 关键规则
- NAS 只接受显式 release bundle
- 现网保持 retained-data Postgres
- 发布后必须执行 runtime reconciliation
- 运行中心应优先展示 operator 可读状态，而不是原始任务字符串

## 来源
- [PROJECT_WHITEPAPER.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PROJECT_WHITEPAPER.md)
- [DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md)
- [DEV_COLLAB_LOG.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_COLLAB_LOG.md)
