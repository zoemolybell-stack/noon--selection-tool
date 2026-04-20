# ERP 工作台 UI 交付模式

<!-- generated: scripts/sync_obsidian_context.py -->

更新时间：`2026-04-11 00:39:10`

## 固定流水线

1. page spec
2. data contract check
3. implementation
4. screenshot validation
5. UI critic
6. real-data regression
7. repair
8. manual review

## 关键原则

- 表格是主工作面，不是附属组件
- 详情优先进 drawer / split pane，不频繁整页跳转
- KPI 要紧凑、可比较、可扫描
- 页面必须回答：我在看什么、哪里异常、下一步是什么、还能 drill 到哪
