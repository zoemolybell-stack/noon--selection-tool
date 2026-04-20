# ERP(Web Beta)架构与页面逻辑

<!-- generated: scripts/sync_obsidian_context.py -->

更新时间：`2026-04-11 00:39:10`

## 页面主链

- 研究首页：dashboard / scope / category cockpit
- 选品：command bar + summary ribbon + table + drawer
- 收藏夹：favorites list + same drawer
- 关键词研究：query-first + intelligence + hit products
- 运行中心：runtime alerts + freshness + tasks / workers / imports

## 当前前端模块

- [web_beta/static/app.contract.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.contract.js)
- frozen contract constants and default workbench state factory
- [web_beta/static/app.services.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.services.js)
- fetch wrappers and JSON helpers
- [web_beta/static/app.state.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.state.js)
- route normalization, scope encoding, route parsing/building
- [web_beta/static/app.selection.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.selection.js)
- active selection workbench functions
- [web_beta/static/app.favorites.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.favorites.js)
- favorites page and product favorite interactions
- [web_beta/static/app.home.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.home.js)
- home/dashboard rendering and scope interactions

## 当前要求

- 主链页面继续拆模块，不回到 patch-on-patch
- 运行中心要直接显示 watchdog / keyword quality / shared sync
- UI 验收先过截图与 real-data regression，再进入人工 review

## 参考

- [CODEBASE_MAP.md](D:/claude%20noon%20v1/noon-selection-tool/docs/CODEBASE_MAP.md)
- [DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md)
