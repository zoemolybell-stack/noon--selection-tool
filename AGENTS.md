# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project Overview

Noon 电商平台运营工具链 — 数据驱动的选品与定价决策系统。涵盖利润测算（4种模式）、爬虫数据采集、多维评分选品、竞争情报可视化等完整链路，服务中东跨境卖家。

## 文件说明

### 定价工具
| 文件 | 用途 |
|---|---|
| `generate_pricing_v6.py` (~1136 行) | V6 Pro 定价表 Excel 生成器，输出 3 Sheet: Config, UAE_Pricing, KSA_Pricing |
| `noon-pricing-v6.html` (~752 行) | 浏览器端在线定价计算器（单文件，无依赖） |
| `deploy/index.html` | Cloudflare Pages 部署副本 |

### 统一配置中心
| 文件 | 用途 |
|---|---|
| `noon_config.py` | **统一配置中心** — 所有佣金/配送费/汇率/VAT 集中管理，所有下游工具 import 此模块 |

### 数据分析工具
| 文件 | 用途 |
|---|---|
| `noon_data_pipeline.py` | 爬虫数据清洗与增强管道 — 输出多维评分增强报告 |
| `noon_selection_engine.py` | 选品决策引擎 — FBN/FBP 双模式利润测算 + S/A/B/C/D 评级 |
| `noon_category_analyzer.py` | 品类深潜分析工具 — 输出 Markdown 分析报告 |
| `noon_dashboard.html` | 竞争情报可视化仪表板（单文件 HTML，Chart.js + SheetJS） |
| `noon_price_analyzer.py` | 价格追踪分析器 — 价格变动/销量速度/竞争变化/异动告警 |
| `noon_seller_analyzer.py` | 卖家竞争分析器 — 集中度/品牌分布/进入难度评估 |
| `noon_data_fusion.py` | 多源数据融合器 — 合并所有数据源输出统一产品清单 |

### 运营实战工具
| 文件 | 用途 |
|---|---|
| `noon_nis_generator.py` | NIS 上传文件生成器 — 基于 NIS.xlsx 模板生成可直接上传 Noon Seller Lab 的文件 |
| `noon_batch_pricer.py` | 批量定价反算器 — 二分搜索目标利润率对应售价 + 三情景模拟 |
| `noon_weekly_report.py` | 周报自动生成器 — 核心指标/品类机会/风险告警 |
| `noon_competition_monitor.py` | 竞争监控告警器 — 新竞争者/价格战/需求变化/机会检测 |

### 爬虫系统 (`noon-selection-tool/`)
| 文件 | 用途 |
|---|---|
| `scrapers/noon_scraper.py` | Noon 搜索结果爬虫 |
| `scrapers/amazon_scraper.py` | Amazon.sa 搜索爬虫 |
| `scrapers/noon_category_crawler.py` | Noon 类目遍历爬虫 |
| `scrapers/base_scraper.py` | 爬虫基类（Playwright + stealth + 断点续跑） |
| `run_full_pipeline_v2.py` | V2 全流程管道（关键词→爬取→分析→报告） |
| `main.py` | CLI 入口（支持 `--step category --category "automotive"`） |

### 知识库
| 文件 | 用途 |
|---|---|
| `Noon平台各模式 佣金&配送费规则.md` | **权威费率数据源**（佣金、配送费、补贴、退货规则） |
| `Noon-运营定价表底层设计逻辑.md` | 详细公式拆解 |
| `CRAWLER_UPGRADE_SPEC.md` | 爬虫系统升级技术规格 |

## 常用命令

```bash
# 定价工具
open noon-pricing-v6.html                # 浏览器端计算器
pip install openpyxl && python generate_pricing_v6.py  # 生成定价表

# 数据分析管道（第一轮）
python noon_data_pipeline.py --input report_automotive.xlsx --output report_enhanced.xlsx
python noon_selection_engine.py --input report_enhanced.xlsx --output selection_report.xlsx
python noon_category_analyzer.py --report report_automotive.xlsx --category "Dash Cameras" --output analysis.md

# 数据追踪与融合（第二轮）
python noon_price_analyzer.py --baseline report_automotive.xlsx --tracking tracking_dir/ --output price_analysis.xlsx
python noon_seller_analyzer.py --input seller_data.xlsx --output seller_report.xlsx
python noon_data_fusion.py --report report_automotive.xlsx --enhanced report_enhanced.xlsx --output unified.xlsx

# 运营实战工具（第二轮）
python noon_nis_generator.py --input products.csv --nis-template NIS.xlsx --output upload_ready.xlsx
python noon_batch_pricer.py --input products.csv --target-margin 0.20 --mode FBN --output pricing_plan.xlsx
python noon_weekly_report.py --data unified.xlsx --output weekly_report.md
python noon_competition_monitor.py --current report_new.xlsx --baseline report_old.xlsx --output alerts.md

# 爬虫（在 noon-selection-tool/ 目录下）
python main.py --step category --category automotive     # 类目爬取
python main.py --step scrape                             # 关键词爬取
python run_full_pipeline_v2.py                           # V2 全流程
```

## Architecture

### 定价引擎核心逻辑
- 体积重 = `L×W×H / 5000`
- FBN 计费重 = `MIN(MAX(实重, 体积重) + 包装附加, 尺寸档上限)`
- FBP 计费重 = `MAX(实重, 体积重)`
- 佣金三模式：Fixed（固定费率）、Threshold（门槛跳变）、Sliding（超额累进）
- FBN 出库费：尺寸分档 → 计费重查表 → 售价 ≤25 取低档 / >25 取高档
- FBP 补贴 Reimbursement：作为**正向收入**（匹配 Noon Statement 对账逻辑）
- 退货费 = `MIN(15, 佣金×20%)`

### 四种模式计算差异

| 模式 | 头程 | 佣金 | 配送费 | 补贴 | 特殊 |
|---|---|---|---|---|---|
| FBN | 海运 CBM | FBN 费率 | FBN 出库费（尺寸档+重量） | 无 | — |
| 海外仓 FBP | 海运 CBM | FBP 费率 | FBP 配送费（重量） | 有 Reimbursement | — |
| 直邮 FBP | 按公斤快递 | FBP 费率 | FBP 配送费（重量） | 有 Reimbursement | `CEILING(cw, 0.1)` 进位 |
| Global | 海运 CBM（UAE） | FBN 费率 | FBN 出库费（UAE） | 无 | + 跨境费 5% + 关税 + 清关费 + 进口 VAT |

### 市场参数

| 市场 | 货币 | VAT | 默认汇率 (→CNY) |
|---|---|---|---|
| UAE | AED | 5% | 1.96 |
| KSA | SAR | 15% | 1.92 |

## Authoritative Data Sources

- **`Noon平台各模式 佣金&配送费规则.md`** — 所有费率的硬核标准，即使后续迭代到 V7 也以此文件为准
- `Noon-运营定价表底层设计逻辑.md` — 详细公式拆解
- `global模式：gcc关税等税务知识` — GCC 关税/认证/VAT 参考

## Validation Baseline

修改计算逻辑后须验证基准数据：
- **UAE** Bags:Luggage — FBN 利润率 13.46%, FBP 利润率 15.06%
- **KSA** Apparel — FBN 利润率 -8.79%, FBP 利润率 -25.78%

## Development Conventions

- 中文注释，英文变量名
- 所有金额数值保留 2 位小数
- Python 依赖: `pandas`, `openpyxl`
- HTML 工具为单文件架构，不拆分文件
- 中文界面，关键术语保留英文（FBN、FBP、Commission、VAT、Reimbursement 等）
- 费率数据以 `Noon平台各模式 佣金&配送费规则.md` 为权威来源
- Python 生成器禁用 `LET`/`AGGREGATE` 等 Excel 365 专属函数（用户使用 WPS）

## Execution Mode

- 默认直接执行，不先复述方案，不等待确认
- 默认流程：先改代码 -> 运行验证 -> 修复问题 -> 最后汇报
- 不要只给建议；如果可以直接落地，就直接修改并验证
- 对局部不明确的问题，自行做最保守、最小范围、可回退的假设并继续
- 完成后再统一汇报，简洁说明：改了什么、为什么、验证结果、剩余风险
- 仅在以下情况暂停并向用户确认：destructive 操作、会覆盖用户未提交的大改动、需要密钥/登录/付费资源、生产部署、数据库 Schema 变更、或存在多个代价明显不同的实现路径
- 若发现工作区有他人或用户正在进行中的改动，优先避让并最小化冲突；除非用户明确要求，否则不要回滚非自己修改的内容

## ERP Web UI Rules

- 本仓库的 Web 端默认按“数据 ERP / 研究工作台”设计，不按营销页、展示页、博客页思路设计
- 优先延续现有视觉语言：参考 `noon-selection-tool/web_beta/static/index.html`、`noon-selection-tool/web_beta/static/app.css`、`noon_dashboard.html`
- 当前推荐视觉方向：暖白工作区 + 深色导航 + 单一高识别强调色 + 高信息密度 + 粘性筛选栏 + 表格/抽屉/对比托盘
- 字体优先延续现有组合：`Noto Sans SC`、`IBM Plex Sans`、`Space Grotesk` 或同等气质替代，不要退化为默认系统字堆
- 中文界面继续保留，关键业务词保留英文术语；后台页面可继续采用中英双标签
- 设计新页面前，先明确 4 件事：`visual thesis`、`screen type`、`primary action`、`default density`
- 页面骨架优先级：导航壳层 > 全局命令区 > 核心数据区 > 次级详情区；不要先堆组件再拼页面
- 表格是核心产品界面，不是附属组件；默认支持 sticky header、行 hover、状态 badge、密度切换、筛选摘要、导出或对比动作
- 详情优先用右侧 drawer、split pane、compare tray 或上下文面板承载，避免频繁整页跳转和 modal 套 modal
- 图表只在能帮助判断趋势、分布、结构、异常时使用；不能替代决策时，优先表格或摘要块
- KPI 区块必须紧凑、可对比、可扫描，不能做成大面积装饰卡片墙
- 所有后台页面都要回答 4 个问题：`我现在在看什么`、`哪里异常`、`下一步动作是什么`、`还能往哪一层 drill down`
- 允许有层次感背景、局部渐变、强调色高亮，但禁止通用 SaaS 紫色渐变、纯白卡片海、超大圆角漂浮卡片、过度留白
- 禁止把 ERP 页面做成“看起来高级但几乎不能高效工作”的低密度设计
- 移动端允许折叠，但不能把关键筛选、分页、状态和主操作全部藏起来；桌面端仍是第一优先级
- 当任务属于 ERP / dashboard / admin / workbench UI 时，若环境中可用，优先同时使用 `$frontend-skill` 与 `$data-erp-ui`

## Mandatory UI Workflow

- 当任务属于 ERP / analytics / dashboard / admin / 研究工作台 UI 时，默认工作流固定为：`$erp-page-spec` -> `$data-erp-ui` + `$frontend-skill` -> 截图验证 -> `$erp-ui-critic`
- 先输出 `Page Spec`，再进入页面实现；不要跳过规格直接堆页面
- `Page Spec` 至少覆盖：页面目标、用户角色、主任务流、一级导航、二级模块、指标层、表格层、详情层、drill-down 路径、异常/空态/刷新逻辑、默认筛选/排序/密度
- 进入实现前，优先对照 `$seller-analytics-benchmark`，确认页面属于哪种工作台类型，并明确借鉴点与禁止点
- 实现完成后，必须至少生成 desktop、tablet、mobile 三组截图之一的可视验证；优先使用 `playwright`、`screenshot`、`chrome-operator`
- 页面未经过 `$erp-ui-critic` 评审，不要直接认定为完成；评审重点按可扫性、信息密度、状态设计、任务闭环、层级清晰度、运营判断支持度执行
- 若页面评审不通过，先修 workflow / hierarchy / density 问题，再修装饰与微交互
- 成熟页面若需要沉淀到设计资产，优先走 `figma-generate-design` / `figma-implement-design` / `figma-create-design-system-rules`

## Seller Analytics Product Logic

- 默认把页面归入以下类型之一：研究首页、类目研究页、商品机会页、关键词研究页、运行监控台、配置/定价表单页
- 研究首页是 command center，不是品牌 hero；它负责恢复上下文、展示异常、进入研究路径
- 类目研究页必须能形成稳定路径：类目树 -> 榜单 -> 答案页 -> 商品池 -> 商品详情
- 商品机会页必须以筛选与表格扫描为主，不允许被大面积摘要块取代
- 关键词研究页必须围绕“关键词答案 -> 情报层 -> 命中商品”组织，而不是平铺模块
- 运行中心必须先暴露 freshness、failure、queue、worker，再谈二级统计
- 配置/定价页必须让输入分组、结果预览、影响范围、验证状态紧密相邻
