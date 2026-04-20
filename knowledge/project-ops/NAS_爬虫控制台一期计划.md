# 待执行任务：NAS 爬虫控制台一期

## 摘要

请主窗口在现有 ERP Web 基础上，落地一个新的 `Crawler Control / 爬虫控制台` 工作区，统一承载 `类目爬虫` 和 `关键词爬虫` 的可视化控制、计划调度、运行监控和结果可见性说明。

这不是新系统重做。必须复用现有：
- `tasks / task_runs / workers`
- scheduler / worker
- shared warehouse sync contract
- NAS stable Web 壳层和 admin 权限体系

一期目标是让运营者在 NAS 上自行完成：
- 配置类目扫描和关键词扫描
- 设置扫描范围和深度
- 设置立即执行、指定时间执行、按天/按周周期执行
- 查看运行进度、失败原因、worker 状态
- 明确知道 `runtime -> stage -> warehouse -> Web` 每一步是否完成

## 必做实现

### 1. 新增高层工作区

新增一个顶级工作区：
- 名称：`Crawler Control / 爬虫控制台`
- 仅 `admin` 可见
- 不塞回 `Runs & Health`
- `Runs & Health` 继续保留底层监控职责

工作区固定分成 5 块：
- `控制总览`
- `快速启动`
- `计划管理`
- `运行监控`
- `历史结果`

### 2. 新增计划层，不重造执行层

在现有 `ops.db` 上新增 `crawl_plans` 表，作为高层调度定义层。

字段固定：
- `id`
- `plan_type`
- `name`
- `enabled`
- `created_by`
- `schedule_kind`
- `schedule_json`
- `payload_json`
- `last_dispatched_at`
- `next_run_at`
- `last_run_status`
- `last_run_task_id`
- `created_at`
- `updated_at`

现有 `tasks / task_runs / workers` 保持执行层职责，不替代为 plan 表。

对 `tasks` 允许做最小增强：
- `plan_id`
- `display_name`
- `progress_json`

### 3. 类目控制台能力

类目控制必须同时支持：
- `批量类目扫描`
- `单类目/补扫`

类目 UI 字段固定：
- `范围模式`
  - `Ready Categories`
  - `Selected Categories`
  - `Single Category`
- `类目选择`
  - 多选，不允许只靠逗号手输
- `默认商品深度`
  - 语义固定为：`每个子类目抓取商品上限`
- `类目覆盖深度`
  - 支持 `pets=500`、`sports=500` 这类覆盖
- `持久化到 stage`
- `导出 Excel`
- `启动方式`
  - `立即执行`
  - `指定时间一次`
  - `每 N 小时`
  - `每周定时`

类目计划 payload 固定支持：

```json
{
  "plan_type": "category_scan",
  "scope_mode": "ready_categories",
  "categories": [],
  "default_product_count_per_leaf": 300,
  "category_overrides": {
    "pets": { "product_count_per_leaf": 500 },
    "sports": { "product_count_per_leaf": 500 }
  },
  "persist": true,
  "export_excel": false,
  "warehouse_sync_policy": "after_batch"
}
```

行为固定：
- `categories=[]` 表示取 readiness report 当前 `ready_for_scan`
- 类目批扫必须作为一个 batch task 运行
- 只允许 `after_batch` 触发 warehouse sync
- 不做“每个子类目完成就 sync”

### 4. 关键词控制台能力

关键词控制必须同时支持：
- `Keyword Batch`
- `Monitor Config`

关键词 UI 字段固定：
- `模式`
  - `Keyword Batch`
  - `Monitor Config`
- `关键词输入`
  - 多行输入
  - 自动去重
- `监控配置`
  - 下拉读取可用 `keyword_monitor*.json`
- `平台`
  - `noon`
  - `amazon`
  - `noon + amazon`
- `Noon Count`
- `Amazon Count`
- `持久化到 stage`
- `启动方式`
  - `立即执行`
  - `指定时间一次`
  - `每 N 小时`
  - `每周定时`

关键词计划 payload 固定支持两种：

```json
{
  "plan_type": "keyword_scan",
  "mode": "keyword_batch",
  "keywords": ["dog toys", "cat litter"],
  "platforms": ["noon", "amazon"],
  "noon_count": 30,
  "amazon_count": 30,
  "persist": true
}
```

```json
{
  "plan_type": "keyword_scan",
  "mode": "monitor_config",
  "monitor_config": "config/keyword_monitor_pet_sports_7h.json",
  "noon_count": 30,
  "amazon_count": 30,
  "persist": true
}
```

一期必须补一个正式的 `keyword_batch` 执行路径，不能只让用户继续依赖 repo 里的静态 monitor config。

### 5. 调度模型统一

一期调度只允许 4 种：
- `manual`
- `once`
- `interval`
- `weekly`

不开放 raw cron。

`schedule_json` 固定：
- `manual`: `{}`
- `once`: `{ "start_at": "<ISO>", "timezone": "Asia/Shanghai" }`
- `interval`: `{ "start_at": "<ISO>", "interval_hours": 6, "timezone": "Asia/Shanghai" }`
- `weekly`: `{ "weekdays": ["mon","wed","fri"], "time": "23:00", "timezone": "Asia/Shanghai" }`

当前前端里未真正接通的 `once/cron` 占位逻辑，必须统一改成这套正式模型。
当前后端仅支持 `manual/interval` 的 schedule_type，需要扩展到上述 4 种。

### 6. 运行监控与进度上报

任务详情必须展示结构化进度。

类目任务 progress 固定包含：
- 当前类目
- 当前子类目
- 已完成类目数 / 总类目数
- 已完成子类目数 / 当前类目总子类目数
- 当前抓取原始数
- 当前去重数
- stage 写入状态
- warehouse sync 状态

关键词任务 progress 固定包含：
- 当前关键词
- 已完成关键词数 / 总关键词数
- 失败关键词数
- 当前平台阶段
- stage 写入状态
- warehouse sync 状态

必须明确展示 4 个可见性阶段：
- `runtime collecting`
- `stage persisted`
- `warehouse syncing`
- `web visible`

UI 必须直接告诉用户：
- 类目批扫是“按类目写 stage”
- Web 可见性以后续 `warehouse sync completed` 为准

### 7. API 层

保留现有低层 API：
- `/api/tasks`
- `/api/task-runs`
- `/api/workers`

新增高层 API：
- `GET /api/crawler/catalog`
- `GET /api/crawler/plans`
- `POST /api/crawler/plans`
- `PATCH /api/crawler/plans/{id}`
- `POST /api/crawler/plans/{id}/launch`
- `POST /api/crawler/plans/{id}/pause`
- `POST /api/crawler/plans/{id}/resume`
- `GET /api/crawler/runs`
- `GET /api/crawler/runs/{task_id}`

`/api/crawler/catalog` 必须返回：
- readiness categories
- runtime category map
- available monitor configs
- default recommended depths

## 关键修正点

主窗口执行时，必须一并修掉这些当前限制：

- 当前 task center 前端虽然有 schedule 输入，但提交时仍固定走 `manual`
- 当前 `task_type=category_single` 的 `product_count` 没有真正下沉到 worker 命令语义，必须修通
- 当前类目批扫只支持单一 `product_count`，必须支持 `default + category overrides`
- 当前控制面板还是“任务中心”，不是“爬虫控制台”，必须提升为高层运营界面

## 测试与验收

必须覆盖：

- `crawl_plans` CRUD
- `once / interval / weekly` 派发逻辑
- 类目计划：
  - ready categories
  - selected categories
  - single category
  - default depth + overrides
- 关键词计划：
  - keyword_batch
  - monitor_config
- 权限：
  - `operator` 不可访问控制台
  - `admin` 可完整操作
- progress_json：
  - category 进度更新
  - keyword 进度更新
- 可见性状态：
  - stage 已写但 warehouse 未完成
  - warehouse 完成后 Web 可见
- NAS 联调：
  - 一个小规模 category plan
  - 一个小规模 keyword batch plan
  - 验证 worker 接单、进度更新、完成、历史记录可查

## 默认假设

- 部署目标是 `NAS stable`
- 继续复用现有执行底座，不重写任务系统
- 控制台放在现有 ERP Web 内，不另起独立站点
- 一期不做每子类目级别的 warehouse sync
- 一期不开放 raw cron
- 一期关键词控制台支持 `Keyword Batch + Monitor Config`
- 一期类目深度的唯一业务语义是：`每个子类目抓取商品上限`

## 可直接下发给主窗口的指令

请主窗口按以下要求执行：

1. 在现有 ERP Web 中新增一个仅 `admin` 可见的 `Crawler Control / 爬虫控制台` 工作区，不要继续把这块能力塞在 `Runs & Health` 里。
2. 不重造执行系统，必须复用现有 `tasks / task_runs / workers / scheduler / worker / shared warehouse sync`。
3. 在 `ops.db` 上新增 `crawl_plans` 作为高层计划层，并在 `tasks` 上做最小增强：`plan_id / display_name / progress_json`。
4. 一期调度只支持 `manual / once / interval / weekly`，不要开放 raw cron；把当前前端未接通的 `once/cron` 占位逻辑改成正式模型。
5. 类目控制必须支持：
   - ready categories
   - selected categories
   - single category
   - 默认深度 + 类目覆盖深度
   - 立即执行 / 指定时间 / 周期执行
6. 关键词控制必须支持：
   - keyword batch
   - monitor config
   - 平台选择
   - 立即执行 / 指定时间 / 周期执行
7. 必须新增结构化进度上报和可见性状态展示，明确区分：
   - runtime collecting
   - stage persisted
   - warehouse syncing
   - web visible
8. 必须补齐当前两个真实缺口：
   - `category_single` 的 `product_count` 真正下沉执行
   - `category batch` 支持 `default depth + category overrides`
9. 必须补一组完整测试：
   - plan CRUD
   - once/interval/weekly 调度
   - category/keyword 执行
   - admin 权限
   - progress
   - NAS 联调
10. 一期目标是做成 NAS 上可长期运营的爬虫控制台，不是一次性任务表单增强，也不是另起独立后台。
