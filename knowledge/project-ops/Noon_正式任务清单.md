# Noon 正式任务清单

更新时间：`2026-04-14`

## North Star

- `multi-source crawling -> unified product intelligence -> Web operations workbench -> scoring/classification -> listing preparation -> AI enrichment -> controlled automation`

## 执行原则

- repo 主文档与 `latest green pointers` 是实现与绿色基线真相
- Obsidian 负责上下文恢复、推进管理与方法论沉淀
- 默认停止条件是 `goal complete`，不是“一个技术步骤完成”
- destructive / release / paid resource / credential / 路线明显分叉，才是确认闸门
- 未通过 acceptance gate 的结论，不写成 fully ready

---

## 阶段 A：控制面与绿色基线稳定

当前状态：`ready`

已完成：

- single control plane 运行语义已明确
- `latest green pointers` 已成为稳定基线入口
- keyword/runtime-center 主路径已形成固定检查方式

验收标准：

- 当前绿色基线可被稳定复用
- runtime-center、health、workers 等关键入口有明确真相源

---

## 阶段 B：remote category node rollout

当前状态：`in_progress`

已完成：

- remote category worker heartbeat 已可见
- NAS compose 已切到正确 Postgres DSN
- `runtime_category_map.json` / `runtime_category_map.meta.json` 回推 NAS 共享目录链路已打通

当前 blocker：

- 远端类目抓取仍会命中上游 `Access Denied`
- 当前不能把“控制面接通”误写成“类目抓取可正式接管”

验收标准：

- 受控类目样本能够稳定跑出非空 observations 或明确可接受的失败语义
- remote category rollout 不导致 keyword/runtime-center 回归
- 是否移除 fallback 有明确 acceptance 结论

---

## 阶段 C：category anti-bot 与 acceptance 收口

当前状态：`in_progress`

目标：

- 收口 remote category 节点的上游阻塞与接受条件
- 形成“什么时候可升 stable、什么时候必须继续 fallback”的明确规则

当前重点：

- `Access Denied` 复现与归因
- 受控样本验证
- rollout acceptance gate

验收标准：

- 失败原因可解释
- rollout 结果可归类为：
  - `ready`
  - `partial`
  - `blocked`
  之一，而不是模糊状态

---

## 阶段 D：文档、知识库与发布对齐

当前状态：`in_progress`

目标：

- 保持 repo 主文档、Obsidian、绿色基线、近期节点、发布判断一致

已完成：

- `系统架构知识库`
- `可复用方法论`
- `90/91/99` 上下文层

本轮新增：

- `项目推进与调度` 层：
  - 驾驶舱
  - 正式任务清单
  - 开发调度台

验收标准：

- 新会话能用 3~5 分钟恢复当前推进现场
- 近期变化、当前批次、绿色基线、阻塞项不互相矛盾

---

## 阶段 E：产品能力与工作台扩展

当前状态：`planned`

说明：

- 在 remote category rollout 与 acceptance gate 没有稳定前，不把注意力转移到过多新功能面
- 新能力推进仍以 runtime truth 和 release safety 为前置
