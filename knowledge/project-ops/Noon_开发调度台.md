# Noon 开发调度台

更新时间：`2026-04-14`

## 角色

- 指挥者：`leader`
- 作用：
  - 做任务拆解
  - 给 worker 划定 bounded slice
  - 做最终集成、验证、release judgment

## 默认执行模式

- one `leader` owns：
  - decomposition
  - integration
  - release judgment
  - final verification
- worker agents：
  - 只接收 bounded slice
  - 只在声明过写入范围后执行
  - 不同时改主路径文件、release docs、NAS runtime 关键路径

## 当前批次

- 批次编号：`2026-04-14-remote-category-rollout-stabilization`
- 目标：
  - 判断 remote category rollout 是否已经进入可正式接管的边界
  - 在不牺牲 keyword/runtime-center 绿色状态的前提下继续推进 category rollout
  - 把文档层的“当前状态 / 当前批次 / 最近节点”同步起来

## 当前状态

- `infra / compose / postgres cutover`：已完成
- `artifact sync`：已完成
- `worker heartbeat visibility`：已完成
- `remote category crawl acceptance`：未完成
- `upstream anti-bot mitigation`：未完成
- `doc alignment`：进行中

## 当前批次的关键判断

- “控制面接通”不等于“类目抓取已可稳定接管”
- 在非空 observations 与 acceptance gate 没过前：
  - rollout 只能写成 `in_progress` 或 `blocked`
  - 不能直接写成稳定结论

## 推荐 lane

- `runtime / ops`
  - 关注 compose、worker、artifact、health、release wiring
- `crawler / anti-bot`
  - 关注 remote category sample、Access Denied、acceptance
- `verification / report`
  - 关注 green pointers、近期节点、验证结论和文档同步

## 调度硬规则

- 任务队列真相源：
  - [Noon_正式任务清单](./Noon_正式任务清单.md)
- 当前批次真相源：
  - 本调度台
- 当前状态恢复入口：
  - [90-当前开发上下文](../系统架构知识库/90-当前开发上下文.md)
  - [91-近期开发节点](../系统架构知识库/91-近期开发节点.md)
- 绿色基线真相：
  - repo 文档里的 `latest green pointers`

## 默认停止条件

- `goal complete`
- true decision gate exists
- destructive or release action needs approval
- credentials / external access / paid resources are missing

## 不应提前停下的情况

- 普通实现细节
- 文案与轻量结构调整
- 可以通过现有 repo truth 与当前上下文自行判断的问题

## 当前批次完成定义

- remote category rollout 的 acceptance 边界被说清楚
- keyword/runtime-center 保持 no-regression
- 当前阻塞项、绿色基线、近期节点和任务清单之间没有浅层矛盾

## 完成后要更新的文档

- [Noon项目驾驶舱](./Noon项目驾驶舱.md)
- [Noon_正式任务清单](./Noon_正式任务清单.md)
- [90-当前开发上下文](../系统架构知识库/90-当前开发上下文.md)
- [91-近期开发节点](../系统架构知识库/91-近期开发节点.md)
