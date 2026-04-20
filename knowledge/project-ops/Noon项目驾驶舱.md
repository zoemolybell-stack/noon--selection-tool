# Noon 项目驾驶舱

> 项目代码根目录：`D:/claude noon v1/noon-selection-tool`
>
> 项目文档根目录：`/Users/a16/Documents/Obsidian Vault/noon`

## 当前阶段

- `single control plane + remote category node rollout`
- 当前不是扩散新功能，而是先把：
  - remote category node rollout
  - keyword/runtime no-regression
  - release 与 docs 对齐
  做稳

## 当前 blocker

- 远端类目节点的控制面、Postgres、artifact sync 已经打通
- 但类目抓取本身仍被 Noon 上游 `Access Denied` 卡住
- 当前还不能把“remote category steady-state”写成 fully ready

## 当前正在推进的任务

- remote category node rollout completion
- category anti-bot / Access Denied mitigation
- category crawler gradual layering
- keyword protection and runtime observability no-regression
- doc / knowledge-base alignment

## 下一任务

1. 用受控样本继续复现 remote category `Access Denied`
2. 先把“控制面已打通”和“抓取已可正式接管”这两个结论彻底拆开
3. 在 non-zero observations 与 acceptance gate 过关前，继续保留 fallback 思路
4. 保持 `latest green pointers`、近期节点、任务清单和调度台同步

## 当前最值得先看的文档

1. [Noon_正式任务清单](./Noon_正式任务清单.md)
2. [Noon_开发调度台](./Noon_开发调度台.md)
3. [00-Noon系统总览](../系统架构知识库/00-Noon系统总览.md)
4. [90-当前开发上下文](../系统架构知识库/90-当前开发上下文.md)
5. [91-近期开发节点](../系统架构知识库/91-近期开发节点.md)
6. [07-测试、校验与运行入口](../系统架构知识库/07-测试、校验与运行入口.md)

## 当前工作模型

- repo 文档里的 `latest green pointers` 是绿色基线真相
- Obsidian 用于：
  - 恢复上下文
  - 维护推进状态
  - 沉淀方法论
- 任务推进默认按：
  - `任务清单 -> 调度台 -> 当前上下文 / 近期节点`

## 推荐打开顺序

1. 先打开 [Noon项目驾驶舱](./Noon项目驾驶舱.md)
2. 再看 [Noon_正式任务清单](./Noon_正式任务清单.md)
3. 再看 [Noon_开发调度台](./Noon_开发调度台.md)
4. 需要恢复最新现场时，看 [90-当前开发上下文](../系统架构知识库/90-当前开发上下文.md)
5. 需要看最近变化时，看 [91-近期开发节点](../系统架构知识库/91-近期开发节点.md)
