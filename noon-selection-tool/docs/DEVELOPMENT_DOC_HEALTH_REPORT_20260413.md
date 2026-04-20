# 开发文档体检报告

Last Updated: 2026-04-13  
Status: active assessment

## 目的

这份文档用于回答 3 个问题：

1. 当前开发文档体系到底在约束什么
2. 哪些约束是正收益，能提高稳定性和效率
3. 哪些约束开始变成负担，后续应该怎么优化

结论先行：

当前文档体系整体是正资产，不是负担。  
但它已经从“缺约束”进入“约束开始变多”的阶段。下一步最重要的不是继续加文档，而是让文档分层更清楚、入口更轻。

## 当前文档体系

当前主要文档可以分成 6 层：

1. [AGENTS.md](D:/claude%20noon%20v1/AGENTS.md)
   - 全局执行方式、UI 规则、默认开发行为
2. [PROJECT_WHITEPAPER.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PROJECT_WHITEPAPER.md)
   - 项目战略、阶段目标、架构总纲
3. [DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md)
   - 当前 owner、边界、优先级、执行规则
4. [DEV_COLLAB_LOG.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_COLLAB_LOG.md)
   - 时间线事实日志
5. [CODEX_COLLAB_GUIDE.md](D:/claude%20noon%20v1/noon-selection-tool/docs/CODEX_COLLAB_GUIDE.md)
   - 用户如何给任务，Codex 如何按整块闭环推进
6. 入口与同步文档
   - [README.md](D:/claude%20noon%20v1/noon-selection-tool/docs/README.md)
   - [OBSIDIAN_SYNC.md](D:/claude%20noon%20v1/noon-selection-tool/docs/OBSIDIAN_SYNC.md)

## 评分

按“对实际开发的帮助”评分：

| 文档 | 作用 | 评分 | 判断 |
|---|---|---:|---|
| [PROJECT_WHITEPAPER.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PROJECT_WHITEPAPER.md) | 战略、阶段、架构 | 9/10 | 强正收益 |
| [DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md) | 执行边界、owner、当前规则 | 9/10 | 强正收益 |
| [CODEX_COLLAB_GUIDE.md](D:/claude%20noon%20v1/noon-selection-tool/docs/CODEX_COLLAB_GUIDE.md) | 任务 intake 与闭环执行 | 9/10 | 强正收益 |
| [OBSIDIAN_SYNC.md](D:/claude%20noon%20v1/noon-selection-tool/docs/OBSIDIAN_SYNC.md) | 防止双重真相源 | 8/10 | 明显正收益 |
| [README.md](D:/claude%20noon%20v1/noon-selection-tool/docs/README.md) | 文档入口 | 7.5/10 | 正收益，但需优化 |
| [AGENTS.md](D:/claude%20noon%20v1/AGENTS.md) | 全局行为规则 | 7/10 | 正收益，但过宽 |
| [DEV_COLLAB_LOG.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_COLLAB_LOG.md) | 时间线审计 | 6.5/10 | 有价值，但不适合高频入口 |

整体评分：

- `稳定性价值`: 9/10
- `开发效率价值`: 8/10
- `认知负担`: 6/10
- `负面约束风险`: 中等

## 明确的正面约束

### 1. Whitepaper 是有效的战略约束

[PROJECT_WHITEPAPER.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PROJECT_WHITEPAPER.md) 现在能有效回答：

- 当前项目到底处在哪个阶段
- 当前优先级是 runtime hardening 还是新功能扩张
- 本地 beta、NAS stable、shared sync 各自是什么角色

这能明显减少“做着做着偏题”。

### 2. Handoff 是有效的执行约束

[DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md) 现在能有效回答：

- 谁能改什么
- 当前哪些文件/模块是 main-path
- 哪些动作需要停下来确认
- 当前默认执行模式是什么

这能减少窗口之间相互踩文件、踩运行资源。

### 3. Collaboration Guide 直接提升效率

[CODEX_COLLAB_GUIDE.md](D:/claude%20noon%20v1/noon-selection-tool/docs/CODEX_COLLAB_GUIDE.md) 是当前最直接提升效率的文档。

它把协作模型从：

`提一个点 -> 修一个点 -> 停下来等继续`

改成：

`给清楚目标 -> Codex 整块闭环推进 -> 到可验收再回来`

这条规则是高价值约束，应该长期保留。

### 4. Obsidian Sync 规则是健康的

[OBSIDIAN_SYNC.md](D:/claude%20noon%20v1/noon-selection-tool/docs/OBSIDIAN_SYNC.md) 明确了：

- repo docs 才是 authority
- Obsidian 只做镜像和知识整理

这避免了“仓库一套、Obsidian 一套”的双重真相源问题。

## 已经开始出现的负面约束

### 1. AGENTS.md 过宽

[AGENTS.md](D:/claude%20noon%20v1/AGENTS.md) 的问题不是方向错，而是内容层级太多：

- 既有当前核心项目规则
- 又有根目录历史工具背景
- 又有 UI 设计方法
- 又有执行模式

它现在更像：

`宪法 + 历史背景 + 风格规范 + 行为守则`

混在一个文件里。

结果是：

- 高价值规则仍然有用
- 但读取成本开始变高
- 新任务进入时，真正必须看的内容和历史背景混在一起

这属于“轻度负面约束”。

### 2. Collab Log 太长，不适合当入口

[DEV_COLLAB_LOG.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_COLLAB_LOG.md) 现在已经更像审计日志，而不是日常导航。

它的价值在于：

- 查历史事实
- 查某次改动何时发生
- 查验证和 effect boundary

但它不适合：

- 每次开工都当第一入口
- 新窗口快速建立上下文

如果把它当成高频入口，它就会变成纯负担。

### 3. Whitepaper 有阶段绑定过强的风险

[PROJECT_WHITEPAPER.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PROJECT_WHITEPAPER.md) 现在和当前阶段高度匹配，所以非常有用。

但如果未来项目进入：

- ERP 中台建设
- 采购 / 库存 / 补货模块
- 飞书 / 企业微信协同

而 Whitepaper 还长期停留在当前 Phase 0/1 的口径，它就会开始变成“历史阶段约束”，而不是“当前战略约束”。

### 4. 文档入口开始变重

当前问题不是文档不够，而是：

- Whitepaper
- Handoff
- Collab Log
- Guide
- README
- Obsidian Sync

都存在，而且彼此有引用关系。

这说明治理变成熟了，但也说明：

“新任务真正要先看哪 2-3 份”必须明确，否则光建立上下文就会慢。

## 当前最推荐的阅读顺序

### 日常开发开工

1. [AGENTS.md](D:/claude%20noon%20v1/AGENTS.md)
   - 只看全局行为规则
2. [PROJECT_WHITEPAPER.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PROJECT_WHITEPAPER.md)
   - 看当前阶段和架构总纲
3. [DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md)
   - 看当前 owner、边界、优先级
4. [CODEX_COLLAB_GUIDE.md](D:/claude%20noon%20v1/noon-selection-tool/docs/CODEX_COLLAB_GUIDE.md)
   - 看这次任务该怎么被定义和推进

### 按需读取

- 需要查代码主路径时：
  - [CODEBASE_MAP.md](D:/claude%20noon%20v1/noon-selection-tool/docs/CODEBASE_MAP.md)
- 需要查历史事实时：
  - [DEV_COLLAB_LOG.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_COLLAB_LOG.md)
- 需要做发布或同步知识库时：
  - [README.md](D:/claude%20noon%20v1/noon-selection-tool/docs/README.md)
  - [OBSIDIAN_SYNC.md](D:/claude%20noon%20v1/noon-selection-tool/docs/OBSIDIAN_SYNC.md)

## 下一轮优化建议

### 建议保留

- Whitepaper 作为战略总纲
- Handoff 作为当前执行边界
- Collaboration Guide 作为协作协议
- Obsidian Sync 作为真相源保护规则

### 建议优化

1. [README.md](D:/claude%20noon%20v1/noon-selection-tool/docs/README.md)
   - 从“文档列表”优化为“阅读顺序 + 任务入口”

2. [DEV_COLLAB_LOG.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_COLLAB_LOG.md)
   - 明确降级为审计日志
   - 不再作为默认第一入口

3. [AGENTS.md](D:/claude%20noon%20v1/AGENTS.md)
   - 后续可考虑拆成：
     - 核心执行规则
     - 历史工具背景附录

4. [PROJECT_WHITEPAPER.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PROJECT_WHITEPAPER.md)
   - 保持战略性
   - 避免沉淀太多只对短期阶段有效的细节

## 一句话结论

当前开发文档体系总体是：

`治理成熟、方向正确、执行清晰，但入口开始偏重`

下一步最重要的不是继续加规则，而是：

`保留高价值约束，把高频入口变轻，把日志型文档降级为按需读取`
