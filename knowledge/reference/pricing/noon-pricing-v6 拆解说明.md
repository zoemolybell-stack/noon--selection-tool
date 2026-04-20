# Noon V6 Pro HTML 拆解说明

## 文档目的
这份文档基于 [noon-pricing-v6.html](/Users/a16/Documents/Obsidian%20Vault/noon/noon-pricing-v6.html) 的实际代码整理，目的是把这份单文件 HTML 内部的页面结构、数据表、计算规则、5 个利润场景、历史 SKU、本地批量对比等逻辑拆解成一份可读的 Markdown 说明，便于后续维护、迁移到 Excel、继续改版或交接。

## 文件概况
- 文件类型：单文件 HTML
- 规模：约 1430 行
- 架构：`HTML + CSS + JS` 全部内嵌，无外部依赖
- 主要市场：`UAE`、`KSA`
- 主要利润场景：
  - `FBN海运`
  - `FBN空运`
  - `海外仓FBP海运`
  - `海外仓FBP空运`
  - `跨境FBP`

## 页面结构

### 1. 顶部 Header
- 标题：`Noon V6 Pro 定价计算器`
- 副标题：说明当前支持的 5 个利润场景

### 2. 国家切换 Tabs
- `UAE 阿联酋 (AED)`
- `KSA 沙特 (SAR)`

切换国家时会同步改变：
- 币种展示
- VAT
- 默认汇率
- 国家级计抛比
- 海外仓操作费默认值
- 可选类目列表

### 3. 输入区
输入区分为三层：

#### 核心输入
- `SKU 编号`
- `品名 / 备注`
- `类目 Category`
- `采购价 (CNY)`
- `售价 (AED / SAR)`
- `长 / 宽 / 高`
- `重量`
- `广告 (%)`
- `退货率 (%)`

#### 物流参数
- `海运基础单价 (CNY/m³)`
- `海运限重 (kg/m³)`
- `空运基础单价 (CNY/kg)`
- `空运最低计费重 (kg)`
- `直邮操作费 (CNY/单)`
- `直邮空运价 (CNY/kg)`
- `直邮最低计费重 (kg)`

#### 国家配置
- `汇率 (AED/SAR -> CNY)`
- `FBP 交货方式`
  - `Drop-off`
  - `Pick-up`
- `海运计抛比`
- `空运计抛比`
- `直邮计抛比`
- `海外仓操作费 ≤5kg`
- `海外仓操作费 超出/kg`
- `跨境限重`：固定显示 `2kg 封顶`

### 4. 结果区
结果区分为四块：

#### 费率透视
展示共享计算结果：
- FBN 尺寸层级
- FBN 计费重
- FBP 计费重
- VAT 税额
- 海运 CBM / 海运体积重 / 海运计费重 / 海运密度倍率
- 空运计费重
- 直邮计费重
- 跨境状态
- FBN 佣金
- FBP 佣金
- FBN 出库费
- FBP 配送费
- 海外仓操作费

#### 5 张利润摘要卡片
每张卡片展示：
- 净利（CNY）
- 利润率
- ROI
- 是否为当前最优模式

#### 明细卡片
默认折叠，可展开查看单个模式的费用拆解。

#### 批量对比
把当前 SKU 快照加入表格，用于和其他 SKU 横向比较。

### 5. 本地记忆库
使用浏览器 `localStorage` 存储历史 SKU：
- 保存当前 SKU
- 通过 `SKU / 品名 / 类目 / 国家` 搜索
- 调用历史记录回填输入区
- 删除单条
- 清空全部

---

## JS 数据表拆解

### 1. `CONFIG`
代码位置：HTML 内约 `L432`

两个国家的基础配置：

#### UAE
- `vat = 0.05`
- `exch = 1.96`
- `restock = 0`
- `currency = AED`
- `seaThrow = 6000`
- `airThrow = 6000`
- `crossThrow = 6000`
- `owHandleBase = 5`
- `owHandleExtra = 2`

#### KSA
- `vat = 0.15`
- `exch = 1.92`
- `restock = 0`
- `currency = SAR`
- `seaThrow = 6000`
- `airThrow = 6000`
- `crossThrow = 6000`
- `owHandleBase = 5`
- `owHandleExtra = 2`

### 2. `CROSS_CFG`
- `maxChargeableWeight = 2`

含义：
- 跨境 FBP 的直邮计费重超过 `2kg` 时，场景判定为不可用。

### 3. `CATEGORIES`
代码位置：约 `L438`

总类目数：`59`

每个类目结构如下：
```js
[
  类目名称,
  UAE_FBN_类型, UAE_FBN_基础费率, UAE_FBN_门槛, UAE_FBN_第二档费率,
  UAE_FBP_类型, UAE_FBP_基础费率, UAE_FBP_门槛, UAE_FBP_第二档费率,
  KSA_FBN_类型, KSA_FBN_基础费率, KSA_FBN_门槛, KSA_FBN_第二档费率,
  KSA_FBP_类型, KSA_FBP_基础费率, KSA_FBP_门槛, KSA_FBP_第二档费率
]
```

支持的佣金模式有三种：
- `Fixed`
- `Threshold`
- `Sliding`

不支持的场景标记为：
- `NA`

### 4. `FBN_SIZE_TIERS`
代码位置：约 `L501`

共有 `7` 档：
- `Small Envelope`
- `Standard Envelope`
- `Large Envelope`
- `Standard Parcel`
- `Oversize`
- `Extra Oversize`
- `Bulky`

每一档包含：
- 最大长宽高
- 包装附加重量 `pkg`
- 最大计费重 `maxCw`

### 5. `FBN_UAE_FEES`
代码位置：约 `L512`

UAE 的 FBN 出库费表，结构为：
```js
"尺寸档": [[maxWt, lowFee, highFee], ...]
```

其中：
- `maxWt`：该档位上限重量
- `lowFee`：售价 `<= 25` 时费用
- `highFee`：售价 `> 25` 时费用

### 6. `FBN_KSA_FEES`
代码位置：约 `L521`

KSA 的 FBN 出库费表，结构与 UAE 相同。

### 7. `FBP_FEES`
代码位置：约 `L532`

FBP 配送费表结构：
```js
[maxWt, uae_dropoff, uae_pickup, ksa_dropoff, ksa_pickup]
```

含义：
- 按 FBP 计费重分段
- 再按国家与交货方式选价格

---

## 核心函数拆解

### `getSizeTier(l, w, h)`
代码位置：约 `L557`

逻辑：
- 把长宽高从大到小排序
- 按排序后的三边去匹配 `FBN_SIZE_TIERS`
- 返回第一个满足条件的尺寸档
- 如果全部不满足，返回最后一档 `Bulky`

### `getCommission(catIdx, price, mode)`
代码位置：约 `L566`

支持四种查找入口：
- `uae_fbn`
- `uae_fbp`
- `ksa_fbn`
- `ksa_fbp`

佣金规则：

#### Fixed
```text
commission = price * base
```

#### Threshold
```text
price <= limit  => price * base
price >  limit  => price * tier
```

说明：
- 这是门槛跳变，不是超额累进。

#### Sliding
```text
commission = min(price, limit) * base + max(0, price - limit) * tier
```

#### NA
- 返回 `null`
- 代表当前市场/模式下该类目不支持

### `categorySupported(catIdx, ctry)`
代码位置：约 `L580`

逻辑：
- 只要该国家下 `FBN` 或 `FBP` 任意一侧不是 `NA`
- 该类目就会显示在类目搜索中

### `lookupFbnFee(tierName, cw, price, ctry)`
代码位置：约 `L586`

逻辑：
- 先根据国家选 `FBN_UAE_FEES` 或 `FBN_KSA_FEES`
- 再根据尺寸档找到对应表
- 逐行匹配 `cw <= maxWt`
- 若 `price <= 25`，取低售价档
- 否则取高售价档

### `lookupFbpFee(cw, ctry, mode)`
代码位置：约 `L596`

逻辑：
- 逐行查 `FBP_FEES`
- 根据国家和 `dropoff/pickup` 返回相应价格
- 如果超过最后一档，则直接取最后一档价格

### `getReimb(price, ctry)`
代码位置：约 `L608`

补贴规则：

#### UAE
```text
price < 100 => 10
else => 0
```

#### KSA
```text
price < 100 => 12
100 <= price <= 500 => 6
price > 500 => 0
```

### `getEditableSnapshot()`
代码位置：约 `L703`

作用：
- 把当前页面输入项打包成一个快照对象
- 用于历史 SKU 保存、回填和计算结果快照

### `applyCountryConfig(nextCountry, preferredLabel)`
代码位置：约 `L778`

作用：
- 切换国家
- 把该国家的默认汇率、计抛比、海外仓操作费写回输入框
- 同时刷新类目可选项

### `initMemory()` / `saveCurrentSku()` / `loadSavedSku()`
代码位置：
- `initMemory()`：约 `L927`
- `saveCurrentSku()`：约 `L968`
- `loadSavedSku()`：约 `L1023`

作用：
- 初始化本地存储能力
- 保存本地历史 SKU
- 按记录回填输入并重新计算

### `calculate()`
代码位置：约 `L1079`

作用：
- 这是整份 HTML 的主计算引擎
- 所有输入变化最终都汇总到这里计算

### `renderResults(result)`
代码位置：约 `L1275`

作用：
- 把主计算结果渲染到“费率透视、利润卡片、明细卡片”

### `addToBatch()`
代码位置：约 `L1353`

作用：
- 把当前结果写入批量对比表

### `init()`
代码位置：约 `L1386`

作用：
- 初始化 UI
- 绑定事件
- 初始化国家
- 初始化记忆库

---

## 核心计费逻辑

### 1. 基础体积重
```text
volWt5000 = L * W * H / 5000
```

这是 Noon 站内配送相关的基础体积重。

### 2. FBP 计费重
```text
fbpCw = max(实重, volWt5000)
```

### 3. FBN 计费重
```text
fbnCw = min(max(实重, volWt5000) + 包装附加重量, 当前尺寸档最大计费重)
```

其中：
- `包装附加重量` 来自尺寸档 `pkg`
- `当前尺寸档最大计费重` 来自尺寸档 `maxCw`

### 4. 海运相关

### 海运 CBM
```text
seaCbm = L * W * H / 1,000,000
```

### 海运体积重
```text
seaVolWt = L * W * H / 海运计抛比
```

### 海运计费重
```text
seaCw = max(实重, seaVolWt)
```

### 海运密度倍率
```text
seaFactor = max(1, seaCw / (seaCbm * 海运限重))
```

### 海运头程
```text
seaHead = seaCbm * 海运基础单价 * seaFactor
```

### 5. 本地空运相关

### 空运体积重
```text
airVolWt = L * W * H / 空运计抛比
```

### 空运计费重
```text
airCw = max(实重, airVolWt, 空运最低计费重)
```

### 空运头程
```text
airHead = airCw * 空运基础单价
```

### 6. 跨境直邮相关

### 直邮体积重
```text
crossVolWt = L * W * H / 直邮计抛比
```

### 直邮计费重
```text
crossCw = max(实重, crossVolWt, 直邮最低计费重)
```

### 跨境可售条件
```text
crossAvailable = fbpComm != null 且 crossCw <= 2
```

### 跨境头程
```text
crossHead = 直邮操作费 + crossCw * 直邮空运价
```

### 7. VAT
```text
vatAmt = price / (1 + vat) * vat
```

说明：
- 这里把站点售价视为含税价
- 从含税售价中反推 VAT 部分

### 8. 海外仓操作费
```text
owHandleFee = 基准价 + max(0, fbpCw - 5) * 超出每kg加价
```

说明：
- 使用的是 `FBP计费重`
- 当前代码是连续加价，不是向上取整到整公斤

### 9. 广告费
```text
ads = price * adsRate
```

其中：
- `adsRate` 在代码里先除以 100 再参与计算

### 10. 退货管理费预期成本
```text
retCost = min(15, commission * 0.2) * returnRate
```

### 11. 物流损耗
代码使用统一模型：
```text
logLossBase = (是否纳入头程 ? head / exch : 0) + fee + restock
logLoss = logLossBase * returnRate
```

当前差异：
- `跨境FBP`：`logLossIncludesHead = true`
- 其他四个模式：`logLossIncludesHead = false`

也就是说：
- `FBN海运 / FBN空运 / 海外仓FBP海运 / 海外仓FBP空运`
  - 物流损耗只按 `配送费 + restock` 计算
- `跨境FBP`
  - 物流损耗会把头程折回站点币后一起参与

---

## 总成本与利润公式

所有场景最终都由 `makeScene()` 统一生成。

### 1. 佣金 VAT
```text
commVat = commission * vat
```

### 2. 总成本
```text
total =
  purchase
  + head
  + opsFee
  + (vatAmt + commission + commVat + fee + ads + retCost + logLoss) * exch
```

说明：
- `purchase / head / opsFee` 视为 `CNY`
- `vatAmt / commission / fee / ads / retCost / logLoss` 属于站点币，统一再乘汇率转成 `CNY`

### 3. 利润
```text
profit = (price + reimbValue) * exch - total
```

说明：
- 补贴 `Reimbursement` 被视为收入端增加
- 不是成本冲减

### 4. 利润率
```text
margin = profit / (price * exch)
```

### 5. ROI
```text
roi = profit / total
```

---

## 五个利润场景拆解

### 1. FBN海运
- 是否可用：`当前国家下 FBN 佣金不为 null`
- 头程：`seaCbm * seaRate * seaFactor`
- 平台费用：`FBN佣金`
- 配送费用：`FBN出库费`
- 补贴：无
- 海外仓操作费：无

### 2. FBN空运
- 是否可用：`当前国家下 FBN 佣金不为 null`
- 头程：`airCw * airRate`
- 平台费用：`FBN佣金`
- 配送费用：`FBN出库费`
- 补贴：无
- 海外仓操作费：无

### 3. 海外仓FBP海运
- 是否可用：`当前国家下 FBP 佣金不为 null`
- 头程：`seaCbm * seaRate * seaFactor`
- 平台费用：`FBP佣金`
- 配送费用：`FBP配送费`
- 补贴：有
- 海外仓操作费：有

### 4. 海外仓FBP空运
- 是否可用：`当前国家下 FBP 佣金不为 null`
- 头程：`airCw * airRate`
- 平台费用：`FBP佣金`
- 配送费用：`FBP配送费`
- 补贴：有
- 海外仓操作费：有

### 5. 跨境FBP
- 是否可用：
  - `当前国家下 FBP 佣金不为 null`
  - `直邮计费重 <= 2kg`
- 头程：`crossOps + crossCw * crossRate`
- 平台费用：`FBP佣金`
- 配送费用：`FBP配送费`
- 补贴：有
- 海外仓操作费：无
- 特殊：
  - 物流损耗会把头程折回站点币后一起参与
  - 超过 `2kg` 时直接返回 `N/A`

---

## 最优模式逻辑

代码会从所有 `valid = true` 的场景里选利润最高的那个：
```text
bestScene = validScenes.reduce((best, scene) =>
  scenes[scene].profit > scenes[best].profit ? scene : best
)
```

输出：
- `bestKey`
- `bestLabel`

页面会把最优模式的摘要卡标成：
- `最优`
- 加粗高亮边框

---

## 类目与国家支持逻辑

类目不是所有国家都全开：
- 某些类目在 `UAE` 为 `NA`
- 某些类目只在 `KSA` 开放

页面展示逻辑是：
- 只要某国家下该类目 `FBN` 或 `FBP` 任一可用
- 这个类目就会出现在搜索结果中

所以：
- 类目下拉是“按国家过滤后”的列表
- 切换国家时可选类目会变化

---

## 历史 SKU 模块逻辑

### 1. 存储位置
```text
localStorage key = noon-pricing-v6-sku-memory-v1
```

### 2. 保存规则
- 如果填写了 `SKU`
  - 按 `国家 + SKU` 更新/覆盖
- 如果没有 SKU
  - 按 `国家 + 品名 + 类目` 做兜底匹配

### 3. 保存内容
- 当前全部输入快照
- 当前国家
- 当前类目
- FBN 海运利润
- 海外仓 FBP 海运利润
- 最优模式
- 创建时间 / 更新时间

### 4. 使用方式
- 输入检索词
- 调用匹配项
- 回填表单
- 自动重新计算

---

## 批量对比模块逻辑

批量对比表记录以下字段：
- SKU
- 品名
- 类目
- 尺寸
- 售价
- 5 个场景的净利
- 5 个场景的利润率
- 最优模式

说明：
- 批量表仅存在当前页面会话中
- 没有写入本地存储
- 刷新页面后会清空

---

## 当前文件的规则特征总结

这份 HTML 的规则特点可以总结为：

### 1. 佣金是按“类目 + 国家 + FBN/FBP”独立建模
- 同一类目在 UAE/KSA 可不同
- 同一类目在 FBN/FBP 可不同

### 2. FBN 与 FBP 的核心差异明确分开
- FBN：按尺寸档查出库费
- FBP：按计费重查尾程配送费

### 3. 头程成本独立建模
- FBN/海外仓支持海运和空运两套头程
- 跨境用直邮头程

### 4. 补贴视为收入
- 这符合 Noon Statement 的对账口径

### 5. 历史 SKU 与批量对比都内置在同一个 HTML
- 不依赖后端
- 适合本地离线测算

---

## 建议的后续拆分方向

如果后续要把这份 HTML 继续工程化，可以按下面的方式拆分：

### 1. 数据层
- `CONFIG`
- `CATEGORIES`
- `FBN_UAE_FEES`
- `FBN_KSA_FEES`
- `FBP_FEES`

### 2. 计算层
- 尺寸档计算
- 佣金计算
- FBN/FBP 配送费查表
- 5 场景利润引擎

### 3. UI 层
- 输入区
- 摘要卡片
- 明细卡片
- 费率透视
- 批量对比
- 本地记忆库

### 4. 状态层
- 当前国家
- 当前测算结果
- 本地 SKU 记忆库
- 批量对比列表

---

## 关键代码位置索引
- `CONFIG`：约 `L432`
- `CATEGORIES`：约 `L438`
- `FBN_SIZE_TIERS`：约 `L501`
- `FBN_UAE_FEES`：约 `L512`
- `FBN_KSA_FEES`：约 `L521`
- `FBP_FEES`：约 `L532`
- `getSizeTier()`：约 `L557`
- `getCommission()`：约 `L566`
- `lookupFbnFee()`：约 `L586`
- `lookupFbpFee()`：约 `L596`
- `getReimb()`：约 `L608`
- `initMemory()`：约 `L927`
- `calculate()`：约 `L1079`
- `renderResults()`：约 `L1275`
- `addToBatch()`：约 `L1353`
- `init()`：约 `L1386`

---

## 一句话总结
这份 `noon-pricing-v6.html` 本质上是一个“前端单文件版的 Noon 5 场景利润引擎”，它把 `国家规则 + 类目佣金 + FBN/FBP配送费 + 头程参数 + 本地历史 SKU + 批量对比` 全部收在一个 HTML 中，通过 `calculate()` 统一完成实时利润测算。
