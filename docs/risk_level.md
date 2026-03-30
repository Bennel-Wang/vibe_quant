# 风险等级评定机制

## 概述

风险等级由 `risk_manager.py` 中的 `_assess_risk_level()` 方法评定，在每次计算组合风险时自动执行。评定结果为三档：**LOW（低）/ MEDIUM（中）/ HIGH（高）**。

---

## 两个核心输入指标

| 指标 | 计算公式 | 含义 |
|------|---------|------|
| **仓位比例** `position_ratio` | `总持仓市值 / 总资产` | 当前资金有多少比例投入了股票 |
| **集中度** `concentration` | `最大单股市值 / 总持仓市值` | 持仓中最大一只股票占总仓位的比例 |

---

## 评定规则

```
配置上限 max_position_ratio（默认 0.8，来自 config.yaml）

HIGH（高风险），满足以下任一条件：
  - position_ratio > max_position_ratio × 0.9   （仓位达到上限的 90%）
  - concentration > 0.5                          （单股集中度超过 50%）

MEDIUM（中风险），HIGH 不满足时，满足以下任一条件：
  - position_ratio > max_position_ratio × 0.7   （仓位达到上限的 70%）
  - concentration > 0.3                          （单股集中度超过 30%）

LOW（低风险）：
  - 以上条件均不满足
```

### 默认阈值速查表（`max_position_ratio = 0.8`）

| 等级 | 仓位比例触发线 | 集中度触发线 |
|------|-------------|------------|
| HIGH | > 72%（0.8×0.9） | > 50% |
| MEDIUM | > 56%（0.8×0.7） | > 30% |
| LOW | ≤ 56% 且 ≤ 30% | — |

---

## 风险等级的使用场景

1. **仪表盘展示**：组合风险页面（`/api/risk/portfolio`）直接返回当前风险等级，前端以颜色区分（绿/黄/红）。
2. **交易前检查**：下单时若 `risk_level == HIGH`，系统会在日志中警告并可配置拦截。
3. **AI 日报**：`send_daily_report()` 会将当前风险等级写入报告的风险提示部分。
4. **风险报告**：`generate_risk_report()` 汇总最大回撤、波动率、VaR 等指标，并附上当前风险等级。

---

## 可调配置

`config/config.yaml` 中的 `risk_management` 节：

```yaml
risk_management:
  max_position_ratio: 0.8      # 单组合最大仓位上限（对应 HIGH/MEDIUM 阈值的基准）
  max_single_position: 0.3     # 单只股票最大仓位（与 concentration 不同，此项作用于建仓拦截）
  stop_loss_ratio: 0.08        # 止损线（亏损超过此比例触发提醒）
  take_profit_ratio: 0.20      # 止盈线（盈利超过此比例触发提醒）
```

---

## 相关代码位置

- 评定逻辑：`quant_system/risk_manager.py` → `_assess_risk_level()` 方法（约第 301-308 行）
- 组合风险计算：`quant_system/risk_manager.py` → `calculate_portfolio_risk()` 方法
- API 接口：`quant_system/web_app.py` → `api_risk_portfolio()` 路由（`/api/risk/portfolio`）
