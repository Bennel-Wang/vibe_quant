# AI 日报生成机制

## 触发方式

AI 日报由 `scheduler.py` 中的 `send_daily_report()` 方法生成。支持两种触发方式：

1. **定时任务**：调度器在每个交易日收盘后自动执行 `ai_report` 任务。
2. **手动触发**：通过前端「立即执行」按钮调用 `/api/scheduler/run/<task_id>` 接口手动触发。

---

## 生成流程

```
1. 获取所有持仓/关注股票列表
       ↓
2. 逐股票获取历史 K 线数据（最近 60 日）
       ↓
3. 计算技术指标（RSI / MACD / 布林 / 均线）
       ↓
4. 逐股票判断信号（见下方信号类型）
       ↓
5. 汇总所有股票的信号，生成 Markdown 报告文本
       ↓
6. 通过 notification_manager 发送（微信 / 邮件 / 企业微信等）
```

---

## 检测的信号类型

| 信号 | 触发条件 |
|------|---------|
| **RSI 超卖** | 日线 `rsi_6 < 30`，提示可能超卖 |
| **RSI 超买** | 日线 `rsi_6 > 70`，提示可能超买 |
| **MACD 金叉** | `macd_histogram` 由负转正（前一日 < 0，当日 >= 0） |
| **MACD 死叉** | `macd_histogram` 由正转负（前一日 >= 0，当日 < 0） |
| **均线多头排列** | `ma_5 > ma_20 > ma_60`，趋势向上 |
| **均线空头排列** | `ma_5 < ma_20 < ma_60`，趋势向下 |
| **价格突破布林上轨** | `close > boll_upper`，短期强势 |
| **价格跌破布林下轨** | `close < boll_lower`，短期弱势 |

---

## 报告内容结构

生成的 Markdown 报告包含：

1. **报告标题与日期**
2. **大盘情绪摘要**（`overall_score` 均值，范围 -100 ~ 100）
3. **各股票信号列表**：代码、名称、当日涨跌幅、触发的信号
4. **重点关注列表**：有多个信号叠加的股票
5. **风险提示**：持仓中超买/跌破布林下轨的股票

---

## 发送方式

由 `notification_manager` 按配置的通知渠道发送。支持渠道在 `config/config.yaml` 的 `notifications` 节下配置（微信、邮件、企业微信等）。若发送失败，日志记录错误但不中断系统运行。

---

## 相关代码位置

- 生成逻辑：`quant_system/scheduler.py` → `send_daily_report()` 方法（约第 401-498 行）
- 通知发送：`quant_system/notification_manager.py`
- 调度配置：`config/config.yaml` → `scheduler.tasks` 下的 `ai_report` 任务
