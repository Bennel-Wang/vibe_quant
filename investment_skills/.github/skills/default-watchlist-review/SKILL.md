---
name: default-watchlist-review
description: |
  兼容入口：默认股票列表批量复盘能力现已并入 investment-mij。
  当用户要求复盘 config/stocks.yaml 中的默认股票列表、默认自选，或发送到微信时，
  按 investment-mij 中的批量复盘规则执行。
---

# Default Watchlist Review

这个 skill 现在是 **兼容入口**，避免旧引用失效；核心批量复盘规则已合并到 `investment-mij`。

执行要求：

1. 从仓库根目录 `config/stocks.yaml` 读取默认股票列表；
2. 只提取 `stocks` 中的 `name / code / market`；
3. 先判断大盘，再批量分析默认列表中的股票；
4. 输出 **买入 / 观望 / 回避**，并重点解释“为什么这么判”；
5. 只有在用户明确要求时，才复用 `quant_system.notification.notification_manager` 发送到微信。

优先使用同目录脚本：

```powershell
python .github\skills\default-watchlist-review\default_stock_list_review.py
python .github\skills\default-watchlist-review\default_stock_list_review.py --send-wechat
```

为避免两套提示词漂移，具体的批量打分、输出格式、市场判断口径以 `investment\SKILL.md` 为准。
