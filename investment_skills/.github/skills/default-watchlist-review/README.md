# Default Watchlist Review Skill

> 该能力已并入 `investment-mij`；本目录保留为兼容入口和批量脚本位置。

批量读取仓库根目录 `config/stocks.yaml` 中的默认股票列表，先看大盘，再对列表中的股票做统一筛选，最后输出一版更适合日常复盘的结论。

Copilot CLI 的主 skill 规则现在以 `..\investment\SKILL.md` 为准；这里主要保留：

1. 兼容旧 skill 名称/目录
2. 批量复盘脚本 `default_stock_list_review.py`
3. 与默认自选复盘相关的说明

1. 当前市场能不能做
2. 默认列表里哪些是 **买入 / 观望 / 回避**
3. **具体到重点票，为什么这么判**
4. 用户要求时，直接复用 `quant_system.notification.notification_manager` 发到微信

## 文件结构

```text
default-watchlist-review/
├── skill.yml
├── SKILL.md            # 兼容入口，已指向 investment-mij 的统一规则
├── README.md
└── default_stock_list_review.py
```

## 直接使用

```powershell
python .github\skills\default-watchlist-review\default_stock_list_review.py
python .github\skills\default-watchlist-review\default_stock_list_review.py --top 6
python .github\skills\default-watchlist-review\default_stock_list_review.py --limit 5
python .github\skills\default-watchlist-review\default_stock_list_review.py --send-wechat
```

## 输出风格

默认输出重点是：

- 大盘结论
- 批量统计（买入 / 观望 / 回避）
- `4. 具体到重点票，为什么这么判`

不会把 30 只股票全展开成长报告，而是优先解释最值得关注的几只。

## 判定规则

脚本把《投资秘笈》批量化为 6 个维度，每项 0-2 分，总分 12 分：

1. 股价位置
2. K 线/短期价格行为
3. 均线趋势
4. RSI 强弱
5. 量能
6. 大盘环境

默认口径：

- `>= 10` 且均线多头：**买入**
- `7 ~ 9`：**观望**
- `< 7`：**回避**

## 微信发送

加 `--send-wechat` 时，脚本会调用：

```python
from quant_system.notification import notification_manager
notification_manager.send_markdown_message(...)
```

前提是项目根目录下的通知配置已可用。

> 仅供学习参考，不构成投资建议。
