# 投资秘笈 Investment Skill

基于《投资秘笈》投资哲学的 A 股分析 Skill。既支持单只股票分析，也支持读取 `config/stocks.yaml` 对默认股票列表做批量复盘，并按书中框架给出买入/观望/回避决策。

## 文件结构

```
investment/
├── SKILL.md            # Copilot CLI skill 主定义（frontmatter + 核心提示词）
├── skill.yml           # 历史元数据/接口说明
├── investment_data.py  # Python 数据抓取脚本（基于 akshare）
└── README.md
```

## 安装依赖

```bash
pip install akshare ta requests
```

## 直接使用（命令行）

```bash
python investment_data.py 贵州茅台
python investment_data.py 宁德时代
python investment_data.py 000333
```

默认股票列表批量复盘由 `SKILL.md` 中的统一规则处理；批量脚本位于：

```powershell
python ..\default-watchlist-review\default_stock_list_review.py
python ..\default-watchlist-review\default_stock_list_review.py --send-wechat
```

## 分析框架（来自《投资秘笈》）

| 步骤 | 内容 | 权重 |
|------|------|------|
| 1. 大盘判断 | 趋势(多/空/震荡) + 大盘RSI | 30% |
| 2. 板块分析 | 板块强弱 + 是否龙头 | 20% |
| 3. 技术面 | 均线位置 + RSI + 成交量 + K线形态 | 35% |
| 4. 主力行为 | 大单净流入/净流出 | 15% |

**决策阈值：** 综合分 ≥ 7.5 → 买入；5.5–7.5 → 观望；< 5.5 → 不买

## 核心买入条件（六大要素共振）

1. **股价位置** — 在支撑位附近（均线支撑/前低支撑）
2. **K线形态** — 底部反转形态（锤头/吞没/放量阳线）
3. **均线趋势** — 多头排列或均线粘合突破
4. **RSI 指标** — ≤30 超卖区，或出现底背离
5. **成交量** — 缩量回调+放量突破
6. **板块+大盘** — 板块强势 + 大盘不在下降趋势

## 仓位管理（4421 模型）

- **40%** 大盘 ETF（沪深300/上证50）
- **40%** AI/成长赛道核心股（中线持有）
- **20%** 短线机动仓
- **1%**  超高弹性小票

## 数据源接口

`skill.yml` 中定义了以下接口（由 `investment_data.py` 实现）：

| 接口名 | 数据源 | 说明 |
|--------|--------|------|
| `eastmoney_quote` | 东方财富 | 实时行情、换手率、市值 |
| `eastmoney_sector` | 东方财富 | 板块强弱、排名 |
| `eastmoney_money_flow` | 东方财富 | 主力资金流向 |
| `ths_technical_indicators` | 同花顺/akshare | RSI、MACD、均线 |
| `ths_kline_history` | 同花顺/akshare | K线历史数据 |
| `market_index` | akshare | 大盘指数趋势 |
| `news_search` | 东方财富新闻 | 个股新闻公告 |

> ⚠️ 数据接口需在中国大陆网络环境下访问。
>
> ⚠️ 本工具仅供学习参考，不构成投资建议。投资有风险，入市须谨慎。
