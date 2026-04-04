# AI分析工作原理

本文档描述系统中"AI分析"功能的完整执行流程——从数据输入、特征提取、AI调用，到结果输出与存储。

---

## 目录

1. [功能概述](#功能概述)
2. [触发方式](#触发方式)
3. [执行流程](#执行流程)
4. [数据输入](#数据输入)
5. [AI模型调用](#ai模型调用)
6. [Prompt结构](#prompt结构)
7. [输出结果](#输出结果)
8. [本地降级机制](#本地降级机制)
9. [结果存储](#结果存储)
10. [相关代码位置](#相关代码位置)

---

## 功能概述

"AI分析"（特征分析）由 `FeatureExtractor.analyze_with_ai()` 驱动，对每支股票执行以下三步：

1. **提取多维特征**：技术指标特征 + 新闻情感特征 + 市场特征
2. **发送 Prompt 给 AI 模型**：调用 ModelScope Qwen Chat API
3. **解析并保存 JSON 结果**：策略类型推荐、置信度、风险等级等

最终目的是**自动推荐最适合该股票当前状态的投资策略类型**。

---

## 触发方式

AI分析有三个触发入口：

| 入口 | 说明 |
|------|------|
| **Web API** `GET /api/features/<code>` | 访问股票详情页时自动触发；若本地已有缓存则直接读取，否则实时调用 AI |
| **定时任务调度器** | 交易日 16:30 后自动批量分析所有关注股票（默认关闭，需在 config 中启用） |
| **手动调用** `run_scheduler.py` | 交互式运行时选择是否执行 AI分析 |

---

## 执行流程

```
用户请求 / 定时触发
        │
        ▼
FeatureExtractor.analyze_with_ai(code)
        │
        ├─► extract_all_features(code)
        │         ├─ extract_technical_features()   ← 读取技术指标信号
        │         ├─ extract_sentiment_features()   ← 读取新闻情感分数
        │         └─ extract_market_features()      ← 市场贝塔/行业排名
        │
        ├─► indicator_analyzer.get_latest_signals(code)
        │         └─ RSI、MACD、均线趋势、综合评分
        │
        ├─► 构建 Prompt（中文，见下方）
        │
        ├─► AIModelClient.call(prompt)
        │         ├─ 正常：POST → api.modelscope.cn (Qwen Chat)
        │         └─ 失败：_call_mock() 返回本地规则结果
        │
        ├─► 解析 AI 返回的 JSON 字符串
        │
        └─► 返回 { features, ai_analysis }
                  │
                  └─► save_features(code, result)  → data/features/{code}_features.json
```

---

## 数据输入

AI分析汇总了三类特征作为输入：

### 技术特征（来自 `indicators.py`）

| 字段 | 含义 | 取值范围 |
|------|------|---------|
| `trend_strength` | 趋势强度（综合评分绝对值 / 100） | 0 ~ 1 |
| `trend_direction` | 趋势方向 | +1（多头） / -1（空头） |
| `rsi_level` | RSI(6) 归一化值 | 0 ~ 1 |
| `macd_momentum` | MACD 柱状图方向 | +1（正） / -1（负） |
| `ma_alignment` | 均线多空排列 | +1（多头） / -1（空头） |
| `volatility_proxy` | 波动率代理（KDJ-J 偏离度） | 0 ~ 1 |
| `bollinger_position` | 布林带位置 | 0（下轨）~ 1（上轨） |

### 情感特征（来自 `news_collector.py`）

| 字段 | 含义 |
|------|------|
| `avg_sentiment` | 近期新闻平均情感分数 |
| `sentiment_volatility` | 情感分数标准差 |
| `sentiment_trend` | 近5日 vs 早期情感趋势差值 |
| `news_volume` | 平均每日新闻数量 |
| `positive_ratio` | 正面新闻占比 |

### 市场特征

| 字段 | 含义 |
|------|------|
| `market_beta` | 相对大盘的贝塔系数（当前为占位值 1.0） |
| `sector_rank` | 行业排名（当前为占位值 0.5） |

---

## AI模型调用

**模型提供商：** ModelScope（魔搭社区）  
**模型：** Qwen Chat（通义千问对话模型）  
**API 地址：** `https://api.modelscope.cn/api/v1/studio/iic/nlp_qwen_chat/gradio/api/predict`

**调用参数：**

```python
{
    "input": {
        "prompt": "<用户提示>",
        "system": "你是一个专业的量化投资分析师。"
    },
    "parameters": {
        "max_tokens": 2000,
        "temperature": 0.7
    }
}
```

**认证：** Bearer Token（配置文件中 `tokens.modelscope_token`）

---

## Prompt结构

每次分析发送如下中文 Prompt：

```
请分析以下股票的技术特征，判断该股票最适合的投资策略类型：

股票代码: {code}
股票名称: {name}

【技术指标】
- RSI(6): {rsi_6}
- MACD柱状图: {macd_histogram}
- 均线趋势: {ma_trend}
- 综合评分: {overall_score}

【技术特征】
- 趋势强度: {trend_strength}
- RSI水平: {rsi_level}
- 布林带位置: {bollinger_position}

【情感特征】
- 平均情感: {avg_sentiment}
- 情感趋势: {sentiment_trend}

请输出JSON格式的分析结果，包含以下字段：
- strategy_type: 策略类型 (trend_following/value/momentum/swing/mean_reversion)
- confidence: 置信度 (0-1)
- reasoning: 分析理由
- recommended_indicators: 推荐指标列表
- risk_level: 风险等级 (low/medium/high)
- suitable_for: 适合的投资者类型
```

---

## 输出结果

AI 模型返回 JSON，系统提取其中的 `{}` 部分并解析：

```json
{
  "strategy_type": "trend_following",
  "confidence": 0.85,
  "reasoning": "RSI处于中性区间，MACD柱状图为正，均线呈多头排列，趋势较为明确。",
  "recommended_indicators": ["MA", "MACD", "RSI"],
  "risk_level": "medium",
  "suitable_for": "中等风险承受能力的趋势投资者"
}
```

### 策略类型说明

| `strategy_type` | 中文名 | 适用场景 | 参考指标 |
|----------------|--------|---------|---------|
| `trend_following` | 趋势跟踪 | 明确单边行情 | MA、MACD、ADX |
| `momentum` | 动量策略 | RSI 偏离中值、近期强势 | RSI、ROC、MOM |
| `swing` | 波段操作 | 价格振幅较大、区间震荡 | KDJ、布林带、RSI |
| `mean_reversion` | 均值回归 | 价格偏离均值、趋势弱 | 布林带、RSI、Z-Score |
| `value` | 价值投资 | 基本面低估（当前 AI 较少输出） | PE、PB、ROE |

完整分析结果结构：

```json
{
  "features": {
    "code": "600519.SH",
    "name": "贵州茅台",
    "extract_time": "2026-04-04 10:00:00",
    "technical": { ... },
    "sentiment": { ... },
    "market": { ... }
  },
  "ai_analysis": {
    "strategy_type": "trend_following",
    "confidence": 0.85,
    "reasoning": "...",
    "recommended_indicators": ["MA", "MACD"],
    "risk_level": "medium",
    "suitable_for": "..."
  }
}
```

---

## 本地降级机制

当 ModelScope API 无法访问时（网络异常、DNS 解析失败等），系统自动切换至本地规则引擎 `_call_mock()`，返回基于规则的默认结果，同时将 `provider` 临时置为 `'local'` 避免短时间内重复尝试网络调用：

```json
{
  "strategy_type": "trend_following",
  "confidence": 0.7,
  "reasoning": "基于技术指标分析，该股票呈现明显的趋势特征。",
  "recommended_indicators": ["ma", "macd", "rsi"],
  "risk_level": "medium"
}
```

此外，`StrategyTypeClassifier.classify()` 提供了一套**纯规则打分**的本地分类器，可独立于 AI 模型使用：对趋势强度、波动率、RSI 偏离度等进行加权打分，选择得分最高的策略类型。

---

## 结果存储

- **存储路径：** `data/features/{code}_features.json`
- **触发存储的时机：**
  - Web API 首次访问后自动保存（下次访问直接读取缓存）
  - 定时任务批量执行后保存
- **缓存策略：** Web API 优先读取本地文件；如需强制刷新，删除对应 JSON 文件即可

---

## 相关代码位置

| 功能 | 文件 | 位置 |
|------|------|------|
| AI 分析主流程 | `quant_system/feature_extractor.py` | `FeatureExtractor.analyze_with_ai()` L226 |
| AI 模型客户端 | `quant_system/feature_extractor.py` | `AIModelClient` L24 |
| 技术特征提取 | `quant_system/feature_extractor.py` | `extract_technical_features()` L128 |
| 情感特征提取 | `quant_system/feature_extractor.py` | `extract_sentiment_features()` L155 |
| 本地规则分类器 | `quant_system/feature_extractor.py` | `StrategyTypeClassifier.classify()` L381 |
| Web API 接口 | `quant_system/web_app.py` | `GET /api/features/<code>` L2142 |
| 定时任务 | `quant_system/scheduler.py` | `run_ai_analysis_task()` |
| 配置（模型/Token） | `quantization_config.yaml` | `ai_model` / `tokens.modelscope_token` |
