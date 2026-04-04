# 项目重构记录

**日期**: 2026-04-03  
**目的**: 清理无用文件，统一数据存储目录，提高项目可维护性

---

## 目录结构（重构后）

```
vibecoding_quant/
├── main.py                  # 主程序入口（CLI，15+ 子命令）
├── run_scheduler.py         # 定时调度器启动脚本
├── requirements.txt         # Python 依赖
├── config/
│   └── stocks.yaml          # 股票列表配置
├── data/                    # 📁 统一数据存储目录（重构后）
│   ├── history/             # ✅ 历史K线CSV（原位于 data_sourcing/data/）
│   │   └── {code}_{freq}.csv    # 如 600519_SH_day.csv
│   ├── news/                # 新闻数据 {code}_news.csv
│   ├── features/            # AI特征分析 {code}_features.json
│   ├── backtests/           # 回测结果 {uuid}.json
│   ├── strategies.json      # 策略定义
│   ├── groups.json          # 股票分组
│   ├── scheduler_config.json
│   ├── system_state.json
│   ├── listing_dates.json
│   ├── trading_dates.json
│   └── trading_calendars.json
├── data_sourcing/           # 数据采集模块（不再存储数据）
│   ├── data_manager.py      # 核心数据调度（自动降级、增量更新）
│   ├── storage.py           # CSV读写（现写入 data/history/）
│   ├── config.py            # 数据源配置（DATA_DIR → data/history/）
│   ├── trading_calendar.py  # 交易日历（A股/港股）
│   ├── validator.py         # 数据完整性校验
│   ├── code_mapper.py       # 股票代码格式转换
│   ├── test_sources.py      # 数据源测试
│   └── sources/             # 各数据源适配器
│       ├── tushare_source.py
│       ├── baostock_source.py
│       ├── easyquotation_source.py
│       ├── pytdx_source.py
│       ├── mootdx_source.py
│       └── adjust_utils.py
├── quant_system/            # 量化系统核心模块
│   ├── config_manager.py    # 配置管理（读取 quantization_config.yaml）
│   ├── stock_manager.py     # 股票管理（从 config/stocks.yaml 加载）
│   ├── data_source.py       # 统一数据接口（委托 data_sourcing）
│   ├── data_cleaner.py      # 数据清洗与验证
│   ├── indicators.py        # 技术指标（读写 data/history/）
│   ├── feature_extractor.py # AI特征提取（写入 data/features/）
│   ├── news_collector.py    # 新闻采集（写入 data/news/）
│   ├── strategy.py          # 策略定义与执行
│   ├── strategy_matcher.py  # 策略匹配器
│   ├── market_regime.py     # 市场状态检测
│   ├── stock_classifier.py  # 股票分类器
│   ├── backtest.py          # 回测引擎（结果写入 data/backtests/）
│   ├── risk_manager.py      # 风险管理
│   ├── notification.py      # 通知推送（PushPlus）
│   ├── scheduler.py         # 定时任务
│   ├── web_app.py           # Flask Web界面
│   └── code_converter.py    # 代码格式转换工具
├── scripts/                 # 辅助脚本（开发/维护工具）
│   ├── backtest_new_strategies.py    # 批量回测新策略
│   ├── backtest_regime_match.py      # 市场状态匹配回测
│   ├── diagnose_indicators.py        # 指标诊断工具
│   ├── optimize_specialized.py       # 专项策略优化
│   ├── optimize_strategy.py          # 通用策略优化
│   ├── recompute_indicators.py       # 重新计算指标
│   ├── run_quick_backtest.py         # 快速回测
│   └── test_api_and_backtest.py      # API与回测测试
├── logs/                    # ✅ 统一日志目录（原 data_sourcing/logs/ 也重定向到此）
├── easyquotation/           # EasyQuotation库（本地修改版）
└── docs/                    # 文档
    └── refactoring_notes.md # 本文件
```

---

## 重构变更详情

### 1. 数据存储统一 ✅

**问题**：历史K线CSV存储在 `data_sourcing/data/`，与系统其他数据（策略、回测等存于 `data/`）分离。

**变更**：
- 将 35 个 CSV 文件从 `data_sourcing/data/` 迁移到 `data/history/`
- 修改 `data_sourcing/config.py`：
  ```python
  # 改前
  DATA_DIR = PROJECT_ROOT / "data"       # → data_sourcing/data/
  LOG_DIR  = PROJECT_ROOT / "logs"       # → data_sourcing/logs/

  # 改后
  DATA_DIR = _REPO_ROOT / "data" / "history"  # → data/history/
  LOG_DIR  = _REPO_ROOT / "logs"              # → logs/
  ```
- 修改 `quant_system/indicators.py`（2处硬编码路径）：
  ```python
  # 改前
  self.data_dir = ...'data_sourcing', 'data'
  # 改后
  self.data_dir = ...'data', 'history'
  ```

### 2. 日志统一 ✅

`data_sourcing` 模块的日志现在写入根目录 `logs/`，不再使用 `data_sourcing/logs/`（历史日志文件保留）。

### 3. 无用脚本清理 ✅

删除以下开发迭代遗留脚本（共 11 个）：

| 文件 | 删除原因 |
|------|---------|
| `scripts/optimize_r2.py ~ optimize_r5.py` | 版本迭代遗留，已被 optimize_specialized.py 取代 |
| `scripts/optimize_spec_r2.py ~ spec_r3.py` | 版本迭代遗留 |
| `scripts/compile_pyc.py` | 无实际用途 |
| `scripts/migrate_csv_names.py` | 一次性迁移脚本，已完成历史使命 |
| `scripts/debug_strategy_ai.py` | 开发调试用，不属于系统功能 |
| `scripts/test_scheduler_delete.py` | 临时测试脚本 |
| `scripts/regime_backtest_results.txt` | 输出结果文件，非源码 |

---

## 数据流说明

```
数据采集流程：
  main.py update-data
      → data_source.py (UnifiedDataSource)
          → data_sourcing/data_manager.py (DataManager)
              → sources/{tushare,baostock,...}_source.py
                  → data/history/{code}_{freq}.csv  ← 写入统一路径

指标计算：
  data_manager._add_indicators()  或  quant_system/indicators.py
      → 读取 data/history/*.csv
      → 写回 data/history/*.csv（含指标列）

分析/回测：
  quant_system/{backtest,strategy,feature_extractor}.py
      → 读取 data/history/*.csv（通过 data_source.get_historical_data）
      → 写入 data/{backtests,features}/ 下的 JSON 文件
```

---

## 配置文件位置

| 配置 | 路径 |
|------|------|
| 主配置（Token、路径等）| `C:\Users\quantization_config.yaml`（用户本地） |
| 股票列表 | `config/stocks.yaml` |
| 策略定义 | `data/strategies.json`（运行时生成） |
| 调度器配置 | `data/scheduler_config.json` |
