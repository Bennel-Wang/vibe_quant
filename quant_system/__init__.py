"""
量化交易系统
============

一个功能完整的量化交易系统，包含数据采集、技术指标计算、策略回测等功能。

主要模块:
- config: 配置管理
- stock_manager: 股票代码管理
- data_source: 数据源管理 (Tushare + Easyquotation)
- news_collector: 新闻采集与情感分析
- indicators: 技术指标计算
- data_cleaner: 数据清洗
- feature_extractor: 特征提取
- strategy: 策略层
- backtest: 回测引擎
- risk_manager: 风控模块
- notification: 消息通知
- web: Web可视化界面
"""

__version__ = "1.0.0"
__author__ = "Quant System"
