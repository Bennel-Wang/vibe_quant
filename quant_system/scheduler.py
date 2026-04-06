"""
定时任务调度模块
每个交易日北京时间 14:25 和 14:45 自动运行数据更新、新闻更新和AI分析
"""

import os
import sys
import json
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from threading import Thread
import pytz

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from .config_manager import config
from .stock_manager import stock_manager
from .data_source import unified_data
from .news_collector import news_collector, news_pipeline, sentiment_analyzer
from .indicators import technical_indicators, indicator_analyzer
from .feature_extractor import feature_extractor
from .strategy import ai_decision_maker, strategy_manager
from .notification import notification_manager
from .strategy_matcher import strategy_matcher
from .market_regime import market_regime_detector
from .stock_classifier import stock_classifier

logger = logging.getLogger(__name__)

# 北京时间时区
BEIJING_TZ = pytz.timezone('Asia/Shanghai')


class TradingScheduler:
    """交易定时调度器"""
    
    def __init__(self):
        self.scheduler = BackgroundScheduler(timezone=BEIJING_TZ)
        self.is_running = False
        self.data_dir = config.get('data_storage.data_dir', './data')
        self.scheduler_config_path = os.path.join(self.data_dir, 'scheduler_config.json')
        self._load_config()
        # 任务执行状态跟踪
        self.task_status: Dict[str, Dict] = {}
        self._task_funcs_map = {
            'data_update': self.run_data_update_task,
            'indicators': self.run_indicators_task,
            'news': self.run_news_task,
            'ai_analysis': self.run_ai_analysis_task,
            'notification': self.run_notification_task,
        }
        self._task_labels = {
            'data_update': '数据更新',
            'indicators': '指标计算',
            'news': '新闻采集',
            'ai_analysis': 'AI分析',
            'notification': '通知推送',
        }
        # 自定义任务支持的类型（前端可选择）
        # key: 内部标识, value: 描述
        self.custom_task_types = {
            'data_update': '数据更新',
            'ai_report': '生成AI日报并发送',
            'strategy_alert': '策略触发提醒',
            'market_strategy_analysis': '大盘阶段判断及对应策略推荐'
        }
        # 映射到实际方法（在实例化后，方法可用）
        self._custom_type_to_func = {
            'data_update': self.update_all,
            'ai_report': self.send_daily_report,
            'strategy_alert': self.run_strategy_backtest_alert,
            'market_strategy_analysis': self.run_market_strategy_analysis
        }
    
    # 内建任务默认调度配置（hour/minute/enabled 必须完整）
    DEFAULT_TASK_SCHEDULE = {
        'data_update': {'hour': 15, 'minute': 30, 'enabled': True},
        'indicators': {'hour': 15, 'minute': 35, 'enabled': True},
        'news': {'hour': 15, 'minute': 45, 'enabled': True},
        'notification': {'hour': 16, 'minute': 0, 'enabled': True},
        'ai_analysis': {'hour': 16, 'minute': 30, 'enabled': False},
    }

    def _load_config(self):
        """加载调度器配置"""
        default_config = {
            'enabled': True,
            'morning_time': '09:25',
            'afternoon_times': ['15:30', '15:45'],
            'selected_stocks': [],
            'tasks': {
                'update_data': True,
                'update_news': True,
                'update_indicators': True,
                'ai_analysis': False,
                'send_notification': True,
            },
            'task_schedule': {},
            'custom_tasks': [],
        }
        
        if os.path.exists(self.scheduler_config_path):
            try:
                with open(self.scheduler_config_path, 'r', encoding='utf-8') as f:
                    loaded_config = json.load(f)
                    default_config.update(loaded_config)
            except Exception as e:
                logger.error(f"加载调度器配置失败: {e}")
        
        # 深度合并 task_schedule：确保每个内建任务都有完整的 hour/minute/enabled
        merged_schedule = {}
        loaded_schedule = default_config.get('task_schedule', {})
        for name, defaults in self.DEFAULT_TASK_SCHEDULE.items():
            if name in loaded_schedule:
                # 以默认值为底，用已保存的值覆盖
                entry = dict(defaults)
                entry.update(loaded_schedule[name])
                merged_schedule[name] = entry
            else:
                # 任务在配置文件中不存在 → 已被用户删除，不恢复
                pass
        # 保留配置中不在默认列表里的条目（理论上不应有，但防御性处理）
        for name, val in loaded_schedule.items():
            if name not in merged_schedule:
                merged_schedule[name] = val
        
        # 如果 task_schedule 为空（首次运行 / 文件被清空），用全部默认
        if not merged_schedule and not loaded_schedule:
            merged_schedule = {k: dict(v) for k, v in self.DEFAULT_TASK_SCHEDULE.items()}
        
        default_config['task_schedule'] = merged_schedule
        self.config = default_config
    
    def _save_config(self):
        """保存调度器配置"""
        try:
            with open(self.scheduler_config_path, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存调度器配置失败: {e}")
    
    def is_trading_day(self) -> bool:
        """
        判断今天是否为交易日
        简化版：周一到周五为交易日（不考虑节假日）
        """
        now = datetime.now(BEIJING_TZ)
        return now.weekday() < 5  # 0-4 为周一到周五
    
    def get_selected_stocks(self) -> List[str]:
        """获取选中的股票代码列表（后缀格式如 600519.SH）"""
        if self.config.get('selected_stocks'):
            return self.config['selected_stocks']
        
        # 如果没有指定，返回所有股票的 full_code
        stocks = stock_manager.get_all_stocks()
        return [s.full_code for s in stocks]
    
    def run_daily_tasks(self):
        """运行每日定时任务（全量）"""
        if not self.is_trading_day():
            logger.info("今天不是交易日，跳过任务")
            return
        self.run_daily_tasks_once()

    def run_daily_tasks_once(self):
        logger.info("=" * 60)
        logger.info("开始执行定时任务")
        logger.info("=" * 60)
        
        stocks = self.get_selected_stocks()
        tasks = self.config.get('tasks', {})
        
        analysis_results = []
        
        for code in stocks:
            try:
                logger.info(f"处理股票: {code}")
                stock = stock_manager.get_stock_by_code(code)
                stock_name = stock.name if stock else code
                
                # 1. 更新数据
                if tasks.get('update_data', True):
                    logger.info(f"  - 更新历史数据...")
                    unified_data.get_historical_data(code)
                
                # 2. 更新新闻
                if tasks.get('update_news', True):
                    logger.info(f"  - 采集新闻...")
                    news_df = news_collector.fetch_stock_news(code)
                    if not news_df.empty:
                        sentiment_analyzer.analyze_news_df(news_df)
                
                # 3. 更新技术指标
                if tasks.get('update_indicators', True):
                    logger.info(f"  - 计算技术指标...")
                    df = technical_indicators.calculate_all_indicators(code)
                    if not df.empty:
                        technical_indicators.save_indicators(code, df)
                
                # 4. AI分析
                if tasks.get('ai_analysis', False):
                    logger.info(f"  - AI分析...")
                    features = feature_extractor.analyze_with_ai(code)
                    feature_extractor.save_features(code, features)
                    
                    decision = ai_decision_maker.make_decision(code)
                    
                    analysis_results.append({
                        'code': code,
                        'name': stock_name,
                        'decision': decision,
                        'features': features
                    })
                
                logger.info(f"  完成")
                
            except Exception as e:
                logger.error(f"处理 {code} 失败: {e}")
        
        # 5. 发送通知
        if tasks.get('send_notification', True) and analysis_results:
            self._send_analysis_report(analysis_results)
        
        logger.info("=" * 60)
        logger.info("定时任务执行完成")
        logger.info("=" * 60)
    
    def run_data_update_task(self, force: bool = False):
        """仅更新行情数据。force=True 时跳过交易日检查（由调用方自行控制）。"""
        cfg = self.config.get('task_schedule', {}).get('data_update', {})
        if not force and cfg.get('skip_non_trading_day', True) and not self.is_trading_day():
            logger.info("跳过数据更新任务：今天不是交易日")
            return
        logger.info("开始执行数据更新任务")
        stocks = self.get_selected_stocks()
        for i, code in enumerate(stocks):
            try:
                unified_data.get_historical_data(code)
            except Exception as e:
                logger.error(f"更新数据失败 {code}: {e}")
            if i < len(stocks) - 1:
                time.sleep(3)
        logger.info("数据更新任务完成")
    
    def run_indicators_task(self):
        """仅计算技术指标"""
        cfg = self.config.get('task_schedule', {}).get('indicators', {})
        if cfg.get('skip_non_trading_day', True) and not self.is_trading_day():
            logger.info("跳过指标计算任务：今天不是交易日")
            return
        logger.info("开始执行指标计算任务")
        stocks = self.get_selected_stocks()
        for i, code in enumerate(stocks):
            try:
                df = technical_indicators.calculate_all_indicators(code)
                if not df.empty:
                    technical_indicators.save_indicators(code, df)
            except Exception as e:
                logger.error(f"计算指标失败 {code}: {e}")
            if i < len(stocks) - 1:
                time.sleep(2)
        logger.info("指标计算任务完成")
    
    def run_news_task(self):
        """仅更新新闻"""
        cfg = self.config.get('task_schedule', {}).get('news', {})
        if cfg.get('skip_non_trading_day', True) and not self.is_trading_day():
            logger.info("跳过新闻更新任务：今天不是交易日")
            return
        logger.info("开始执行新闻更新任务")
        stocks = self.get_selected_stocks()
        for i, code in enumerate(stocks):
            try:
                news_df = news_collector.fetch_stock_news(code)
                if not news_df.empty:
                    sentiment_analyzer.analyze_news_df(news_df)
            except Exception as e:
                logger.error(f"更新新闻失败 {code}: {e}")
            if i < len(stocks) - 1:
                time.sleep(5)
        logger.info("新闻更新任务完成")
    
    def run_notification_task(self):
        """发送每日行情总结通知"""
        cfg = self.config.get('task_schedule', {}).get('notification', {})
        if cfg.get('skip_non_trading_day', True) and not self.is_trading_day():
            logger.info("跳过通知任务：今天不是交易日")
            return
        if not self.config.get('tasks', {}).get('send_notification', True):
            logger.info("通知已禁用，跳过")
            return
        if not notification_manager.enabled:
            logger.warning("通知渠道未配置，跳过发送")
            return

        logger.info("开始发送每日总结通知")
        try:
            now = datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M')
            stocks = self.get_selected_stocks()

            # 收集各股票当日行情
            lines = []
            for code in stocks[:20]:  # 最多显示20只
                try:
                    stock = stock_manager.get_stock_by_code(code)
                    name = stock.name if stock else code
                    df = unified_data.get_historical_data(code)
                    if df is not None and not df.empty and len(df) >= 2:
                        last = df.iloc[-1]
                        prev = df.iloc[-2]
                        close = float(last.get('close', 0))
                        prev_close = float(prev.get('close', 0))
                        chg = (close - prev_close) / prev_close * 100 if prev_close else 0
                        sign = '📈' if chg >= 0 else '📉'
                        lines.append(f"- {sign} **{name}({code})**: {close:.2f}  {chg:+.2f}%")
                except Exception:
                    pass

            body = f"## 📊 每日收盘总结\n\n**时间**: {now}\n\n"
            body += "### 监控股票今日表现\n\n"
            body += "\n".join(lines) if lines else "- 暂无数据"
            body += f"\n\n---\n*数据更新、指标计算、新闻抓取已完成*"

            notification_manager.send_markdown_message(
                f"📊 收盘总结 ({now})", body
            )
            logger.info("每日总结通知已发送")
        except Exception as e:
            logger.error(f"发送每日总结通知失败: {e}")

    def run_ai_analysis_task(self, force: bool = False):
        """仅执行AI分析。force=True 时跳过交易日检查。"""
        cfg = self.config.get('task_schedule', {}).get('ai_analysis', {})
        if not force and cfg.get('skip_non_trading_day', True) and not self.is_trading_day():
            logger.info("跳过AI分析任务：今天不是交易日")
            return
        logger.info("开始执行AI分析任务")
        results = []
        for code in self.get_selected_stocks():
            try:
                stock = stock_manager.get_stock_by_code(code)
                features = feature_extractor.analyze_with_ai(code)
                feature_extractor.save_features(code, features)
                decision = ai_decision_maker.make_decision(code)
                results.append({'code': code, 'name': stock.name if stock else code,
                                 'decision': decision, 'features': features})
            except Exception as e:
                logger.error(f"AI分析失败 {code}: {e}")
        if results and self.config.get('tasks', {}).get('send_notification', True):
            self._send_analysis_report(results)
        logger.info("AI分析任务完成")

    def run_strategy_alert_task(self, force: bool = False):
        """扫描所有或选中股票，运行所有策略并对触发信号发送通知。force=True 时跳过交易日检查。"""
        cfg = self.config.get('task_schedule', {}).get('strategy_alert', {})
        if not force and cfg.get('skip_non_trading_day', True) and not self.is_trading_day():
            logger.info("跳过策略触发任务：今天不是交易日")
            return
        logger.info("开始执行策略触发提醒任务")
        stocks = self.get_selected_stocks()
        for i, code in enumerate(stocks):
            try:
                stock = stock_manager.get_stock_by_code(code)
                results = strategy_manager.run_all_strategies(code)
                for strat_name, decision in results.items():
                    # decision is StrategyDecision
                    try:
                        action = getattr(decision, 'action', None)
                        confidence = getattr(decision, 'confidence', 0)
                        reasoning = getattr(decision, 'reasoning', '')
                    except Exception:
                        continue
                    # 发送阈值: 仅在非 hold 且置信度较高时发送
                    if action and action != 'hold' and (confidence and confidence >= 0.5):
                        try:
                            notification_manager.send_strategy_signal(code, strat_name, action, confidence, reasoning)
                            logger.info(f"策略触发通知已发送: {code} {strat_name} {action} {confidence}")
                        except Exception as e:
                            logger.error(f"发送策略通知失败 {code} {strat_name}: {e}")
            except Exception as e:
                logger.error(f"策略触发检查失败 {code}: {e}")
            if i < len(stocks) - 1:
                time.sleep(2)
        logger.info("策略触发提醒任务完成")

    # ===================================================================
    # 自定义任务类型对应的真实业务函数
    # ===================================================================

    def update_all(self, force: bool = False):
        """数据更新：更新所有股票、指数行情数据（一次性增量更新）"""
        if not force and not self.is_trading_day():
            logger.info("跳过全量数据更新：今天不是交易日")
            return
        logger.info("开始执行全量行情更新")
        try:
            unified_data.update_all_data()
            logger.info("全量行情更新完成")
        except Exception as e:
            logger.error(f"全量行情更新失败: {e}")

    def send_daily_report(self, force: bool = False):
        """生成AI日报并通过通知渠道发送"""
        if not force and not self.is_trading_day():
            logger.info("跳过日报发送：今天不是交易日")
            return
        logger.info("开始生成AI日报")
        try:
            # 先拉取最新实时行情，确保报告价格是收盘/最新价，而非历史缓存中的开盘价
            realtime_prices: dict = {}
            try:
                live_df = unified_data.get_realtime_data()
                if live_df is not None and not live_df.empty:
                    for _, row in live_df.iterrows():
                        raw_code = str(row.get('code', ''))
                        bare = raw_code.split('.')[0]
                        price = float(row.get('close', row.get('price', 0)) or 0)
                        if price > 0:
                            realtime_prices[bare] = price
                    logger.info(f"实时行情已加载: {len(realtime_prices)} 只股票")
            except Exception as e:
                logger.warning(f"实时行情获取失败，将使用历史数据价格: {e}")

            stocks = stock_manager.get_stocks()
            report_items = []

            for stock in stocks:
                try:
                    df = unified_data.get_historical_data(stock.full_code)
                    if df is None or df.empty or len(df) < 5:
                        continue
                    df_ind = technical_indicators.calculate_all_indicators_from_df(df)
                    if df_ind.empty:
                        continue
                    latest = df_ind.iloc[-1]
                    prev = df_ind.iloc[-2] if len(df_ind) >= 2 else latest

                    # 优先使用实时价格，避免开盘价缓存导致价格滞后
                    hist_close = float(latest.get('close', 0) or 0)
                    close = realtime_prices.get(stock.code, hist_close) or hist_close
                    prev_close = float(prev.get('close', 0) or close)
                    chg_pct = (close - prev_close) / prev_close * 100 if prev_close > 0 else 0
                    rsi6 = float(latest.get('rsi_6', 50) or 50)
                    macd_val = float(latest.get('macd', 0) or 0)
                    macd_sig = float(latest.get('macd_signal', 0) or 0)
                    ma5 = float(latest.get('ma_5', close) or close)
                    ma20 = float(latest.get('ma_20', close) or close)

                    signals = []
                    if rsi6 < 30:
                        signals.append('RSI超卖，关注反弹机会')
                    elif rsi6 > 70:
                        signals.append('RSI超买，注意回调风险')
                    if macd_val > macd_sig and float(prev.get('macd', 0) or 0) <= float(prev.get('macd_signal', 0) or 0):
                        signals.append('MACD金叉，看涨信号')
                    elif macd_val < macd_sig and float(prev.get('macd', 0) or 0) >= float(prev.get('macd_signal', 0) or 0):
                        signals.append('MACD死叉，看跌信号')
                    if close > ma5 > ma20:
                        signals.append('多头排列，趋势向好')
                    elif close < ma5 < ma20:
                        signals.append('空头排列，趋势偏弱')

                    if rsi6 < 35 and close > ma20:
                        suggestion = '建议关注买入'
                    elif rsi6 > 65 and close < ma20:
                        suggestion = '建议减仓观望'
                    elif macd_val > macd_sig and close > ma20:
                        suggestion = '持有/加仓'
                    else:
                        suggestion = '观望等待'

                    report_items.append({
                        'code': stock.code,
                        'name': stock.name,
                        'close': round(close, 2),
                        'change_pct': round(chg_pct, 2),
                        'rsi_6': round(rsi6, 2),
                        'signals': signals,
                        'suggestion': suggestion,
                    })
                except Exception as e:
                    logger.debug(f"日报生成 {stock.code} 失败: {e}")
                    continue

            if not report_items:
                logger.warning("日报无数据，跳过发送")
                return

            bullish_count = sum(1 for r in report_items if '多头' in str(r['signals']) or r['change_pct'] > 1)
            bearish_count = sum(1 for r in report_items if '空头' in str(r['signals']) or r['change_pct'] < -1)

            if bullish_count > bearish_count * 1.5:
                market_sentiment, market_advice = '偏多', '市场整体偏强，可适当增加仓位'
            elif bearish_count > bullish_count * 1.5:
                market_sentiment, market_advice = '偏空', '市场整体偏弱，建议控制仓位'
            else:
                market_sentiment, market_advice = '震荡', '市场方向不明，建议谨慎操作'

            date_str = datetime.now(BEIJING_TZ).strftime('%Y-%m-%d')
            content = f"## 📊 量化交易日报 ({date_str})\n\n"
            content += f"### 市场概览\n"
            content += f"- 市场情绪: **{market_sentiment}**\n"
            content += f"- 操作建议: {market_advice}\n"
            content += f"- 看涨/看跌: {bullish_count}/{bearish_count}\n\n"
            content += f"### 个股分析\n\n"
            for item in report_items:
                chg_sign = '+' if item['change_pct'] >= 0 else ''
                content += f"**{item['name']}({item['code']})** ¥{item['close']} ({chg_sign}{item['change_pct']}%)\n"
                if item['signals']:
                    content += f"  信号: {', '.join(item['signals'])}\n"
                content += f"  建议: {item['suggestion']}\n\n"

            notification_manager.send_markdown_message(f"量化日报 {date_str}", content)
            logger.info("AI日报已发送")
        except Exception as e:
            logger.error(f"发送AI日报失败: {e}")

    def run_strategy_backtest_alert(self, force: bool = False):
        """策略提醒：调用web_app中的compute_and_send_strategy_alerts，
        与「策略提醒」子页面的「全部计算」+「发送到微信」完全一致。"""
        if not force and not self.is_trading_day():
            logger.info("跳过策略回测提醒：今天不是交易日")
            return
        logger.info("开始执行策略回测提醒任务")
        try:
            from .web_app import compute_and_send_strategy_alerts
            compute_and_send_strategy_alerts()
        except Exception as e:
            logger.error(f"策略回测提醒失败: {e}")

    def run_market_strategy_analysis(self, force: bool = False):
        """大盘T/V评分阶段分析 + 个股评分匹配推荐，发送每日通知"""
        if not force and not self.is_trading_day():
            logger.info("跳过大盘策略分析：今天不是交易日")
            return
        logger.info("开始执行大盘策略分析任务")
        try:
            result = strategy_matcher.analyze_all_stocks()
            market = result['market']
            stocks_data = result['stocks']

            date_str = datetime.now(BEIJING_TZ).strftime('%Y-%m-%d')
            regime_emoji = market.get('regime_emoji', '❓')
            regime_label = market.get('regime_label', '未知')
            t_score = market.get('t_score', 0)
            v_score = market.get('v_score', 0)
            detail = market.get('detail', '')

            content = f"## {regime_emoji} 大盘操作建议 ({date_str})\n\n"
            content += f"### 大盘阶段: {regime_label}（T评分={t_score:.0f} / V评分={v_score:.0f}）\n"
            content += f"{detail}\n\n"

            ACTION_ICONS  = {'buy': '🟢', 'layout': '🔵', 'watch': '🟡', 'empty': '⚪'}
            ACTION_LABELS = {'buy': '买入', 'layout': '可布局', 'watch': '观望', 'empty': '空仓'}

            buy_stocks   = [s for s in stocks_data if s.get('action') in ('buy', 'layout')]
            watch_stocks = [s for s in stocks_data if s.get('action') == 'watch']
            empty_stocks = [s for s in stocks_data if s.get('action') == 'empty']

            if buy_stocks:
                content += f"### ✅ 建议操作 ({len(buy_stocks)}只)\n\n"
                for s in buy_stocks:
                    sc   = s.get('scores', {})
                    t    = sc.get('t_score', '-')
                    v    = sc.get('v_score', '-')
                    icon = ACTION_ICONS.get(s.get('action', 'empty'), '⚪')
                    lbl  = ACTION_LABELS.get(s.get('action', 'empty'), '')
                    content += f"{icon} **{s['name']}({s['code']})** T={t}/V={v} [{lbl}]\n"
                    content += f"  {s.get('reason', '')}\n"
                content += "\n"

            if watch_stocks:
                content += f"### 🟡 观望 ({len(watch_stocks)}只)\n"
                for s in watch_stocks[:10]:
                    sc = s.get('scores', {})
                    t  = sc.get('t_score', '-')
                    v  = sc.get('v_score', '-')
                    content += f"- {s['name']}({s['code']}) T={t}/V={v}  {s.get('reason','')}\n"
                content += "\n"

            if empty_stocks:
                content += f"### ⚪ 空仓 ({len(empty_stocks)}只)\n"
                content += '、'.join(s['name'] for s in empty_stocks[:12]) + "\n\n"

            content += f"\n---\n*生成时间: {datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M')}*"

            notification_manager.send_markdown_message(
                f"{regime_emoji} 大盘策略分析 {date_str} | {regime_label}", content
            )
            logger.info(f"大盘策略分析通知已发送: {regime_label}(T={t_score}/V={v_score})")

        except Exception as e:
            logger.error(f"大盘策略分析失败: {e}", exc_info=True)

    # ------ 策略回测辅助方法 ------

    def _backtest_rsi(self, df) -> float:
        """RSI策略: RSI_6 < 30 买入，> 70 卖出，返回累计收益率(%)"""
        returns = 0.0
        entry_price = None
        for i in range(len(df)):
            row = df.iloc[i]
            close = float(row.get('close', 0) or 0)
            rsi = float(row.get('rsi_6', 50) or 50)
            if entry_price is None and rsi < 30 and close > 0:
                entry_price = close
            elif entry_price is not None and rsi > 70 and close > 0:
                returns += (close - entry_price) / entry_price * 100
                entry_price = None
        if entry_price is not None and len(df) > 0:
            last = float(df.iloc[-1].get('close', entry_price) or entry_price)
            returns += (last - entry_price) / entry_price * 100
        return returns

    def _backtest_macd(self, df) -> float:
        """MACD策略: 金叉买入，死叉卖出，返回累计收益率(%)"""
        returns = 0.0
        entry_price = None
        for i in range(1, len(df)):
            cur, prev = df.iloc[i], df.iloc[i - 1]
            close = float(cur.get('close', 0) or 0)
            macd = float(cur.get('macd', 0) or 0)
            sig = float(cur.get('macd_signal', 0) or 0)
            prev_macd = float(prev.get('macd', 0) or 0)
            prev_sig = float(prev.get('macd_signal', 0) or 0)
            if entry_price is None and macd > sig and prev_macd <= prev_sig and close > 0:
                entry_price = close
            elif entry_price is not None and macd < sig and prev_macd >= prev_sig and close > 0:
                returns += (close - entry_price) / entry_price * 100
                entry_price = None
        if entry_price is not None and len(df) > 0:
            last = float(df.iloc[-1].get('close', entry_price) or entry_price)
            returns += (last - entry_price) / entry_price * 100
        return returns

    def _backtest_ma(self, df) -> float:
        """均线策略: 价格上穿MA20买入，下穿卖出，返回累计收益率(%)"""
        returns = 0.0
        entry_price = None
        for i in range(1, len(df)):
            cur, prev = df.iloc[i], df.iloc[i - 1]
            close = float(cur.get('close', 0) or 0)
            ma20 = float(cur.get('ma_20', close) or close)
            prev_close = float(prev.get('close', 0) or 0)
            prev_ma20 = float(prev.get('ma_20', prev_close) or prev_close)
            if entry_price is None and close > ma20 and prev_close <= prev_ma20 and close > 0:
                entry_price = close
            elif entry_price is not None and close < ma20 and prev_close >= prev_ma20 and close > 0:
                returns += (close - entry_price) / entry_price * 100
                entry_price = None
        if entry_price is not None and len(df) > 0:
            last = float(df.iloc[-1].get('close', entry_price) or entry_price)
            returns += (last - entry_price) / entry_price * 100
        return returns

    def _backtest_boll(self, df) -> float:
        """布林带策略: 触及下轨买入，触及上轨卖出，返回累计收益率(%)"""
        returns = 0.0
        entry_price = None
        for i in range(len(df)):
            row = df.iloc[i]
            close = float(row.get('close', 0) or 0)
            boll_lower = float(row.get('boll_lower', 0) or 0)
            boll_upper = float(row.get('boll_upper', 0) or 0)
            if entry_price is None and boll_lower > 0 and close < boll_lower and close > 0:
                entry_price = close
            elif entry_price is not None and boll_upper > 0 and close > boll_upper and close > 0:
                returns += (close - entry_price) / entry_price * 100
                entry_price = None
        if entry_price is not None and len(df) > 0:
            last = float(df.iloc[-1].get('close', entry_price) or entry_price)
            returns += (last - entry_price) / entry_price * 100
        return returns

    def _get_current_signal(self, strategy: str, latest, prev) -> str:
        """根据策略名称和最新指标值返回当前操作建议"""
        if strategy == 'RSI策略':
            rsi = float(latest.get('rsi_6', 50) or 50)
            if rsi < 30:
                return '建议买入'
            elif rsi > 70:
                return '建议卖出'
            return '建议持有'
        elif strategy == 'MACD策略':
            macd = float(latest.get('macd', 0) or 0)
            sig = float(latest.get('macd_signal', 0) or 0)
            prev_macd = float(prev.get('macd', 0) or 0)
            prev_sig = float(prev.get('macd_signal', 0) or 0)
            if macd > sig and prev_macd <= prev_sig:
                return '建议买入'
            elif macd < sig and prev_macd >= prev_sig:
                return '建议卖出'
            return '持有观望' if macd > sig else '观望'
        elif strategy == '均线策略':
            close = float(latest.get('close', 0) or 0)
            ma5 = float(latest.get('ma_5', close) or close)
            ma20 = float(latest.get('ma_20', close) or close)
            if close > ma5 > ma20:
                return '持有/加仓'
            elif close < ma5 < ma20:
                return '建议卖出'
            return '观望'
        elif strategy == '布林带策略':
            close = float(latest.get('close', 0) or 0)
            boll_lower = float(latest.get('boll_lower', 0) or 0)
            boll_upper = float(latest.get('boll_upper', 0) or 0)
            if boll_lower > 0 and close < boll_lower:
                return '建议买入'
            elif boll_upper > 0 and close > boll_upper:
                return '建议卖出'
            return '持有观望'
        elif strategy == '综合策略':
            rsi = float(latest.get('rsi_6', 50) or 50)
            macd_hist = float(latest.get('macd_histogram', 0) or 0)
            if rsi < 35 and macd_hist > 0:
                return '建议买入'
            elif rsi > 65 and macd_hist < 0:
                return '建议卖出'
            return '持有观望'
        return '观望'

    def _send_analysis_report(self, results: List[Dict]):
        """发送分析报告到微信"""
        now = datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M')
        
        title = f"📊 量化分析报告 ({now})"
        
        content = f"## 量化交易系统分析报告\n\n**时间**: {now}\n\n"
        
        # 买入建议
        buy_signals = [r for r in results if r['decision'].action == 'buy']
        if buy_signals:
            content += "### 📈 买入建议\n\n"
            for r in buy_signals:
                d = r['decision']
                content += f"- **{r['name']}({r['code']})**\n"
                content += f"  - 建议仓位: {d.position_ratio*100:.0f}%\n"
                content += f"  - 置信度: {d.confidence*100:.0f}%\n"
                content += f"  - 理由: {d.reasoning[:100]}...\n\n"
        
        # 卖出建议
        sell_signals = [r for r in results if r['decision'].action == 'sell']
        if sell_signals:
            content += "### 📉 卖出建议\n\n"
            for r in sell_signals:
                d = r['decision']
                content += f"- **{r['name']}({r['code']})**\n"
                content += f"  - 建议仓位: {d.position_ratio*100:.0f}%\n"
                content += f"  - 置信度: {d.confidence*100:.0f}%\n"
                content += f"  - 理由: {d.reasoning[:100]}...\n\n"
        
        # 持有建议
        hold_signals = [r for r in results if r['decision'].action == 'hold']
        if hold_signals:
            content += "### ➡️ 持有观望\n\n"
            for r in hold_signals[:5]:  # 只显示前5个
                d = r['decision']
                content += f"- {r['name']}({r['code']})\n"
        
        # 发送通知
        notification_manager.send_markdown_message(title, content)
        logger.info("分析报告已发送")
    
    def get_custom_tasks(self) -> List[Dict]:
        """返回自定义任务列表（来自配置）"""
        return self.config.get('custom_tasks', [])

    def add_custom_task(self, time_str: str, content: str = '', name: Optional[str] = None, types: Optional[List[str]] = None, enabled: bool = True, skip_non_trading_day: bool = True) -> Dict:
        """添加一个自定义任务并持久化
        types: 列表, 可选的值包括 'data_update', 'ai_report', 'strategy_alert'
        name: 任务显示名称
        content: 任务描述/内容 (可选，前端已移除)
        skip_non_trading_day: 如果为 True，则在非交易日跳过执行
        """
        import uuid
        task_id = uuid.uuid4().hex[:8]
        if types is None:
            types = ['data_update']
        task_name = name or (content[:30] if content else f'任务-{task_id}')
        task = {
            'id': task_id,
            'time': time_str,
            'name': task_name,
            'content': content,
            'types': list(types),
            'enabled': bool(enabled),
            'skip_non_trading_day': bool(skip_non_trading_day)
        }
        self.config.setdefault('custom_tasks', []).append(task)
        self._save_config()
        if self.is_running and task.get('enabled', True):
            try:
                self._schedule_custom_task(task)
            except Exception as e:
                logger.error(f"调度自定义任务失败: {e}")
        return task

    def update_custom_task(self, task_id: str, time_str: Optional[str] = None, content: Optional[str] = None, enabled: Optional[bool] = None, name: Optional[str] = None, types: Optional[List[str]] = None, skip_non_trading_day: Optional[bool] = None) -> bool:
        tasks = self.config.setdefault('custom_tasks', [])
        for t in tasks:
            if t.get('id') == task_id:
                if time_str is not None:
                    t['time'] = time_str
                if content is not None:
                    t['content'] = content
                if name is not None:
                    t['name'] = name
                if types is not None:
                    t['types'] = list(types)
                if enabled is not None:
                    t['enabled'] = bool(enabled)
                if skip_non_trading_day is not None:
                    t['skip_non_trading_day'] = bool(skip_non_trading_day)
                self._save_config()
                # reschedule job
                try:
                    job_id = f"custom_task_{task_id}"
                    if self.scheduler:
                        try:
                            self.scheduler.remove_job(job_id)
                        except Exception:
                            pass
                    if t.get('enabled', True) and self.is_running:
                        self._schedule_custom_task(t)
                except Exception as e:
                    logger.error(f"更新自定义任务调度失败: {e}")
                return True
        return False

    def remove_custom_task(self, task_id: str) -> bool:
        """移除自定义任务，支持带或不带 custom_ 前缀的 id"""
        if isinstance(task_id, str) and task_id.startswith('custom_'):
            task_id = task_id.replace('custom_', '', 1)

        tasks = self.config.get('custom_tasks', [])
        for i, t in enumerate(list(tasks)):
            if t.get('id') == task_id:
                # unschedule
                job_id = f"custom_task_{task_id}"
                try:
                    if getattr(self, 'scheduler', None):
                        try:
                            self.scheduler.remove_job(job_id)
                        except Exception:
                            pass
                except Exception:
                    pass
                try:
                    tasks.pop(i)
                except Exception:
                    # fallback: remove by identity
                    try:
                        tasks.remove(t)
                    except Exception:
                        pass
                self._save_config()
                logger.info(f"已移除自定义任务 {task_id}")
                return True
        logger.debug(f"未找到自定义任务 {task_id} 以移除")
        return False

    def schedule_custom_tasks(self):
        """将所有自定义任务添加到 APScheduler"""
        if not self.scheduler:
            logger.debug('无可用 scheduler，跳过自定义任务调度')
            return
        # remove existing custom jobs first
        for job in list(self.scheduler.get_jobs()):
            if job.id.startswith('custom_task_'):
                try:
                    self.scheduler.remove_job(job.id)
                except Exception:
                    pass
        for task in self.get_custom_tasks():
            if task.get('enabled', True):
                self._schedule_custom_task(task)

    def _schedule_custom_task(self, task: Dict):
        """调度单个自定义任务（内部）"""
        time_str = task.get('time', '')
        try:
            hh, mm = map(int, time_str.split(':'))
        except Exception as e:
            logger.error(f"无法解析自定义任务时间 {time_str}: {e}")
            return
        job_id = f"custom_task_{task.get('id')}"
        try:
            # 不在 CronTrigger 层面限制星期，由应用层 skip_non_trading_day 标志控制
            self.scheduler.add_job(self.run_custom_task, CronTrigger(hour=hh, minute=mm), args=[task.get('id')], id=job_id, replace_existing=True)
            logger.info(f"已调度自定义任务 [{task.get('id')}]: {time_str} -> {task.get('name')[:30]}")
        except Exception as e:
            logger.error(f"添加自定义任务失败: {e}")

    def run_custom_task(self, task_id: str, manual: bool = False):
        """执行自定义任务（根据 types 调用对应函数）
        
        Args:
            task_id: 任务ID
            manual: 是否手动触发（手动触发跳过非交易日检查）
        """
        tasks = self.config.get('custom_tasks', [])
        task = next((t for t in tasks if t.get('id') == task_id), None)
        if not task or not task.get('enabled', True):
            return
        # 跳过非交易日（如果配置了；手动触发时不跳过）
        if not manual and task.get('skip_non_trading_day', True) and not self.is_trading_day():
            logger.info(f"跳过自定义任务 {task_id}：今天不是交易日")
            nowstr = datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
            self.task_status.setdefault(f"custom_{task_id}", {})
            self.task_status[f"custom_{task_id}"].update({'running': False, 'last_run': nowstr, 'last_result': '跳过：非交易日', 'last_success': None})
            return

        self.task_status.setdefault(f"custom_{task_id}", {})
        self.task_status[f"custom_{task_id}"].update({'running': True, 'start_time': datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S'), 'last_result': '运行中...', 'last_success': None})
        types = task.get('types', ['data_update']) or ['data_update']
        # 确保 data_update 永远最先执行，其余顺序不变
        types = sorted(types, key=lambda t: (0 if t == 'data_update' else 1, types.index(t)))
        results = []
        for ttype in types:
            func = self._custom_type_to_func.get(ttype)
            if not func:
                results.append({'type': ttype, 'status': 'unsupported'})
                continue
            try:
                logger.info(f"执行自定义任务 [{task.get('id')}] 类型: {ttype}")
                func(force=True)  # 交易日检查已在 run_custom_task 顶部处理
                results.append({'type': ttype, 'status': 'ok'})
            except Exception as e:
                logger.error(f"自定义任务子项失败 {task.get('id')} {ttype}: {e}")
                results.append({'type': ttype, 'status': 'error', 'error': str(e)})
        # 构建执行摘要（仅用于任务状态记录；各任务函数自行发送通知内容）
        summary_lines = [f"任务时间: {task.get('time')}"]
        errors = []
        for r in results:
            if r.get('status') == 'ok':
                summary_lines.append(f"- {r['type']}: ✅ 完成")
            elif r.get('status') == 'unsupported':
                summary_lines.append(f"- {r['type']}: ⚠️ 不支持")
            else:
                err_msg = f"- {r['type']}: ❌ 错误: {r.get('error')}"
                summary_lines.append(err_msg)
                errors.append(err_msg)
        summary = "\n".join(summary_lines)
        # 只在有错误时发送通知，避免与任务本身的通知重复
        if errors:
            try:
                title = f"⚠️ 定时任务异常: {task.get('name', task.get('content',''))}"
                notification_manager.send_markdown_message(title, "\n".join(errors))
            except Exception as e:
                logger.error(f"发送任务错误通知失败 {task_id}: {e}")
        try:
            self.task_status[f"custom_{task_id}"].update({'running': False, 'last_run': datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S'), 'last_result': summary, 'last_success': all(r.get('status')=='ok' for r in results)})
            logger.info(f"自定义任务 {task_id} 执行完成")
        except Exception as e:
            logger.error(f"更新任务状态失败 {task_id}: {e}")
            self.task_status[f"custom_{task_id}"].update({'running': False, 'last_run': datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S'), 'last_result': str(e), 'last_success': False})

    def start(self):
        """启动调度器（可多次 start/stop）"""
        if self.is_running:
            logger.warning("调度器已在运行")
            return

        if not self.config.get('enabled', True):
            logger.info("调度器已禁用")
            return

        # always create a fresh BackgroundScheduler so stop() can shutdown fully
        try:
            self.scheduler = BackgroundScheduler(timezone=BEIJING_TZ)
        except Exception as e:
            logger.error(f"创建 APScheduler 实例失败: {e}")
            return

        task_schedule = self.config.get('task_schedule', {}) or {}

        if task_schedule:
            # 多任务独立调度
            for task_name, sched in task_schedule.items():
                if not isinstance(sched, dict):
                    logger.debug(f"跳过非字典任务配置: {task_name}")
                    continue
                if not sched.get('enabled', True):
                    logger.debug(f"任务 {task_name} 已禁用，跳过注册")
                    continue
                if task_name not in self._task_funcs_map:
                    logger.info(f"跳过未知任务配置: {task_name}")
                    continue

                # 支持两种调度字段：hour/minute 或 time 字符串
                hh = None
                mm = None
                if 'hour' in sched and 'minute' in sched:
                    try:
                        hh = int(sched.get('hour'))
                        mm = int(sched.get('minute'))
                    except Exception as e:
                        logger.error(f"任务 {task_name} 的 hour/minute 格式不正确: {e}")
                        continue
                elif 'time' in sched:
                    try:
                        hh, mm = map(int, str(sched.get('time')).split(':'))
                    except Exception as e:
                        logger.error(f"任务 {task_name} 的 time 字段解析失败: {e}")
                        continue
                else:
                    logger.warning(f"任务 {task_name} 缺少调度信息，跳过")
                    continue

                try:
                    self.scheduler.add_job(
                        self._task_funcs_map[task_name],
                        CronTrigger(hour=hh, minute=mm, day_of_week='mon-fri'),
                        id=f'task_{task_name}',
                        replace_existing=True
                    )
                    logger.info(f"已添加任务 [{task_name}]: {hh:02d}:{mm:02d}")
                except Exception as e:
                    logger.error(f"添加任务 {task_name} 失败: {e}")
        else:
            # 向后兼容：使用 afternoon_times 运行全量任务
            afternoon_times = self.config.get('afternoon_times', ['15:30', '15:45'])
            for time_str in afternoon_times:
                try:
                    hour, minute = map(int, time_str.split(':'))
                    self.scheduler.add_job(
                        self.run_daily_tasks,
                        CronTrigger(hour=hour, minute=minute, day_of_week='mon-fri'),
                        id=f'daily_task_{time_str}',
                        replace_existing=True
                    )
                    logger.info(f"已添加定时任务: {time_str}")
                except Exception as e:
                    logger.error(f"添加定时任务失败 {time_str}: {e}")

        # Schedule custom user-defined tasks (if any)
        try:
            self.schedule_custom_tasks()
        except Exception as e:
            logger.error(f"自定义任务调度失败: {e}")

        try:
            self.scheduler.start()
        except Exception as e:
            logger.error(f"启动 scheduler 失败: {e}")
            return

        self.is_running = True
        logger.info("调度器已启动")
    
    def stop(self):
        """停止调度器

        调用 shutdown 后 APScheduler 实例不可重用，因此把 self.scheduler 置为 None，
        下次 start() 时会新建实例。
        """
        if not self.is_running:
            return

        try:
            if self.scheduler:
                # 非阻塞关闭
                self.scheduler.shutdown(wait=False)
        except Exception as e:
            logger.exception(f"停止调度器失败: {e}")
        finally:
            self.scheduler = None
            self.is_running = False
            logger.info("调度器已停止")
    
    def update_config(self, new_config: Dict):
        """更新配置"""
        self.config.update(new_config)
        self._save_config()
        
        # 如果正在运行，重启调度器
        if self.is_running:
            self.stop()
            self.start()
    
    def get_status(self) -> Dict:
        """获取调度器状态"""
        return {
            'is_running': self.is_running,
            'is_trading_day': self.is_trading_day(),
            'config': self.config,
            'next_run_times': self._get_next_run_times()
        }
    
    def _get_next_run_times(self) -> List[str]:
        """获取下次运行时间（已考虑跳过非交易日配置）"""
        if not self.is_running:
            return []

        custom_tasks_map = {f"custom_task_{t.get('id')}": t for t in self.get_custom_tasks()}

        next_times = []
        for job in self.scheduler.get_jobs():
            next_run = job.next_run_time
            if not next_run:
                continue

            # 自定义任务：如果下次执行时间本身落在周末，且配置了跳过非交易日，则推算到下一个工作日
            task_cfg = custom_tasks_map.get(job.id)
            if task_cfg and task_cfg.get('skip_non_trading_day', True):
                next_run = self._next_trading_day_run_time(next_run)

            next_times.append(next_run.strftime('%Y-%m-%d %H:%M:%S'))

        return sorted(next_times)

    def _next_trading_day_run_time(self, dt) -> datetime:
        """从给定时间推算到下一个交易日（跳过周末）"""
        candidate = dt
        while candidate.weekday() >= 5:  # 5=周六, 6=周日
            candidate = candidate + timedelta(days=1)
        return candidate
    
    def run_once(self, force: bool = True):
        """立即运行一次任务（用于测试）
        
        Args:
            force: 如果为 True，跳过交易日检查（用于测试）
        """
        logger.info("手动触发任务执行")
        if force:
            logger.info("[FORCE] 跳过交易日检查，强制执行所有任务")
            self.run_daily_tasks_once()
        else:
            self.run_daily_tasks()

    def _send_failure_alert(self, task_name: str, error: str):
        """任务失败时发送告警通知"""
        if not self.config.get('alert_on_failure', True):
            return
        try:
            label = self._task_labels.get(task_name, task_name)
            now = datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
            notification_manager.send_system_alert(
                f"⚠️ 调度任务失败: {label}",
                f"任务 [{label}] 在 {now} 执行失败\n\n错误信息:\n{error}"
            )
        except Exception as e:
            logger.warning(f"发送失败告警本身出错: {e}")

    def run_single_task(self, task_name: str) -> Dict:
        """手动触发单个任务"""
        if task_name not in self._task_funcs_map:
            return {'success': False, 'error': f'未知任务: {task_name}'}

        if self.task_status.get(task_name, {}).get('running'):
            return {'success': False, 'error': f'任务 {task_name} 正在运行中'}

        self.task_status[task_name] = {
            'running': True,
            'start_time': datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': None,
            'result': '运行中...',
            'success': None,
        }

        def _run():
            try:
                self._task_funcs_map[task_name]()
                self.task_status[task_name].update({
                    'running': False,
                    'end_time': datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S'),
                    'result': '执行成功',
                    'success': True,
                })
            except Exception as e:
                logger.error(f"手动任务 {task_name} 失败: {e}")
                self.task_status[task_name].update({
                    'running': False,
                    'end_time': datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S'),
                    'result': f'执行失败: {str(e)}',
                    'success': False,
                })
                self._send_failure_alert(task_name, str(e))

        thread = Thread(target=_run, daemon=True)
        thread.start()
        return {'success': True, 'message': f'任务 [{self._task_labels.get(task_name, task_name)}] 已在后台启动'}

    def get_task_status(self) -> Dict:
        """获取所有任务的执行状态（基于当前配置，包含自定义任务）"""
        task_schedule = self.config.get('task_schedule', {}) or {}
        result = {}
        # 根据配置中的条目返回状态（删除的任务不会显示）
        for name, sched in task_schedule.items():
            label = self._task_labels.get(name, name)
            status = self.task_status.get(name, {})
            schedule = '--'
            if isinstance(sched, dict):
                if 'hour' in sched and 'minute' in sched:
                    try:
                        schedule = f"{int(sched.get('hour')):02d}:{int(sched.get('minute')):02d}"
                    except Exception:
                        schedule = sched.get('time', '--')
                elif 'time' in sched:
                    schedule = sched.get('time')
            result[name] = {
                'label': label,
                'enabled': bool(sched.get('enabled', False)) if isinstance(sched, dict) else False,
                'schedule': schedule,
                'skip_non_trading_day': bool(sched.get('skip_non_trading_day', True)) if isinstance(sched, dict) else True,
                'running': status.get('running', False),
                'last_run': status.get('start_time') or status.get('last_run'),
                'last_end': status.get('end_time'),
                'last_result': status.get('result'),
                'last_success': status.get('success'),
            }
        # 自定义任务也加入到状态列表中，key 使用 custom_<id>
        for t in self.get_custom_tasks():
            tid = t.get('id')
            key = f"custom_{tid}"
            status = self.task_status.get(key, {})
            display_name = t.get('name') or t.get('content', '')
            result[key] = {
                'label': f"自定义: {display_name[:20]}",
                'name': display_name,
                'content': t.get('content', ''),
                'types': t.get('types', []),
                'enabled': bool(t.get('enabled', True)),
                'schedule': t.get('time', '--'),
                'skip_non_trading_day': bool(t.get('skip_non_trading_day', True)),
                'running': status.get('running', False),
                'last_run': status.get('last_run') or status.get('start_time'),
                'last_end': status.get('end_time'),
                'last_result': status.get('last_result') or status.get('result'),
                'last_success': status.get('last_success') if 'last_success' in status else status.get('success'),
            }
        return result


# 全局调度器实例
scheduler = TradingScheduler()


def start_scheduler():
    """启动调度器（供外部调用）"""
    scheduler.start()
    
    try:
        # 保持程序运行
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.stop()


if __name__ == '__main__':
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    start_scheduler()
