"""
回测引擎模块
支持指定股票和策略的历史数据回测
"""

import os
import json
import logging
from typing import List, Dict, Optional, Any, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

from .config_manager import config
from .stock_manager import stock_manager
from .data_source import unified_data
from .strategy import QuantStrategy, StrategyDecision, strategy_manager
from .indicators import technical_indicators

logger = logging.getLogger(__name__)


@dataclass
class TradeRecord:
    """交易记录"""
    date: str
    action: str  # buy/sell
    code: str
    price: float
    shares: int
    amount: float
    commission: float
    reason: str


@dataclass
class BacktestResult:
    """回测结果"""
    code: str
    strategy_name: str
    start_date: str
    end_date: str
    initial_capital: float
    final_capital: float
    total_return: float
    total_return_pct: float       # 以总资金为基准的收益率
    deployed_return_pct: float    # 以策略最大仓位上限资金为基准的收益率
    max_position_ratio: float     # 策略最大仓位上限
    annual_return: float
    max_drawdown: float
    max_drawdown_pct: float
    sharpe_ratio: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    avg_profit: float
    avg_loss: float
    profit_factor: float
    trades: List[TradeRecord] = field(default_factory=list)
    daily_returns: pd.DataFrame = None
    equity_curve: pd.DataFrame = None


class BacktestEngine:
    """回测引擎"""

    # 周/月指标需要从每个周/月棒转发到对应的每个交易日
    _WM_COLS = [
        'close', 'rsi_6', 'rsi_12', 'rsi_24',
        'macd', 'macd_signal', 'macd_histogram',
        'kdj_k', 'kdj_d', 'kdj_j',
        'boll_upper', 'boll_middle', 'boll_lower', 'boll_position',
        'ma_5', 'ma_20', 'ma_60',
        'volume_ratio', 'wr_14', 'volatility', 'overall_score',
        'rsi6_pct100',  # RSI_6 百分位，自动生成 w_rsi6_pct100 / m_rsi6_pct100
    ]
    _WM_DEFAULTS = {
        'rsi_6': 50, 'rsi_12': 50, 'rsi_24': 50,
        'kdj_k': 50, 'kdj_d': 50, 'kdj_j': 50,
        'boll_position': 0.5, 'volume_ratio': 1, 'wr_14': -50,
    }

    def __init__(self):
        self.config = config.get_backtest_config()
        self.initial_capital = self.config['initial_capital']
        self.commission_rate = self.config['commission_rate']
        self.slippage = self.config['slippage']

    @staticmethod
    def _resample_ohlcv(daily_df: pd.DataFrame, rule: str) -> pd.DataFrame:
        """将日线 OHLCV 重采样为周线(W-FRI)或月线(ME)。每根K线日期=周/月最后一个交易日。"""
        df = daily_df.copy()
        df = df.set_index('date').sort_index()
        agg = {
            'open':   'first',
            'high':   'max',
            'low':    'min',
            'close':  'last',
            'volume': 'sum',
        }
        # 只用实际存在的列
        agg = {k: v for k, v in agg.items() if k in df.columns}
        if 'amount' in df.columns:
            agg['amount'] = 'sum'
        if 'pct_chg' in df.columns:
            # period return: compound
            agg['pct_chg'] = 'sum'
        resampled = df.resample(rule, label='right', closed='right').agg(agg).dropna(how='all')
        resampled.index.name = 'date'
        return resampled.reset_index()

    def _merge_weekly_monthly(self, code: str, daily_df: pd.DataFrame) -> pd.DataFrame:
        """将周线/月线指标（通过对日线重采样计算）前向填充合并到日线DataFrame，添加 w_/m_ 前缀列。"""
        if daily_df.empty:
            return daily_df

        result = daily_df.copy().sort_values('date').reset_index(drop=True)

        # rule: W-FRI = 周线(每周五收盘), ME = 月线(每月最后交易日)
        for rule, pfx in [('W-FRI', 'w_'), ('ME', 'm_')]:
            try:
                resampled = self._resample_ohlcv(result, rule)
                if resampled.empty:
                    continue

                # 计算技术指标（在周/月K线上）
                wdf = technical_indicators.calculate_all_indicators_from_df(resampled)
                if wdf is None or wdf.empty:
                    continue

                wdf['date'] = pd.to_datetime(wdf['date'])
                wdf = wdf.dropna(subset=['date']).sort_values('date').reset_index(drop=True)

                # 补充 boll_position / overall_score（如缺失）
                if 'boll_position' not in wdf.columns and {'boll_upper','boll_lower','close'}.issubset(wdf.columns):
                    _rng = wdf['boll_upper'] - wdf['boll_lower']
                    wdf['boll_position'] = (
                        (wdf['close'] - wdf['boll_lower']) / _rng.replace(0, np.nan)
                    ).clip(0, 1).fillna(0.5)
                if 'overall_score' not in wdf.columns:
                    _sc = pd.Series(0.0, index=wdf.index)
                    if 'rsi_6' in wdf.columns:
                        _sc += wdf['rsi_6'].between(30, 70).astype(float) * 10
                    if 'macd_histogram' in wdf.columns:
                        _sc += wdf['macd_histogram'].fillna(0) * 10
                    if 'close' in wdf.columns and 'ma_20' in wdf.columns:
                        _sc += ((wdf['close'] > wdf['ma_20']).astype(float) * 2 - 1) * 10
                    if 'kdj_j' in wdf.columns:
                        _sc += (wdf['kdj_j'].fillna(50) - 50) * 0.3
                    wdf['overall_score'] = _sc.clip(-100, 100).round(2)

                # 仅取需要的列并重命名
                available = [c for c in self._WM_COLS if c in wdf.columns]
                sub = wdf[['date'] + available].rename(
                    columns={c: f'{pfx}{c}' for c in available}
                )

                # merge_asof：每个日线行取最近（≤当日）的周/月棒值
                result = pd.merge_asof(result, sub, on='date', direction='backward')
            except Exception as exc:
                logger.debug(f'合并{rule}指标失败({code}): {exc}')

        return result

    def run_backtest(self, code: str, strategy: QuantStrategy,
                     start_date: str, end_date: str,
                     initial_capital: float = None,
                     per_trade_ratio: float = None,
                     progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
                     precomputed_df: pd.DataFrame = None) -> BacktestResult:
        """
        运行回测（增强版：增加详细调试日志以便定位失败）。
        新增参数：
          per_trade_ratio  — 每笔交易固定占初始总资金的比例（如 0.1 = 10%）。
                             设置后每次买入/卖出金额相同，忽略策略规则中的 position_ratio。
          progress_callback— 回测过程中推送进度信息的回调函数。
          precomputed_df   — 预计算好的含全部指标的 DataFrame（跳过数据获取/指标计算）。
                             调用方需保证 df 已包含日线+周线/月线指标、基准相对强弱等列。
        """
        if initial_capital is None:
            initial_capital = self.initial_capital
        # 每笔固定交易比例：覆盖策略规则中的 position_ratio，保证每次买卖金额相同
        _per_trade = per_trade_ratio  # None 表示沿用策略规则中各自的 position_ratio
        
        # ── 数据准备：若提供了 precomputed_df 则跳过所有获取/计算步骤 ──
        if precomputed_df is not None:
            df = precomputed_df.copy()
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            logger.info(f"使用预计算DataFrame回测 {code}, 策略 {strategy.name}, 行数={len(df)}")
        else:
            stock = stock_manager.get_stock_by_code(code)
        
            # 获取历史数据
            logger.info(f"开始回测 {code} 使用策略 {strategy.name}, 区间: {start_date} - {end_date}")
            df = unified_data.get_historical_data(code, start_date, end_date)
            
            if df.empty:
                logger.error(f"无法获取 {code} 的历史数据 (start={start_date}, end={end_date})")
                raise ValueError(f"无法获取 {code} 的历史数据")
            logger.info(f"获取历史数据行数: {len(df)}, 列: {list(df.columns)}")
            logger.debug(f"历史数据样例: {df.head(3).to_dict(orient='records')}")
            
            # 计算技术指标
            try:
                df = technical_indicators.calculate_all_indicators(code, start_date, end_date)
            except Exception as e:
                logger.exception(f"计算技术指标时抛出异常: {e}")
                raise
            
            if df.empty:
                logger.error(f"计算技术指标返回空 DataFrame for {code}")
                raise ValueError(f"无法计算 {code} 的技术指标")
            logger.info(f"指标DataFrame大小: {df.shape}, 列: {list(df.columns)}")
            logger.debug(f"指标样例: {df.head(3).to_dict(orient='records')}")

            # 确保 boll_position 存在
            if 'boll_position' not in df.columns and 'boll_upper' in df.columns:
                _rng = df['boll_upper'] - df['boll_lower']
                df['boll_position'] = (
                    (df['close'] - df['boll_lower']) / _rng.replace(0, np.nan)
                ).clip(0, 1).fillna(0.5)

            # 合并周线/月线指标（添加 w_/m_ 前缀列）
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            df = self._merge_weekly_monthly(code, df)

            # 按日期范围过滤（技术指标需要回看数据，计算完再过滤）
            if 'date' in df.columns:
                try:
                    sd = pd.to_datetime(str(start_date), format='%Y%m%d', errors='coerce')
                    ed = pd.to_datetime(str(end_date), format='%Y%m%d', errors='coerce') if end_date else None
                    if pd.notna(sd):
                        df = df[df['date'] >= sd]
                    if ed is not None and pd.notna(ed):
                        df = df[df['date'] <= ed]
                    df = df.reset_index(drop=True)
                    logger.info(f"按日期过滤后数据: {len(df)} 行, {start_date} ~ {end_date}")
                except Exception as e:
                    logger.warning(f"日期过滤失败: {e}")

            if df.empty:
                raise ValueError(f"过滤日期范围后数据为空 {code} ({start_date}-{end_date})")

            # ── 加载基准指数（上证综指）并计算超额收益 ───────────────────────────────
            _BENCHMARK = '000001.SH'
            try:
                _idx_raw = unified_data.get_historical_data(_BENCHMARK, start_date, end_date)
                if _idx_raw is not None and not _idx_raw.empty:
                    _idx = _idx_raw.copy()
                    _idx['date'] = pd.to_datetime(_idx['date'])
                    _idx = _idx.sort_values('date').reset_index(drop=True)
                    _idx['idx_pct_chg'] = _idx['close'].pct_change() * 100
                    for _n in [5, 10, 20, 60]:
                        _idx[f'idx_ret_{_n}'] = _idx['close'].pct_change(_n) * 100
                        df[f'stock_ret_{_n}'] = df['close'].pct_change(_n) * 100
                    _idx_cols = ['date', 'idx_pct_chg'] + [f'idx_ret_{_n}' for _n in [5, 10, 20, 60]]
                    df = pd.merge_asof(df.sort_values('date'), _idx[_idx_cols], on='date', direction='backward')
                    for _n in [5, 10, 20, 60]:
                        df[f'rel_strength_{_n}'] = df[f'stock_ret_{_n}'] - df[f'idx_ret_{_n}']
                    logger.info(f"已加载基准指数 {_BENCHMARK}，计算超额收益完成")
                else:
                    raise ValueError("指数数据为空")
            except Exception as _bex:
                logger.warning(f"加载基准指数失败，相对强弱指标默认为0: {_bex}")
                for _n in [5, 10, 20, 60]:
                    df[f'rel_strength_{_n}'] = 0.0
                    df[f'idx_ret_{_n}'] = 0.0
                df['idx_pct_chg'] = 0.0

        capital = initial_capital
        position = 0  # 持仓股数
        trades = []
        equity_curve = []
        # 策略允许的最大总仓位投入上限（按成本计，避免随价格下跌反复补仓）
        _max_pos_ratio = getattr(strategy, 'max_position_ratio', 1.0) or 1.0
        _max_position_value = initial_capital * _max_pos_ratio
        _cost_basis = 0.0  # 当前持仓的实际投入成本（买入总额，不含已卖出部分）
        
        # 进度/性能度量
        import time as _time
        total_rows = len(df)
        start_time = _time.time()
        last_log_time = start_time
        log_interval = max(1, total_rows // 20)  # 每5%记录一次
        # 发送初始进度
        try:
            if progress_callback is not None:
                progress_callback({'processed': 0, 'total': total_rows, 'elapsed': 0, 'eta': None, 'status': 'running'})
        except Exception:
            logger.exception('初始 progress_callback 调用失败')
        
        # 局部缓存配置以减少属性查找开销
        slippage = self.slippage
        commission_rate = self.commission_rate
        
        # 遍历每个交易日
        for idx, row in df.iterrows():
            try:
                date = row['date']
                price = row['close']
            except Exception as e:
                logger.exception(f"在遍历行时无法读取 date/close: idx={idx}, row={row}\n错误: {e}")
                raise
            
            # 记录进度并估算剩余时间
            try:
                if (idx + 1) % log_interval == 0 or (idx + 1) == total_rows:
                    now = _time.time()
                    elapsed = now - start_time
                    processed = idx + 1
                    remaining = max(0, total_rows - processed)
                    est_total = elapsed / processed * total_rows if processed > 0 else 0
                    eta = est_total - elapsed
                    logger.info(f"回测进度: {processed}/{total_rows} ({processed/total_rows:.1%}), 已耗时 {elapsed:.1f}s, 预计剩余 {eta:.1f}s")
                    # 尝试通过回调推送进度
                    try:
                        if progress_callback is not None:
                            progress_callback({'processed': processed, 'total': total_rows, 'elapsed': elapsed, 'eta': eta, 'status': 'running'})
                    except Exception:
                        logger.exception('progress_callback 调用失败')
            except Exception:
                pass
            
            # 获取当前指标（NaN值替换为默认值）
            def _safe(val, default):
                """Return default if val is NaN"""
                try:
                    if pd.isna(val):
                        return default
                except (TypeError, ValueError):
                    pass
                return val
            
            indicators = {
                # ── 价格 ─────────────────────────────────────────────
                'price':  price,
                'close':  price,
                'open':   _safe(row.get('open'),   price),
                'high':   _safe(row.get('high'),   price),
                'low':    _safe(row.get('low'),    price),
                'volume': _safe(row.get('volume'), 0),
                'amount': _safe(row.get('amount'), 0),
                'pct_chg':_safe(row.get('pct_chg'), 0),
                'change': _safe(row.get('change'),  0),
                # ── 基本面 ───────────────────────────────────────────
                'pe_ttm': _safe(row.get('pe_ttm'), 0),
                'pb':     _safe(row.get('pb'),     0),
                # ── 百分位指标 ────────────────────────────────────────
                'rsi6_pct100':  _safe(row.get('rsi6_pct100'),  50),
                'pettm_pct10y': _safe(row.get('pettm_pct10y'), 50),
                # ── RSI ──────────────────────────────────────────────
                'rsi_6':  _safe(row.get('rsi_6'),  50),
                'rsi_12': _safe(row.get('rsi_12'), 50),
                'rsi_24': _safe(row.get('rsi_24'), 50),
                # ── MACD ─────────────────────────────────────────────
                'macd':           _safe(row.get('macd'),           0),
                'macd_signal':    _safe(row.get('macd_signal'),    0),
                'macd_histogram': _safe(row.get('macd_histogram'), 0),
                # ── KDJ ──────────────────────────────────────────────
                'kdj_k': _safe(row.get('kdj_k'), 50),
                'kdj_d': _safe(row.get('kdj_d'), 50),
                'kdj_j': _safe(row.get('kdj_j'), 50),
                # ── 布林带 ───────────────────────────────────────────
                'boll_upper':    _safe(row.get('boll_upper'),    0),
                'boll_middle':   _safe(row.get('boll_middle'),   0),
                'boll_lower':    _safe(row.get('boll_lower'),    0),
                'boll_position': _safe(row.get('boll_position'), 0.5),
                # ── 其他技术 ─────────────────────────────────────────
                'wr_14':         _safe(row.get('wr_14'),         -50),
                'volatility':    _safe(row.get('volatility'),    0),
                'volume_ratio':  _safe(row.get('volume_ratio'),  1),
                'overall_score': _safe(row.get('overall_score'), 0),
                # ── 新闻（回测无历史新闻，给安全默认值） ─────────────
                'news_sentiment': 0,
                'news_count':     0,
                'news_positive':  0,
                # ── 大盘相对强弱 ──────────────────────────────────────
                'rel_strength_5':  _safe(row.get('rel_strength_5'),  0.0),
                'rel_strength_10': _safe(row.get('rel_strength_10'), 0.0),
                'rel_strength_20': _safe(row.get('rel_strength_20'), 0.0),
                'rel_strength_60': _safe(row.get('rel_strength_60'), 0.0),
                'idx_pct_chg':     _safe(row.get('idx_pct_chg'),     0.0),
                'idx_ret_5':       _safe(row.get('idx_ret_5'),        0.0),
                'idx_ret_10':      _safe(row.get('idx_ret_10'),       0.0),
                'idx_ret_20':      _safe(row.get('idx_ret_20'),       0.0),
                'idx_ret_60':      _safe(row.get('idx_ret_60'),       0.0),
            }
            # MA 双格式支持
            for n in [5, 10, 20, 60, 120, 250]:
                val = _safe(row.get(f'ma_{n}', row.get(f'ma{n}')), 0)
                indicators[f'ma_{n}'] = val
                indicators[f'ma{n}']  = val
            # 周线 / 月线指标（已由 _merge_weekly_monthly 合并入 df）
            for _pfx in ('w_', 'm_'):
                for _col, _def in self._WM_DEFAULTS.items():
                    indicators[f'{_pfx}{_col}'] = _safe(row.get(f'{_pfx}{_col}'), _def)
                for _col in self._WM_COLS:
                    if f'{_pfx}{_col}' not in indicators:
                        indicators[f'{_pfx}{_col}'] = _safe(row.get(f'{_pfx}{_col}'), 0)
            
            # 执行策略
            try:
                decision = self._execute_strategy(strategy, code, indicators)
            except Exception as e:
                logger.exception(f"执行策略时出错 idx={idx}, date={date}, indicators={indicators}: {e}")
                # 记录失败行到文件以便离线排查
                try:
                    dump_path = os.path.join(config.get('data_storage.data_dir', './data'), f'backtest_error_{code}_{idx}.json')
                    with open(dump_path, 'w', encoding='utf-8') as f:
                        json.dump({'idx': idx, 'date': str(date), 'indicators': indicators}, f, ensure_ascii=False, indent=2)
                    logger.info(f"已将失败行写入 {dump_path}")
                except Exception:
                    logger.exception('写入失败行文件失败')
                raise
            
            # 处理交易信号
            if decision.action == "buy" and decision.position_ratio > 0:
                # 每笔买入金额 = 初始总资金 × 固定比例（忽略规则中的 position_ratio）
                trade_ratio = _per_trade if _per_trade is not None else decision.position_ratio
                buy_amount = initial_capital * trade_ratio

                # 强制执行最大仓位上限：已投入成本 + 本次买入不得超过上限（按成本计，防止跌价后反复补仓）
                remaining_room = _max_position_value - _cost_basis
                if remaining_room <= 0:
                    # 已达仓位上限，本日跳过买入
                    buy_amount = 0
                else:
                    buy_amount = min(buy_amount, remaining_room)

                # 考虑滑点
                buy_price = price * (1 + self.slippage)

                # 严格整手（100股倍数），不足1手则放弃本次买入
                shares = int(buy_amount / buy_price / 100) * 100

                if shares > 0:
                    actual_amount = shares * buy_price
                    commission = actual_amount * self.commission_rate
                    total_cost = actual_amount + commission

                    # 资金不足时跳过，不强行买入
                    if total_cost <= capital:
                        capital -= total_cost
                        position += shares
                        _cost_basis += actual_amount  # 记录投入成本

                        trades.append(TradeRecord(
                            date=date,
                            action="buy",
                            code=code,
                            price=buy_price,
                            shares=shares,
                            amount=actual_amount,
                            commission=commission,
                            reason=decision.reasoning
                        ))
            
            elif decision.action == "sell" and decision.position_ratio > 0 and position > 0:
                # 每笔卖出金额 = 初始总资金 × 固定比例对应的股数，超过持仓时全部卖出
                try:
                    sell_price = price * (1 - self.slippage)
                    trade_ratio = _per_trade if _per_trade is not None else decision.position_ratio
                    if trade_ratio >= 1.0:
                        sell_shares = position  # 全仓清仓
                    else:
                        target_value = initial_capital * trade_ratio
                        desired_shares = int(target_value / sell_price / 100) * 100
                        # 卖出超出持仓时全部卖出
                        sell_shares = min(desired_shares, position)
                        if sell_shares == 0 and position > 0:
                            sell_shares = 0  # 不足1手则跳过
                except Exception as e:
                    logger.exception(f"计算卖出股数失败: position={position}, trade_ratio={trade_ratio}: {e}")
                    sell_shares = 0
                logger.debug(f"卖出计算: position={position}, trade_ratio={trade_ratio}, sell_shares={sell_shares}")
                
                if sell_shares > 0:
                    # 考虑滑点
                    sell_price = price * (1 - self.slippage)
                    actual_amount = sell_shares * sell_price
                    commission = actual_amount * self.commission_rate
                    total_received = actual_amount - commission
                    
                    old_position = position
                    capital += total_received
                    position -= sell_shares
                    # 按比例缩减成本计数，清仓后归零以允许重新建仓
                    if old_position > 0:
                        _cost_basis *= (position / old_position)
                    if position == 0:
                        _cost_basis = 0.0
                    
                    trades.append(TradeRecord(
                        date=date,
                        action="sell",
                        code=code,
                        price=sell_price,
                        shares=sell_shares,
                        amount=actual_amount,
                        commission=commission,
                        reason=decision.reasoning
                    ))
            
            # 记录权益
            equity = capital + position * price
            equity_curve.append({
                'date': date,
                'equity': equity,
                'cash': capital,
                'position_value': position * price,
                'position': position,
                'price': price,
                'signal_action': decision.action,
                'signal_reason': decision.reasoning,
            })
        
        # 回测结束时，若仍有持仓，按最后一日收盘价强制平仓，确保交易记录完整
        if position > 0 and equity_curve:
            last_bar = equity_curve[-1]
            close_price = last_bar['price']
            commission_rate = 0.0003
            commission = close_price * position * commission_rate
            actual_amount = close_price * position - commission
            capital += actual_amount
            trades.append(TradeRecord(
                date=last_bar['date'],
                action="sell",
                code=code,
                price=close_price,
                shares=position,
                amount=actual_amount,
                commission=commission,
                reason="回测结束强制平仓"
            ))
            position = 0
            _cost_basis = 0.0

        # 计算回测结果
        equity_df = pd.DataFrame(equity_curve)
        
        try:
            final_equity = equity_df['equity'].iloc[-1] if not equity_df.empty else initial_capital
            total_return = final_equity - initial_capital
            total_return_pct = (total_return / initial_capital) * 100
            
            # 以策略最大仓位上限资金为基准的收益率（反映实际使用资金的效率）
            max_pos = getattr(strategy, 'max_position_ratio', 1.0) or 1.0
            deployed_capital = initial_capital * max_pos
            deployed_return_pct = (total_return / deployed_capital) * 100 if deployed_capital > 0 else 0
            
            # 计算年化收益
            days = len(equity_df)
            years = days / 252
            annual_return = ((final_equity / initial_capital) ** (1/years) - 1) * 100 if years > 0 else 0
            
            # 计算最大回撤
            equity_df['cummax'] = equity_df['equity'].cummax()
            equity_df['drawdown'] = (equity_df['equity'] - equity_df['cummax']) / equity_df['cummax']
            max_drawdown_pct = equity_df['drawdown'].min() * 100
            max_drawdown = equity_df['drawdown'].min() * equity_df['cummax'].max()
            
            # 计算夏普比率
            equity_df['daily_return'] = equity_df['equity'].pct_change()
            sharpe_ratio = (equity_df['daily_return'].mean() / equity_df['daily_return'].std()) * np.sqrt(252) if equity_df['daily_return'].std() != 0 else 0
        except Exception as e:
            logger.exception(f"计算回测统计指标时失败: {e}\n equity_df样例: {equity_df.head(5).to_dict(orient='records') if not equity_df.empty else 'empty'}")
            raise
        
        # 统计交易
        buy_trades = [t for t in trades if t.action == "buy"]
        sell_trades = [t for t in trades if t.action == "sell"]
        
        # 计算胜率
        profits = []
        for i, sell in enumerate(sell_trades):
            # 找到对应的买入
            buy_shares = 0
            buy_cost = 0
            for buy in reversed(buy_trades):
                if buy.date <= sell.date and buy_shares < sell.shares:
                    shares = min(buy.shares, sell.shares - buy_shares)
                    buy_shares += shares
                    buy_cost += shares * buy.price
            
            if buy_shares > 0:
                avg_buy_price = buy_cost / buy_shares
                profit = (sell.price - avg_buy_price) / avg_buy_price * 100
                profits.append(profit)
        
        winning_trades = len([p for p in profits if p > 0])
        losing_trades = len([p for p in profits if p <= 0])
        total_trades = len(sell_trades)
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
        
        avg_profit = np.mean([p for p in profits if p > 0]) if winning_trades > 0 else 0
        avg_loss = np.mean([p for p in profits if p <= 0]) if losing_trades > 0 else 0
        
        gross_profit = sum([p for p in profits if p > 0])
        gross_loss = abs(sum([p for p in profits if p <= 0]))
        profit_factor = gross_profit / gross_loss if gross_loss != 0 else float('inf')
        
        result = BacktestResult(
            code=code,
            strategy_name=strategy.name,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            final_capital=final_equity,
            total_return=total_return,
            total_return_pct=total_return_pct,
            deployed_return_pct=deployed_return_pct,
            max_position_ratio=max_pos,
            annual_return=annual_return,
            max_drawdown=max_drawdown,
            max_drawdown_pct=max_drawdown_pct,
            sharpe_ratio=sharpe_ratio,
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            win_rate=win_rate,
            avg_profit=avg_profit,
            avg_loss=avg_loss,
            profit_factor=profit_factor,
            trades=trades,
            equity_curve=equity_df
        )
        try:
            duration_seconds = _time.time() - start_time
            setattr(result, 'duration_seconds', duration_seconds)
            logger.info(f"回测完成: code={code}, strategy={strategy.name}, trades={len(trades)}, duration={duration_seconds:.1f}s")
        except Exception:
            pass
        return result

    def run_backtest_with_df(self, code: str, strategy: QuantStrategy,
                             df: pd.DataFrame,
                             start_date: str, end_date: str,
                             initial_capital: float = None) -> BacktestResult:
        """已废弃：直接委托给 run_backtest(precomputed_df=df)，保留向后兼容。"""
        return self.run_backtest(
            code, strategy, start_date, end_date,
            initial_capital=initial_capital,
            precomputed_df=df,
        )

    def _execute_strategy(self, strategy: QuantStrategy, code: str,
                          indicators: Dict) -> StrategyDecision:
        """执行策略：支持 AND/OR 规则链。"""
        from .strategy import StrategyDecision

        safe_locals = {**indicators, 'abs': abs, 'max': max, 'min': min}

        def eval_cond(cond: str) -> bool:
            try:
                return bool(eval(cond, {"__builtins__": {}}, safe_locals))
            except Exception:
                return False

        def eval_chain(rules) -> bool:
            """按 connector 字段左结合求值整条规则链。"""
            if not rules:
                return False
            result = eval_cond(rules[0].condition)
            for i in range(1, len(rules)):
                conn = getattr(rules[i], 'connector', 'OR').upper()
                nxt = eval_cond(rules[i].condition)
                result = (result and nxt) if conn == 'AND' else (result or nxt)
            return result

        # 正面清单：按操作分组，各自独立求值
        buy_rules  = [r for r in strategy.rules if r.action == 'buy']
        sell_rules = [r for r in strategy.rules if r.action == 'sell']
        buy_fired  = eval_chain(buy_rules)  if buy_rules  else False
        sell_fired = eval_chain(sell_rules) if sell_rules else False

        # 负面清单
        excluded = eval_chain(strategy.exclusion_rules) if strategy.exclusion_rules else False

        if excluded:
            # Find the specific exclusion rule that fired
            exc_reason = next(
                (r.reason or r.condition for r in strategy.exclusion_rules if eval_cond(r.condition)),
                strategy.exclusion_rules[0].reason or strategy.exclusion_rules[0].condition
            )
            return StrategyDecision(code=code, action='hold', position_ratio=0, confidence=0.5,
                                   reasoning=f'排除条件触发: {exc_reason}',
                                   rules_triggered=[],
                                   timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        if buy_fired and not sell_fired:
            action = 'buy'
            position_ratio = min(sum(r.position_ratio for r in buy_rules) / len(buy_rules), strategy.max_position_ratio)
            confidence = 1.0
            reasoning = ' | '.join(r.reason or r.condition for r in buy_rules)
            triggered = [r.condition for r in buy_rules]
        elif sell_fired and not buy_fired:
            action = 'sell'
            position_ratio = min(sum(r.position_ratio for r in sell_rules) / len(sell_rules), strategy.max_position_ratio)
            confidence = 1.0
            reasoning = ' | '.join(r.reason or r.condition for r in sell_rules)
            triggered = [r.condition for r in sell_rules]
        else:
            action = 'hold'
            position_ratio = 0
            confidence = 0.5
            if buy_fired and sell_fired:
                reasoning = '买卖信号冲突'
            elif buy_rules:
                # Find the first buy rule that failed to explain why we didn't buy
                first_failed = next(
                    (r.reason or r.condition for r in buy_rules if not eval_cond(r.condition)),
                    None
                )
                reasoning = f'不满足: {first_failed}' if first_failed else '无买入信号'
            elif sell_rules:
                first_failed = next(
                    (r.reason or r.condition for r in sell_rules if not eval_cond(r.condition)),
                    None
                )
                reasoning = f'不满足: {first_failed}' if first_failed else '无卖出信号'
            else:
                reasoning = '无规则'
            triggered = []

        return StrategyDecision(
            code=code, action=action, position_ratio=position_ratio,
            confidence=confidence, reasoning=reasoning,
            rules_triggered=triggered,
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
    
    def run_multi_stock_backtest(self, codes: List[str], strategy: QuantStrategy,
                                  start_date: str, end_date: str) -> Dict[str, BacktestResult]:
        """
        多股票回测
        
        Args:
            codes: 股票代码列表
            strategy: 策略对象
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            回测结果字典
        """
        results = {}
        
        for code in codes:
            try:
                logger.info(f"正在回测 {code}...")
                result = self.run_backtest(code, strategy, start_date, end_date)
                results[code] = result
            except Exception as e:
                logger.error(f"回测 {code} 失败: {e}")
        
        return results


class BacktestAnalyzer:
    """回测分析器"""
    
    def generate_report(self, result: BacktestResult) -> str:
        """
        生成回测报告
        
        Args:
            result: 回测结果
        
        Returns:
            报告文本
        """
        stock = stock_manager.get_stock_by_code(result.code)
        name = stock.name if stock else result.code
        
        report = f"""
=== {name}({result.code}) 回测报告 ===
策略: {result.strategy_name}
回测区间: {result.start_date} - {result.end_date}

【收益指标】
- 初始资金: ¥{result.initial_capital:,.2f}
- 最终资金: ¥{result.final_capital:,.2f}
- 总收益: ¥{result.total_return:,.2f} ({result.total_return_pct:+.2f}%)
- 年化收益: {result.annual_return:.2f}%
- 夏普比率: {result.sharpe_ratio:.2f}

【风险指标】
- 最大回撤: ¥{result.max_drawdown:,.2f} ({result.max_drawdown_pct:.2f}%)

【交易统计】
- 总交易次数: {result.total_trades}
- 盈利次数: {result.winning_trades}
- 亏损次数: {result.losing_trades}
- 胜率: {result.win_rate:.2f}%
- 平均盈利: {result.avg_profit:.2f}%
- 平均亏损: {result.avg_loss:.2f}%
- 盈亏比: {result.profit_factor:.2f}

【交易明细】
"""
        
        for trade in result.trades[:10]:  # 只显示前10笔交易
            report += f"- {trade.date} {trade.action.upper()}: {trade.shares}股 @ ¥{trade.price:.2f} ({trade.reason[:30]}...)\n"
        
        if len(result.trades) > 10:
            report += f"... 共 {len(result.trades)} 笔交易\n"
        
        return report
    
    def compare_strategies(self, results: Dict[str, BacktestResult]) -> pd.DataFrame:
        """
        比较多个策略的回测结果
        
        Args:
            results: 策略名称到回测结果的字典
        
        Returns:
            比较DataFrame
        """
        comparison = []
        
        for strategy_name, result in results.items():
            comparison.append({
                '策略': strategy_name,
                '总收益': f"{result.total_return_pct:.2f}%",
                '年化收益': f"{result.annual_return:.2f}%",
                '最大回撤': f"{result.max_drawdown_pct:.2f}%",
                '夏普比率': f"{result.sharpe_ratio:.2f}",
                '胜率': f"{result.win_rate:.2f}%",
                '交易次数': result.total_trades,
            })
        
        return pd.DataFrame(comparison)


# 全局实例
backtest_engine = BacktestEngine()
backtest_analyzer = BacktestAnalyzer()
