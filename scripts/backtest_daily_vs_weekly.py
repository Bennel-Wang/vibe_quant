"""
Daily vs Weekly Manipulation Phase Strategy Backtest Comparison

Tests whether the manipulation phase strategy works better on daily or weekly data.
Theory: accumulation/markup/distribution usually operate on weekly timescales,
so weekly signals should be cleaner and more profitable.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
logging.basicConfig(level=logging.WARNING)
logging.getLogger('quant_system.backtest').setLevel(logging.WARNING)

import numpy as np
import pandas as pd
from quant_system.strategy import QuantStrategy
from quant_system.backtest import BacktestEngine


def make_strategy():
    s = QuantStrategy(
        name="Smart Money Phase Tracking",
        description="Track manipulation phases with state machine: accumulate->buy, markup->hold, distribute->sell, dump->sell"
    )
    s.add_rule(condition="manipulation_phase == '吸筹'", action="buy", position_ratio=0.20,
               reason="Accumulation phase", connector="OR")
    s.add_rule(condition="eff_zscore > 0.8 and volume_ratio > 1.2 and rel_price_change_ema5 > 0",
               action="buy", position_ratio=0.10,
               reason="Efficiency breakout + high vol", connector="OR")

    s.add_rule(condition="manipulation_phase == '出货'", action="sell", position_ratio=0.50,
               reason="Distribution phase", connector="OR")
    s.add_rule(condition="manipulation_phase == '砸盘'", action="sell", position_ratio=0.80,
               reason="Dumping phase", connector="OR")
    s.add_rule(condition="manipulation_phase == '无量阴跌' and rel_price_change_ema20 < -0.3",
               action="sell", position_ratio=0.30,
               reason="Low-vol decline + weakness", connector="OR")
    s.max_position_ratio = 0.60
    return s


TEST_STOCKS = [
    ('600519.SH', 'Kweichow Moutai'),
    ('000858.SZ', 'Wuliangye'),
    ('300750.SZ', 'CATL'),
    ('002594.SZ', 'BYD'),
    ('600900.SH', 'Yangtze Power'),
    ('002371.SZ', 'NAURA'),
    ('002572.SZ', 'Sofia'),
    ('002920.SZ', 'Desay SV'),
]


def backtest_freq(code, name, engine, freq, strategy):
    """Run backtest at given frequency. For weekly, precompute indicators on weekly data."""
    try:
        precomputed = None
        if freq == 'week':
            from quant_system.indicators import technical_indicators
            from quant_system.utils.ohlcv import resample_to_weekly
            from quant_system.data_source import unified_data

            # Get daily data, resample to weekly, compute indicators
            df_day = unified_data.get_historical_data(code, '', '20260505')
            if df_day is not None and not df_day.empty:
                if df_day['date'].dtype == 'object':
                    df_day['date'] = pd.to_datetime(df_day['date'].astype(str), format='%Y%m%d', errors='coerce')
                else:
                    df_day['date'] = pd.to_datetime(df_day['date'])
                df_week = resample_to_weekly(df_day)
                # Compute all indicators on weekly data (includes manipulation_phase)
                precomputed = technical_indicators.calculate_all_indicators_from_df(df_week, code=code, freq='week')
                # Merge weekly/monthly indicators
                precomputed = engine._merge_weekly_monthly(code, precomputed)

        result = engine.run_backtest(
            code=code, strategy=strategy,
            start_date='20200101', end_date='20260505',
            initial_capital=1000000,
            precomputed_df=precomputed,
        )
        return {
            'code': code, 'name': name, 'freq': freq,
            'total_return': result.total_return_pct,
            'annual_return': result.annual_return,
            'max_drawdown': result.max_drawdown_pct,
            'sharpe_ratio': result.sharpe_ratio,
            'win_rate': result.win_rate,
            'profit_factor': result.profit_factor,
            'num_trades': len(result.trades),
        }
    except Exception as e:
        import traceback
        print(f"  {name} {freq} ERROR: {e}")
        traceback.print_exc()
        return None


def main():
    strategy = make_strategy()
    print(f"Strategy: {strategy.name}")
    print(f"Rules: {len(strategy.rules)} (buy={sum(1 for r in strategy.rules if r.action=='buy')}, sell={sum(1 for r in strategy.rules if r.action=='sell')})")
    print()

    engine = BacktestEngine()
    all_results = []

    for code, name in TEST_STOCKS:
        print(f"{'='*60}")
        print(f"  {name} ({code})")
        print(f"{'='*60}")

        for freq, freq_label in [('day', 'Daily'), ('week', 'Weekly')]:
            print(f"\n  --- {freq_label} ---")
            r = backtest_freq(code, name, engine, freq, strategy)
            if r:
                all_results.append(r)
                print(f"    Return: {r['total_return']:+.1f}%  Annual: {r['annual_return']:+.1f}%  "
                      f"MaxDD: {r['max_drawdown']:+.1f}%  Sharpe: {r['sharpe_ratio']:.2f}  "
                      f"Win: {r['win_rate']:.1f}%  PF: {r['profit_factor']:.2f}  "
                      f"Trades: {r['num_trades']}")

    # ── Summary Comparison ──
    print(f"\n{'='*100}")
    print("  DAILY vs WEEKLY COMPARISON")
    print(f"{'='*100}")

    daily = {r['code']: r for r in all_results if r['freq'] == 'day'}
    weekly = {r['code']: r for r in all_results if r['freq'] == 'week'}

    print(f"\n{'Stock':<22} {'D-Ret':>8} {'W-Ret':>8} {'D-Sharpe':>9} {'W-Sharpe':>9} {'D-Win':>7} {'W-Win':>7} {'D-PF':>6} {'W-PF':>6} {'D-Tr':>5} {'W-Tr':>5} {'Winner':>6}")
    print("-" * 110)

    w_better = 0
    for code in sorted(daily.keys()):
        dr = daily[code]
        wr = weekly.get(code)
        if not wr:
            continue
        w_win = wr['total_return'] > dr['total_return']
        if w_win:
            w_better += 1
        tag = "WEEKLY" if w_win else "daily"
        pf_d = f"{dr['profit_factor']:.2f}" if dr['profit_factor'] != float('inf') else "inf"
        pf_w = f"{wr['profit_factor']:.2f}" if wr['profit_factor'] != float('inf') else "inf"
        print(f"{dr['name']:<22} "
              f"{dr['total_return']:>7.1f}% {wr['total_return']:>7.1f}% "
              f"{dr['sharpe_ratio']:>8.2f} {wr['sharpe_ratio']:>8.2f} "
              f"{dr['win_rate']:>6.1f}% {wr['win_rate']:>6.1f}% "
              f"{pf_d:>6} {pf_w:>6} "
              f"{dr['num_trades']:>5} {wr['num_trades']:>5}  {tag:>6}")

    d_list = list(daily.values())
    w_list = [weekly[c] for c in daily.keys() if c in weekly]
    print("-" * 110)
    print(f"{'AVERAGE':<22} "
          f"{np.mean([r['total_return'] for r in d_list]):>7.1f}% {np.mean([r['total_return'] for r in w_list]):>7.1f}% "
          f"{np.mean([r['sharpe_ratio'] for r in d_list]):>8.2f} {np.mean([r['sharpe_ratio'] for r in w_list]):>8.2f} "
          f"{np.mean([r['win_rate'] for r in d_list]):>6.1f}% {np.mean([r['win_rate'] for r in w_list]):>6.1f}%")

    print(f"\n  >>> Weekly beats Daily on {w_better}/{len(daily)} stocks")


if __name__ == '__main__':
    main()
