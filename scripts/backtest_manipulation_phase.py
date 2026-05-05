"""
Manipulation Phase Strategy Backtest

Based on relative price change + volume to detect smart money phases:
- Accumulation (buy signal): large volume + small positive return
- Markup (hold): small volume + large positive return
- Distribution (sell signal): large volume + small negative return
- Dumping (panic sell): large volume + large negative return

Usage: python scripts/backtest_manipulation_phase.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger('quant_system.backtest').setLevel(logging.WARNING)

import pandas as pd
import numpy as np

from quant_system.strategy import QuantStrategy
from quant_system.backtest import BacktestEngine

# Strategy definition
STRATEGY = QuantStrategy(
    name="Smart Money Phase Tracking",
    description=(
        "Track smart money manipulation phases using price-volume efficiency. "
        "Accumulation (low return + high volume) -> Buy. "
        "Markup (high return + low volume) -> Hold. "
        "Distribution (low neg return + high volume) -> Sell. "
        "Dumping (high neg return + high volume) -> Panic sell."
    )
)

# Buy rules: accumulation phase
STRATEGY.add_rule(
    condition="manipulation_phase == 'A' or manipulation_phase == 'Accumulation'",  # fallback for both
    action="buy",
    position_ratio=0.15,
    reason="Accumulation: high volume + small positive return",
    connector="OR"
)
# Simplified: use the Chinese phase names from the indicator
STRATEGY.add_rule(
    condition="manipulation_phase == '吸筹'",  # 吸筹
    action="buy",
    position_ratio=0.15,
    reason="Accumulation phase detected",
    connector="OR"
)
STRATEGY.add_rule(
    condition="efficiency > 0.3 and volume_ratio > 1.2 and rel_price_change_ema5 > 0",
    action="buy",
    position_ratio=0.10,
    reason="Positive price-volume efficiency + high volume",
    connector="OR"
)

# Sell rules: distribution / dumping
STRATEGY.add_rule(
    condition="manipulation_phase == '出货'",  # 出货
    action="sell",
    position_ratio=0.50,
    reason="Distribution: high volume + small negative return",
    connector="OR"
)
STRATEGY.add_rule(
    condition="manipulation_phase == '砸盘'",  # 砸盘
    action="sell",
    position_ratio=0.80,
    reason="Dumping: high volume + large negative return",
    connector="OR"
)
STRATEGY.add_rule(
    condition="manipulation_phase == '无量阴跌' and rel_price_change_ema20 < -0.3",  # 无量阴跌
    action="sell",
    position_ratio=0.30,
    reason="Low-vol decline + relative weakness",
    connector="OR"
)

STRATEGY.max_position_ratio = 0.50

# Test stocks
TEST_STOCKS = [
    ('600519.SH', 'Kweichow Moutai'),
    ('000858.SZ', 'Wuliangye'),
    ('300750.SZ', 'CATL'),
    ('002594.SZ', 'BYD'),
    ('600900.SH', 'Yangtze Power'),
    ('002371.SZ', 'NAURA'),
]


def debug_phase_data(code):
    """Check if manipulation_phase column exists in indicators"""
    from quant_system.indicators import technical_indicators
    print(f"\n  [DEBUG] Checking indicator data for {code}...")
    try:
        df_ind = technical_indicators.calculate_all_indicators(code, start_date='20240101', end_date='20260505')
        if df_ind.empty:
            print(f"  [DEBUG] Empty indicator DataFrame")
            return
        if 'manipulation_phase' not in df_ind.columns:
            print(f"  [DEBUG] manipulation_phase column MISSING! Available columns (last 10): {list(df_ind.columns[-10:])}")
            return
        if 'rel_price_change' not in df_ind.columns:
            print(f"  [DEBUG] rel_price_change column MISSING!")
            return

        phase_counts = df_ind['manipulation_phase'].value_counts()
        print(f"  [DEBUG] manipulation_phase distribution: {phase_counts.to_dict()}")
        print(f"  [DEBUG] efficiency range: {df_ind['efficiency'].min():.4f} to {df_ind['efficiency'].max():.4f}")
        print(f"  [DEBUG] rel_price_change_ema5 range: {df_ind['rel_price_change_ema5'].min():.4f} to {df_ind['rel_price_change_ema5'].max():.4f}")
        print(f"  [DEBUG] volume_ratio range: {df_ind['volume_ratio'].min():.4f} to {df_ind['volume_ratio'].max():.4f}")
    except Exception as e:
        print(f"  [DEBUG] Error: {e}")
        import traceback
        traceback.print_exc()


def run_backtest_for_stock(code, name, engine, debug=True):
    """Run backtest for a single stock"""
    print(f"\n{'='*60}")
    print(f"Backtest: {name} ({code})")
    print(f"{'='*60}")

    if debug:
        debug_phase_data(code)

    try:
        result = engine.run_backtest(
            code=code,
            strategy=STRATEGY,
            start_date='20200101',
            end_date='20260505',
            initial_capital=1000000,
        )

        total_ret_pct = result.total_return_pct
        annual_ret = result.annual_return
        max_dd_pct = result.max_drawdown_pct
        print(f"  Initial Capital:   {result.initial_capital:,.0f}")
        print(f"  Final Capital:     {result.final_capital:,.0f}")
        print(f"  Total Return:      {total_ret_pct:+.2f}%")
        print(f"  Annual Return:     {annual_ret:+.2f}%")
        print(f"  Max Drawdown:      {max_dd_pct:+.2f}%")
        print(f"  Sharpe Ratio:      {result.sharpe_ratio:.2f}")
        print(f"  Win Rate:          {result.win_rate:.2f}%")
        print(f"  Profit Factor:     {result.profit_factor:.2f}")
        print(f"  Total Trades:      {len(result.trades)}")

        if result.trades:
            buy_trades = [t for t in result.trades if getattr(t, 'action', '') == 'buy']
            sell_trades = [t for t in result.trades if getattr(t, 'action', '') == 'sell']
            print(f"  Buy Count:         {len(buy_trades)}")
            print(f"  Sell Count:        {len(sell_trades)}")
        else:
            print("  ** NO TRADES! **")

        return {
            'code': code,
            'name': name,
            'total_return': total_ret_pct,
            'annual_return': annual_ret,
            'max_drawdown': max_dd_pct,
            'sharpe_ratio': result.sharpe_ratio,
            'win_rate': result.win_rate,
            'profit_factor': result.profit_factor,
            'num_trades': len(result.trades),
            'alpha': getattr(result, 'alpha', 0),
            'information_ratio': getattr(result, 'information_ratio', 0),
        }

    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    print("=" * 60)
    print("  Smart Money Phase Tracking - Batch Backtest")
    print(f"  Strategy: {STRATEGY.name}")
    n_buy = sum(1 for r in STRATEGY.rules if r.action == 'buy')
    n_sell = sum(1 for r in STRATEGY.rules if r.action == 'sell')
    print(f"  Rules: {len(STRATEGY.rules)} (buy={n_buy}, sell={n_sell})")
    print(f"  Max Position: {STRATEGY.max_position_ratio:.0%}")
    print("=" * 60)

    engine = BacktestEngine()

    results = []
    for code, name in TEST_STOCKS:
        r = run_backtest_for_stock(code, name, engine, debug=False)
        if r:
            results.append(r)

    # Summary
    if results:
        print(f"\n{'='*80}")
        print("  SUMMARY")
        print(f"{'='*80}")
        header = f"{'Stock':<20} {'TotRet':>8} {'AnnRet':>8} {'MaxDD':>8} {'Sharpe':>7} {'WinRate':>8} {'PF':>6} {'Trades':>7}"
        print(header)
        print("-" * 80)
        for r in results:
            print(
                f"{r['name']:<20} "
                f"{r['total_return']:>7.1f}% "
                f"{r['annual_return']:>7.1f}% "
                f"{r['max_drawdown']:>7.1f}% "
                f"{r['sharpe_ratio']:>6.2f} "
                f"{r['win_rate']:>7.1f}% "
                f"{r['profit_factor']:>5.2f} "
                f"{r['num_trades']:>7}"
            )

        avg_return = np.mean([r['total_return'] for r in results])
        avg_sharpe = np.mean([r['sharpe_ratio'] for r in results])
        avg_winrate = np.mean([r['win_rate'] for r in results])
        avg_trades = np.mean([r['num_trades'] for r in results])
        print("-" * 80)
        print(
            f"{'AVERAGE':<20} "
            f"{avg_return:>7.1f}% "
            f"{'':>8} "
            f"{'':>8} "
            f"{avg_sharpe:>6.2f} "
            f"{avg_winrate:>7.1f}% "
            f"{'':>6} "
            f"{avg_trades:>7.0f}"
        )


if __name__ == '__main__':
    main()
