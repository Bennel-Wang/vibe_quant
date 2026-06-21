"""
主力意图分析 — 周线回测 & 绘图
与生产调度器口径一致：日线重采样为周线 → 计算指标 → 回测

Usage: python scripts/plot_manipulation_phase.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
logging.basicConfig(level=logging.WARNING)

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import pandas as pd
import numpy as np

plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'Noto Sans SC']
plt.rcParams['axes.unicode_minus'] = False

from quant_system.strategy import QuantStrategy
from quant_system.backtest import BacktestEngine
from quant_system.indicators import technical_indicators
from quant_system.utils.ohlcv import resample_to_weekly
from quant_system.data_source import unified_data

# ── 策略定义 ──
STRATEGY = QuantStrategy(
    name="Smart Money Phase Tracking (Weekly)",
    description="Track smart money manipulation phases on weekly data."
)
STRATEGY.add_rule(condition="manipulation_phase == '吸筹'",
                  action="buy", position_ratio=0.20,
                  reason="Accumulation phase", connector="OR")
STRATEGY.add_rule(condition="eff_zscore > 0.8 and volume_ratio > 1.2 and rel_price_change_ema5 > 0",
                  action="buy", position_ratio=0.10,
                  reason="Efficiency breakout + high vol", connector="OR")
STRATEGY.add_rule(condition="manipulation_phase == '出货'",
                  action="sell", position_ratio=0.50,
                  reason="Distribution phase", connector="OR")
STRATEGY.add_rule(condition="manipulation_phase == '砸盘'",
                  action="sell", position_ratio=0.80,
                  reason="Dumping phase", connector="OR")
STRATEGY.add_rule(condition="manipulation_phase == '无量阴跌' and rel_price_change_ema20 < -0.3",
                  action="sell", position_ratio=0.30,
                  reason="Low-vol decline + weakness", connector="OR")
STRATEGY.max_position_ratio = 0.60

TEST_STOCKS = [
    ('600519.SH', '贵州茅台'),
    ('000858.SZ', '五粮液'),
    ('300750.SZ', '宁德时代'),
    ('002594.SZ', '比亚迪'),
    ('600900.SH', '长江电力'),
    ('002371.SZ', '北方华创'),
]

END_DATE = '20260505'
START_DATE = '20200101'
BENCHMARK = '000001.SH'  # 上证指数


def build_weekly_precomputed(code):
    """日线 → 周线重采样 → 计算全部指标"""
    df_day = unified_data.get_historical_data(code, '', END_DATE)
    if df_day is None or df_day.empty:
        raise ValueError(f"No daily data for {code}")
    if df_day['date'].dtype == 'object':
        df_day['date'] = pd.to_datetime(df_day['date'].astype(str), format='%Y%m%d', errors='coerce')
    else:
        df_day['date'] = pd.to_datetime(df_day['date'])
    df_week = resample_to_weekly(df_day)
    return technical_indicators.calculate_all_indicators_from_df(df_week, code=code, freq='week')


def load_benchmark_weekly():
    """加载上证指数周线，用于计算跑赢大盘"""
    df_day = unified_data.get_historical_data(BENCHMARK, '', END_DATE)
    if df_day is None or df_day.empty:
        return None
    if df_day['date'].dtype == 'object':
        df_day['date'] = pd.to_datetime(df_day['date'].astype(str), format='%Y%m%d', errors='coerce')
    else:
        df_day['date'] = pd.to_datetime(df_day['date'])
    df_week = resample_to_weekly(df_day)
    df_week = df_week.sort_values('date').reset_index(drop=True)
    df_week = df_week[(df_week['date'] >= START_DATE) & (df_week['date'] <= END_DATE)]
    return df_week


def run_weekly(engine):
    results = []
    for code, name in TEST_STOCKS:
        print(f"  回测: {name} ({code}) ...", end=" ")
        try:
            precomputed = build_weekly_precomputed(code)
            r = engine.run_backtest(
                code=code, strategy=STRATEGY,
                start_date=START_DATE, end_date=END_DATE,
                initial_capital=1_000_000,
                precomputed_df=precomputed,
            )
            results.append((name, code, r))
            print(f"收益: {r.total_return_pct:+.1f}%  Sharpe: {r.sharpe_ratio:.2f}  最大回撤: {r.max_drawdown_pct:.1f}%  胜率: {r.win_rate:.1f}%  交易: {len(r.trades)}笔")
        except Exception as e:
            print(f"ERROR: {e}")
    return results


def plot_all(results, bench_df, save_path):
    """三列布局：净值曲线 | 回撤曲线 | 跑赢大盘"""
    n = len(results)
    if n == 0:
        return

    fig, axes = plt.subplots(n, 3, figsize=(26, 3.2 * n))
    if n == 1:
        axes = axes.reshape(1, -1)

    # 上证指数累计收益（用于第三列对比）
    bench_ret_series = None
    if bench_df is not None and len(bench_df) > 0:
        bm_init = bench_df['close'].iloc[0]
        bench_ret_series = (bench_df['close'] / bm_init - 1) * 100
        bm_total = bench_ret_series.iloc[-1]

    for i, (name, code, r) in enumerate(results):
        eq = r.equity_curve
        if eq is None or eq.empty:
            continue
        eq = eq.copy()
        eq['date'] = pd.to_datetime(eq['date'])
        eq = eq.sort_values('date').reset_index(drop=True)
        eq['ret'] = (eq['equity'] / r.initial_capital - 1) * 100
        eq['dd'] = (eq['equity'] / eq['equity'].cummax() - 1) * 100

        excess = r.total_return_pct - bm_total if bench_ret_series is not None else 0

        # ── 第一列：净值曲线 ──
        ax1 = axes[i, 0]
        ax1.plot(eq['date'], eq['ret'], color='#1f77b4', linewidth=1.2, label='策略净值')
        ax1.axhline(y=0, color='gray', linestyle='--', linewidth=0.5)
        ax1.fill_between(eq['date'], 0, eq['ret'],
                         where=eq['ret'] >= 0, color='#d4edda', alpha=0.3)
        ax1.fill_between(eq['date'], 0, eq['ret'],
                         where=eq['ret'] < 0, color='#f8d7da', alpha=0.3)
        ax1.set_title(f'{name} ({code})  |  累计收益: {r.total_return_pct:+.1f}%  '
                      f'年化: {r.annual_return:+.1f}%  Sharpe: {r.sharpe_ratio:.2f}',
                      fontsize=10, fontweight='bold')
        ax1.set_ylabel('累计收益 (%)', fontsize=9)
        ax1.legend(loc='upper left', fontsize=8)
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

        # ── 第二列：回撤 ──
        ax2 = axes[i, 1]
        ax2.fill_between(eq['date'], 0, eq['dd'], color='#dc3545', alpha=0.5, linewidth=0)
        ax2.plot(eq['date'], eq['dd'], color='#dc3545', linewidth=0.7)
        ax2.axhline(y=r.max_drawdown_pct, color='#721c24', linestyle='--', linewidth=0.7,
                    label=f'最大回撤: {r.max_drawdown_pct:.1f}%')
        ax2.set_title(f'{name} — 回撤  (最大回撤: {r.max_drawdown_pct:.1f}%)',
                      fontsize=10, fontweight='bold')
        ax2.set_ylabel('回撤 (%)', fontsize=9)
        ax2.legend(loc='lower left', fontsize=8)
        ax2.grid(True, alpha=0.3)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        ax2.yaxis.set_major_formatter(mticker.FormatStrFormatter('%.0f%%'))

        # ── 第三列：跑赢大盘 ──
        ax3 = axes[i, 2]
        if bench_ret_series is not None and bench_df is not None:
            ax3.plot(bench_df['date'], bench_ret_series, color='black', linewidth=1.5,
                     linestyle='--', label=f'上证指数 ({bm_total:+.1f}%)')
        ax3.plot(eq['date'], eq['ret'], color='#1f77b4', linewidth=1.5, label=f'策略 ({r.total_return_pct:+.1f}%)')

        # 填充策略 vs 大盘的差值区域
        if bench_ret_series is not None and bench_df is not None:
            # align dates
            eq_aligned = eq.set_index('date')
            bm_aligned = pd.DataFrame({'date': bench_df['date'], 'bm_ret': bench_ret_series.values}).set_index('date')
            common = eq_aligned.join(bm_aligned, how='inner')
            if len(common) > 1:
                ax3.fill_between(common.index, common['bm_ret'], common['ret'],
                                 where=common['ret'] >= common['bm_ret'],
                                 color='#d4edda', alpha=0.3, label='跑赢')
                ax3.fill_between(common.index, common['bm_ret'], common['ret'],
                                 where=common['ret'] < common['bm_ret'],
                                 color='#f8d7da', alpha=0.3, label='跑输')

        ax3.set_title(f'{name} — 跑赢大盘: {excess:+.1f}%  '
                      f'(α={r.alpha:+.1f}%, β={r.beta:.2f})',
                      fontsize=10, fontweight='bold')
        ax3.set_ylabel('累计收益 (%)', fontsize=9)
        ax3.legend(loc='upper left', fontsize=8)
        ax3.grid(True, alpha=0.3)
        ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

    fig.tight_layout(pad=2.0)
    fig.savefig(save_path, dpi=150, bbox_inches='tight')
    print(f"\n图表已保存: {save_path}")
    plt.close(fig)


def main():
    out_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'output')
    os.makedirs(out_dir, exist_ok=True)

    print("=" * 70)
    print("  主力意图分析 — 周线回测 (与生产调度器口径一致)")
    print(f"  策略: {STRATEGY.name}")
    print(f"  基准: {BENCHMARK} (上证指数)")
    print(f"  区间: {START_DATE} ~ {END_DATE}")
    print(f"  规则: {len(STRATEGY.rules)}条, 最大仓位: {STRATEGY.max_position_ratio:.0%}")
    print(f"  股票池: {len(TEST_STOCKS)}只")
    print("=" * 70)

    # 加载基准
    print("\n加载上证指数基准...")
    bench_df = load_benchmark_weekly()
    if bench_df is not None:
        bm_ret = (bench_df['close'].iloc[-1] / bench_df['close'].iloc[0] - 1) * 100
        print(f"  上证指数同期收益: {bm_ret:+.1f}%")
    else:
        bm_ret = 0
        print("  (无法加载基准)")

    # 运行回测
    print("\n运行周线回测...")
    engine = BacktestEngine()
    results = run_weekly(engine)

    if not results:
        print("无回测结果。")
        return

    # ── 汇总表 ──
    print(f"\n{'='*120}")
    print("  回 测 汇 总")
    print(f"{'='*120}")
    header = (f"{'股票':<12} {'累计收益':>8} {'年化收益':>8} {'最大回撤':>8} "
              f"{'Sharpe':>7} {'胜率':>7} {'盈亏比':>6} {'Alpha':>7} "
              f"{'跑赢大盘':>8} {'交易':>5}")
    print(header)
    print("-" * 120)

    for name, code, r in results:
        excess = r.total_return_pct - bm_ret if bench_df is not None else 0
        pf_str = f"{r.profit_factor:.2f}" if r.profit_factor != float('inf') else "inf"
        print(f"{name:<12} {r.total_return_pct:>7.1f}% {r.annual_return:>7.1f}% "
              f"{r.max_drawdown_pct:>7.1f}% {r.sharpe_ratio:>6.2f} "
              f"{r.win_rate:>6.1f}% {pf_str:>6} {r.alpha:>6.1f}% "
              f"{excess:>7.1f}% {len(r.trades):>5}")

    avg_ret = np.mean([r.total_return_pct for _, _, r in results])
    avg_sharpe = np.mean([r.sharpe_ratio for _, _, r in results])
    avg_win = np.mean([r.win_rate for _, _, r in results])
    avg_dd = np.mean([r.max_drawdown_pct for _, _, r in results])
    avg_trades = np.mean([len(r.trades) for _, _, r in results])
    avg_excess = avg_ret - bm_ret if bench_df is not None else 0
    print("-" * 120)
    print(f"{'平均':<12} {avg_ret:>7.1f}% {'':>8} {avg_dd:>7.1f}% {avg_sharpe:>6.2f} "
          f"{avg_win:>6.1f}% {'':>6} {'':>7} {avg_excess:>7.1f}% {avg_trades:>5.0f}")
    print(f"\n  上证指数同期: {bm_ret:+.1f}%  |  策略平均跑赢: {avg_excess:+.1f}%")

    # ── 绘图 ──
    plot_all(results, bench_df, os.path.join(out_dir, 'manipulation_phase_weekly_backtest.png'))


if __name__ == '__main__':
    main()
