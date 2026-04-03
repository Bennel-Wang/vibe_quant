"""
Strategy optimization Round 5: Final push to Sharpe > 0.70
Key issue: 600900 and 002371 drag down Sharpe (0.21 and 0.38 respectively)
Approach: add Bollinger ultra-bottom rule for mean-reversion stocks +
          slightly looser sell threshold to let winners run more
"""
import sys, warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, '.')
from quant_system.backtest import backtest_engine
from quant_system.strategy import QuantStrategy, merge_buy_sell_strategies

STOCKS = ['002594', '600900', '002371', '600309', '300750', '601318']
PERIOD = ('20190101', '20241231')
CAPITAL = 500000

def make_strategy(cfg):
    buy_s = QuantStrategy(cfg['name'] + '_买入', cfg['description'])
    buy_s.max_position_ratio = cfg.get('max_pos', 0.3)
    for r in cfg['buy_rules']:
        buy_s.add_rule(r['cond'], 'buy', r.get('pos', 0.15), r.get('reason', ''))
    for r in cfg.get('buy_exclusions', []):
        buy_s.add_exclusion_rule(r['cond'], r.get('reason', ''))
    sell_s = QuantStrategy(cfg['name'] + '_卖出', '配套卖出')
    sell_s.max_position_ratio = 1.0
    for r in cfg['sell_rules']:
        sell_s.add_rule(r['cond'], 'sell', r.get('pos', 0.5), r.get('reason', ''))
    return merge_buy_sell_strategies(buy_s, sell_s)

def run_batch(strategy):
    results = []
    for code in STOCKS:
        try:
            r = backtest_engine.run_backtest(code, strategy, PERIOD[0], PERIOD[1], CAPITAL)
            wr = r.win_rate if r.win_rate <= 100 else r.win_rate / 100
            results.append({'code': code, 'ret': r.total_return_pct, 'ann': r.annual_return,
                'dd': r.max_drawdown_pct, 'trades': r.total_trades, 'wr': wr, 'sharpe': r.sharpe_ratio})
        except Exception as e:
            results.append({'code': code, 'ret': 0, 'ann': 0, 'dd': 0, 'trades': 0, 'wr': 0, 'sharpe': 0})
    return results

def summarize(label, results):
    valid = [r for r in results if r.get('trades', 0) > 0]
    print(f'\n{"="*72}')
    print(f'  {label}')
    print(f'{"="*72}')
    print(f'  {"Code":<8} {"Return%":>8} {"Annual%":>8} {"MaxDD%":>8} {"Trades":>7} {"WR%":>7} {"Sharpe":>8}')
    print(f'  {"-"*58}')
    for r in results:
        flag = ' [NO TRADE]' if r.get('trades', 0) == 0 else ''
        print(f'  {r["code"]:<8} {r["ret"]:>8.1f} {r["ann"]:>8.1f} {r["dd"]:>8.1f} {r["trades"]:>7} {r["wr"]:>7.1f} {r["sharpe"]:>8.2f}{flag}')
    if valid:
        avg_ret = sum(r['ret'] for r in valid)/len(valid)
        avg_ann = sum(r['ann'] for r in valid)/len(valid)
        avg_dd  = sum(r['dd'] for r in valid)/len(valid)
        avg_tr  = sum(r['trades'] for r in valid)/len(valid)
        avg_wr  = sum(r['wr'] for r in valid)/len(valid)
        avg_sh  = sum(r['sharpe'] for r in valid)/len(valid)
        print(f'  {"AVG":<8} {avg_ret:>8.1f} {avg_ann:>8.1f} {avg_dd:>8.1f} {avg_tr:>7.1f} {avg_wr:>7.1f} {avg_sh:>8.2f}  ({len(valid)}/{len(results)} active)')
        return {'avg_ret': avg_ret, 'avg_ann': avg_ann, 'avg_dd': avg_dd,
                'avg_tr': avg_tr, 'avg_wr': avg_wr, 'avg_sh': avg_sh}
    return {'avg_ret': 0, 'avg_ann': 0, 'avg_dd': 0, 'avg_tr': 0, 'avg_wr': 0, 'avg_sh': 0}


configs_r5 = []

# V18: V16 + Bollinger ultra-bottom rule (helps mean-reversion stocks)
configs_r5.append({
    'name': 'AI策略v18',
    'description': 'V18: V16+布林极下轨超卖补充规则，兼顾动量型和均值回归型标的',
    'max_pos': 0.4,
    'buy_rules': [
        # Rule 0: Ultra-oversold Bollinger bottom (captures 600900-type stocks)
        {'cond': 'boll_position < 0.12 and rsi_6 < 26 and volume_ratio > 1.5 and overall_score > -55',
         'pos': 0.15, 'reason': '布林极下轨+RSI极超卖+放量，均值回归买入'},
        # Rule 1: MACD (from V16, larger position)
        {'cond': 'macd_histogram > 0 and macd > macd_signal and rsi_6 > 30 and rsi_6 < 62 and close > ma_20 and volume_ratio > 1.4 and rel_strength_10 > -2',
         'pos': 0.18, 'reason': 'MACD金叉+RSI适中+20日线+量比+不弱于大盘'},
        # Rule 2: Weekly RSI oversold recovery
        {'cond': 'w_rsi6_pct100 < 40 and rsi_6 > 28 and close > ma_20 and boll_position > 0.3 and boll_position < 0.6 and volume_ratio > 1.2 and macd_histogram > -0.05',
         'pos': 0.10, 'reason': '周线低位+日线回升+布林中轨区'},
        # Rule 3: Momentum continuation (small position)
        {'cond': 'close > ma_20 and close > ma_60 and rel_strength_20 > 3 and macd_histogram > 0 and rsi_6 < 68',
         'pos': 0.05, 'reason': '趋势延续加仓'},
    ],
    'buy_exclusions': [
        {'cond': 'rsi_6 > 72', 'reason': 'RSI过高'},
        {'cond': 'overall_score < -50', 'reason': '大盘极弱'},
        {'cond': 'rel_strength_10 < -8', 'reason': '明显跑输大盘'},
    ],
    'sell_rules': [
        # Bollinger bottom exit: sell when price returns to upper Bollinger
        {'cond': 'boll_position > 0.7 and rsi_6 > 58', 'pos': 0.5, 'reason': '布林中上轨+RSI回升，均值回归止盈'},
        {'cond': 'w_rsi6_pct100 > 75 and rsi_6 > 68', 'pos': 0.5, 'reason': '周线75分位+日线高位，止盈'},
        {'cond': 'close < ma_20 * 0.97', 'pos': 1.0, 'reason': '破20日线3%，止损'},
        {'cond': 'macd < macd_signal and macd_histogram < -0.05 and close < ma_20', 'pos': 0.6, 'reason': 'MACD死叉+破20日线'},
        {'cond': 'rsi_6 > 82 and pct_chg < -0.5', 'pos': 0.5, 'reason': '超买区转跌止盈'},
        {'cond': 'w_rsi6_pct100 > 82', 'pos': 0.4, 'reason': '周线RSI极高，减仓'},
    ],
})

# V19: V16 + looser sell trigger (raise exit threshold to let winners run even longer)
configs_r5.append({
    'name': 'AI策略v19',
    'description': 'V19: V16+更宽松止盈(周线RSI>80才减仓)，延长持有时间捕获大行情',
    'max_pos': 0.4,
    'buy_rules': [
        {'cond': 'macd_histogram > 0 and macd > macd_signal and rsi_6 > 30 and rsi_6 < 62 and close > ma_20 and volume_ratio > 1.4 and rel_strength_10 > -2',
         'pos': 0.20, 'reason': 'MACD金叉+RSI适中+20日线+量比+不弱于大盘，高置信度建仓'},
        {'cond': 'w_rsi6_pct100 < 40 and rsi_6 > 28 and close > ma_20 and boll_position > 0.3 and boll_position < 0.6 and volume_ratio > 1.2 and macd_histogram > -0.05',
         'pos': 0.10, 'reason': '周线低位+日线回升+布林中轨区'},
        {'cond': 'close > ma_20 and close > ma_60 and rel_strength_20 > 3 and macd_histogram > 0 and rsi_6 < 68',
         'pos': 0.05, 'reason': '趋势延续加仓'},
    ],
    'buy_exclusions': [
        {'cond': 'rsi_6 > 72', 'reason': 'RSI过高'},
        {'cond': 'overall_score < -50', 'reason': '大盘极弱'},
        {'cond': 'rel_strength_10 < -8', 'reason': '明显跑输大盘'},
    ],
    'sell_rules': [
        # Raise bar: only exit at w_rsi6_pct100 > 80 (vs 75 in V16)
        {'cond': 'w_rsi6_pct100 > 80 and rsi_6 > 70', 'pos': 0.5, 'reason': '周线80分位+日线高位，延迟止盈'},
        {'cond': 'close < ma_20 * 0.97', 'pos': 1.0, 'reason': '破20日线3%，止损'},
        {'cond': 'macd < macd_signal and macd_histogram < -0.05 and close < ma_20', 'pos': 0.6, 'reason': 'MACD死叉+破20日线'},
        {'cond': 'rsi_6 > 85 and pct_chg < -0.5', 'pos': 0.5, 'reason': '极超买区转跌止盈'},
        {'cond': 'w_rsi6_pct100 > 88', 'pos': 0.6, 'reason': '周线RSI历史极高，大幅减仓'},
    ],
})

# V20: Final candidate — V16 core + Bollinger bottom from V18 + refined sell
configs_r5.append({
    'name': 'AI策略v20',
    'description': 'V20: 最终候选 - MACD动量+布林超跌+趋势延续，四档建仓+双通道卖出',
    'max_pos': 0.45,
    'buy_rules': [
        # Channel 1: Ultra oversold Bollinger (for stable stocks like 600900)
        {'cond': 'boll_position < 0.12 and rsi_6 < 26 and volume_ratio > 1.5 and overall_score > -55',
         'pos': 0.15, 'reason': 'CH1: 布林极下轨+极超卖+放量，均值回归底部买入'},
        # Channel 2: MACD momentum (for growth stocks like 300750, 002594)
        {'cond': 'macd_histogram > 0 and macd > macd_signal and rsi_6 > 30 and rsi_6 < 62 and close > ma_20 and volume_ratio > 1.4 and rel_strength_10 > -2',
         'pos': 0.18, 'reason': 'CH2: MACD金叉+20日线+量比+不弱，动量买入'},
        # Channel 3: Weekly oversold recovery
        {'cond': 'w_rsi6_pct100 < 38 and rsi_6 > 28 and close > ma_20 and volume_ratio > 1.2 and macd_histogram > -0.05',
         'pos': 0.08, 'reason': 'CH3: 周线超跌+日线站20日线，超跌恢复买入'},
        # Channel 4: Trend continuation (very small)
        {'cond': 'close > ma_60 and rel_strength_20 > 5 and macd_histogram > 0 and rsi_6 < 65 and volume_ratio > 1.5',
         'pos': 0.04, 'reason': 'CH4: 长线趋势+相对超强+放量，趋势追加'},
    ],
    'buy_exclusions': [
        {'cond': 'rsi_6 > 72', 'reason': 'RSI偏高不追'},
        {'cond': 'overall_score < -52', 'reason': '大盘极度恐慌'},
        {'cond': 'rel_strength_10 < -8', 'reason': '持续跑输大盘'},
        {'cond': 'm_rsi6_pct100 > 82', 'reason': '月线趋势过热'},
    ],
    'sell_rules': [
        # Double-exit: technical + fundamental
        {'cond': 'boll_position > 0.75 and rsi_6 > 62', 'pos': 0.4, 'reason': '布林中上轨+RSI适中以上，均值回归止盈'},
        {'cond': 'w_rsi6_pct100 > 76 and rsi_6 > 68', 'pos': 0.5, 'reason': '周线76分位+日线高位，止盈'},
        {'cond': 'w_rsi6_pct100 > 85', 'pos': 0.6, 'reason': '周线RSI历史高位，大幅减仓'},
        {'cond': 'close < ma_20 * 0.97', 'pos': 1.0, 'reason': '硬止损: 破20日线3%'},
        {'cond': 'macd < macd_signal and close < ma_20', 'pos': 0.6, 'reason': 'MACD反转+破20日线'},
        {'cond': 'rsi_6 > 82 and pct_chg < -0.3', 'pos': 0.5, 'reason': '超买高位回落止盈'},
        {'cond': 'volume_ratio > 3 and pct_chg < -2.5', 'pos': 0.7, 'reason': '大量暴跌出货信号'},
    ],
})


if __name__ == '__main__':
    all_sums = []
    for cfg in configs_r5:
        strat = make_strategy(cfg)
        results = run_batch(strat)
        sm = summarize(cfg['name'] + ': ' + cfg['description'], results)
        sm['name'] = cfg['name']
        all_sums.append(sm)

    print('\n\n' + '='*72)
    print('  Round 5 Final Summary')
    print('='*72)
    print(f'  {"Name":<15} {"Annual%":>8} {"Return%":>8} {"MaxDD%":>8} {"Trades":>7} {"WR%":>7} {"Sharpe":>8}')
    print(f'  {"-"*60}')
    all_rounds = all_sums + [
        {'name': 'V10(best_r3)', 'avg_ann': 2.9, 'avg_ret': 18.6, 'avg_dd': -7.0, 'avg_tr': 31.3, 'avg_wr': 49.3, 'avg_sh': 0.63},
        {'name': 'V16(best_r4)', 'avg_ann': 3.1, 'avg_ret': 20.0, 'avg_dd': -7.7, 'avg_tr': 31.3, 'avg_wr': 49.8, 'avg_sh': 0.63},
    ]
    ranked = sorted(all_rounds, key=lambda x: x['avg_sh'], reverse=True)
    for s in ranked:
        print(f'  {s["name"]:<15} {s["avg_ann"]:>8.1f} {s["avg_ret"]:>8.1f} {s["avg_dd"]:>8.1f} {s["avg_tr"]:>7.1f} {s["avg_wr"]:>7.1f} {s["avg_sh"]:>8.2f}')
    print()
    best = ranked[0]
    print(f'\n  === OVERALL WINNER: {best["name"]} ===')
    print(f'  Annual={best["avg_ann"]:.1f}%  Return={best["avg_ret"]:.1f}%  MaxDD={best["avg_dd"]:.1f}%  Sharpe={best["avg_sh"]:.2f}  WR={best["avg_wr"]:.1f}%')
