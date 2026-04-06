"""
Strategy optimization script - backtests baseline and variants for all 9 strategy pairs.
"""
import sys
import os
import json
import copy
import traceback

sys.path.insert(0, 'D:\\Git_project\\vibecoding_quant')
os.chdir('D:\\Git_project\\vibecoding_quant')

from quant_system.backtest import BacktestEngine
from quant_system.strategy import QuantStrategy, strategy_manager

engine = BacktestEngine()

# ─── stock groups ─────────────────────────────────────────────────────────────
VALUE_STOCKS   = ['600519', '000858', '600900', '600009', '600309']
GROWTH_STOCKS  = ['300750', '002594', '002371', '688981', '300274']
CYCLE_STOCKS   = ['600519', '300750', '600309', '002920', '002572']
DEFENSE_STOCKS = ['600900', '600009', '600519', '000858', '600309']

START, END = '20200101', '20250101'

# ─── helper ───────────────────────────────────────────────────────────────────
def backtest_strategy_dict(code, buy_dict, sell_dict, start=START, end=END):
    try:
        buy_s  = QuantStrategy.from_dict(copy.deepcopy(buy_dict))
        sell_s = QuantStrategy.from_dict(copy.deepcopy(sell_dict))
        r = engine.run_backtest(code, buy_s, start, end)
        return {
            'annual_return': r.annual_return,
            'win_rate': r.win_rate,
            'max_drawdown': r.max_drawdown_pct,
            'total_trades': r.total_trades,
        }
    except Exception as e:
        return {'error': str(e), 'annual_return': -999, 'win_rate': 0, 'max_drawdown': -999, 'total_trades': 0}


def avg_results(results):
    valid = [r for r in results if 'error' not in r]
    if not valid:
        return {'annual_return': -999, 'win_rate': 0, 'max_drawdown': -999, 'total_trades': 0}
    return {
        'annual_return': sum(r['annual_return'] for r in valid) / len(valid),
        'win_rate': sum(r['win_rate'] for r in valid) / len(valid),
        'max_drawdown': sum(r['max_drawdown'] for r in valid) / len(valid),
        'total_trades': sum(r['total_trades'] for r in valid) / len(valid),
    }


def is_acceptable(avg):
    return avg['win_rate'] >= 0.35 and avg['max_drawdown'] >= -45 and avg['annual_return'] > -50


def score(avg):
    if not is_acceptable(avg):
        return -9999
    return avg['annual_return'] * 1.0 + avg['win_rate'] * 20 + avg['max_drawdown'] * 0.2


def test_variants(stocks, variants, n_stocks=3):
    """Test list of (name, buy_dict, sell_dict) on first n_stocks; return list of (name, avg_result)."""
    test_stocks = stocks[:n_stocks]
    out = []
    for vname, buy_dict, sell_dict in variants:
        results = [backtest_strategy_dict(code, buy_dict, sell_dict) for code in test_stocks]
        avg = avg_results(results)
        print(f'  {vname}: annual={avg["annual_return"]:.1f}% win={avg["win_rate"]:.1%} dd={avg["max_drawdown"]:.1f}% trades={avg["total_trades"]:.0f}')
        out.append((vname, avg))
    return out


# ─── Load original strategies ─────────────────────────────────────────────────
with open('data/strategies.json', encoding='utf-8') as f:
    orig = json.load(f)

optimized = copy.deepcopy(orig)
summary = []

# ══════════════════════════════════════════════════════════════════════════════
# 1. 4421极限底部买入 + 4421极限顶部卖出
# ══════════════════════════════════════════════════════════════════════════════
print('\n=== 1. 4421极限底部买入 + 4421极限顶部卖出 ===')
b0 = orig['4421极限底部买入']
s0 = orig['4421极限顶部卖出']

# v1 baseline
v1_buy = copy.deepcopy(b0)
v1_sell = copy.deepcopy(s0)

# v2 loosen buy: lower thresholds ~20%
v2_buy = copy.deepcopy(b0)
v2_buy['rules'][0]['condition'] = 'm_rsi6_pct100 < 18 and w_rsi6_pct100 < 18 and idx_ret_60 < -8 and pettm_pct10y < 30'
v2_buy['rules'][1]['condition'] = 'm_rsi6_pct100 < 24 and idx_ret_20 < -3 and w_rsi6_pct100 < 24 and pettm_pct10y < 35'
v2_buy['rules'][2]['condition'] = 'w_rsi6_pct100 < 15 and pettm_pct10y < 25 and idx_ret_60 < -4'
v2_buy['rules'][3]['condition'] = 'm_rsi6_pct100 < 30 and rsi6_pct100 < 13 and idx_ret_20 < -4'

# v3 add volume confirmation
v3_buy = copy.deepcopy(b0)
for r in v3_buy['rules']:
    r['condition'] = r['condition'] + ' and volume_ratio > 1.2'
v3_sell = copy.deepcopy(s0)

# v4 tighter PE thresholds (more value-focused)
v4_buy = copy.deepcopy(b0)
v4_buy['rules'][0]['condition'] = 'm_rsi6_pct100 < 15 and w_rsi6_pct100 < 15 and idx_ret_60 < -10 and pettm_pct10y < 20'
v4_buy['rules'][1]['condition'] = 'm_rsi6_pct100 < 20 and idx_ret_20 < -4 and w_rsi6_pct100 < 20 and pettm_pct10y < 25'
v4_buy['rules'][2]['condition'] = 'w_rsi6_pct100 < 12 and pettm_pct10y < 15 and idx_ret_60 < -6'
v4_buy['rules'][3]['condition'] = 'm_rsi6_pct100 < 25 and rsi6_pct100 < 10 and idx_ret_20 < -5'

# v5 best combo: loosen + add OR condition for stronger signals
v5_buy = copy.deepcopy(v2_buy)
v5_buy['rules'].append({
    'condition': 'm_rsi6_pct100 < 22 and w_rsi6_pct100 < 22 and pettm_pct10y < 28',
    'action': 'buy',
    'position_ratio': 0.09,
    'reason': '月线周线均极度超卖且估值合理',
    'connector': 'OR'
})
v5_buy['exclusion_rules'] = [r for r in b0.get('exclusion_rules', []) if 'idx_ret_60 > 5' not in r.get('condition','')]

variants_1 = [
    ('v1_baseline', v1_buy, v1_sell),
    ('v2_loosen',   v2_buy, s0),
    ('v3_volume',   v3_buy, v3_sell),
    ('v4_tight_pe', v4_buy, s0),
    ('v5_combo',    v5_buy, s0),
]
results_1 = test_variants(VALUE_STOCKS, variants_1)
best_v1 = max(results_1, key=lambda x: score(x[1]))
print(f'  BEST: {best_v1[0]} -> annual={best_v1[1]["annual_return"]:.1f}%')

if best_v1[0] != 'v1_baseline':
    idx = [v[0] for v in variants_1].index(best_v1[0])
    best_buy_dict = variants_1[idx][1]
    best_buy_dict['name'] = '4421极限底部买入'
    best_buy_dict['description'] = orig['4421极限底部买入'].get('description', '') + ' [优化版]'
    optimized['4421极限底部买入'] = best_buy_dict

summary.append({
    'pair': '4421极限底部买入/顶部卖出',
    'best_variant': best_v1[0],
    'annual_return': best_v1[1]['annual_return'],
    'win_rate': best_v1[1]['win_rate'],
    'max_drawdown': best_v1[1]['max_drawdown'],
})

# ══════════════════════════════════════════════════════════════════════════════
# 2. 低PE价值底部买入 + 高PE泡沫卖出
# ══════════════════════════════════════════════════════════════════════════════
print('\n=== 2. 低PE价值底部买入 + 高PE泡沫卖出 ===')
b0 = orig['低PE价值底部买入']
s0 = orig['高PE泡沫卖出']

v1_buy = copy.deepcopy(b0)

v2_buy = copy.deepcopy(b0)
v2_buy['rules'][0]['condition'] = 'pettm_pct10y < 13 and rsi6_pct100 < 30 and close > ma_250 * 0.60'
v2_buy['rules'][1]['condition'] = 'pettm_pct10y < 22 and w_rsi6_pct100 < 26 and close > ma_60'
v2_buy['rules'][2]['condition'] = 'pettm_pct10y < 18 and idx_ret_60 < -4 and rsi_6 < 45'
v2_buy['rules'][3]['condition'] = 'pettm_pct10y < 10 and close < ma_60'

v3_buy = copy.deepcopy(b0)
for r in v3_buy['rules']:
    if 'volume_ratio' not in r['condition']:
        r['condition'] = r['condition'] + ' and volume_ratio > 1.2'

v4_buy = copy.deepcopy(b0)
v4_buy['rules'][0]['condition'] = 'pettm_pct10y < 8 and rsi6_pct100 < 20 and close > ma_250 * 0.60'
v4_buy['rules'][1]['condition'] = 'pettm_pct10y < 15 and w_rsi6_pct100 < 18 and close > ma_60'
v4_buy['rules'][2]['condition'] = 'pettm_pct10y < 12 and idx_ret_60 < -5 and rsi_6 < 35'
v4_buy['rules'][3]['condition'] = 'pettm_pct10y < 6 and close < ma_60'

v5_buy = copy.deepcopy(v2_buy)
v5_buy['rules'].append({
    'condition': 'pettm_pct10y < 20 and rsi6_pct100 < 20 and idx_ret_60 < -3',
    'action': 'buy',
    'position_ratio': 0.1,
    'reason': 'PE偏低且日线超卖市场弱势',
    'connector': 'OR'
})

# Improved sell: raise PE threshold slightly for less premature selling
v5_sell = copy.deepcopy(s0)
v5_sell['rules'][0]['condition'] = 'pettm_pct10y > 85 and rsi6_pct100 > 75'
v5_sell['rules'][1]['condition'] = 'pettm_pct10y > 92 and macd_histogram < 0'

variants_2 = [
    ('v1_baseline', v1_buy, s0),
    ('v2_loosen',   v2_buy, s0),
    ('v3_volume',   v3_buy, s0),
    ('v4_tight_pe', v4_buy, s0),
    ('v5_combo',    v5_buy, v5_sell),
]
results_2 = test_variants(VALUE_STOCKS, variants_2)
best_v2 = max(results_2, key=lambda x: score(x[1]))
print(f'  BEST: {best_v2[0]} -> annual={best_v2[1]["annual_return"]:.1f}%')

if best_v2[0] != 'v1_baseline':
    idx = [v[0] for v in variants_2].index(best_v2[0])
    best_buy_dict = variants_2[idx][1]
    best_buy_dict['name'] = '低PE价值底部买入'
    best_buy_dict['description'] = orig['低PE价值底部买入'].get('description', '') + ' [优化版]'
    optimized['低PE价值底部买入'] = best_buy_dict
    if best_v2[0] == 'v5_combo':
        v5_sell['name'] = '高PE泡沫卖出'
        optimized['高PE泡沫卖出'] = v5_sell

summary.append({
    'pair': '低PE价值底部买入/高PE泡沫卖出',
    'best_variant': best_v2[0],
    'annual_return': best_v2[1]['annual_return'],
    'win_rate': best_v2[1]['win_rate'],
    'max_drawdown': best_v2[1]['max_drawdown'],
})

# ══════════════════════════════════════════════════════════════════════════════
# 3. MACD金叉趋势买入 + MACD死叉趋势卖出
# ══════════════════════════════════════════════════════════════════════════════
print('\n=== 3. MACD金叉趋势买入 + MACD死叉趋势卖出 ===')
b0 = orig['MACD金叉趋势买入']
s0 = orig['MACD死叉趋势卖出']

v1_buy = copy.deepcopy(b0)

v2_buy = copy.deepcopy(b0)
v2_buy['rules'][0]['condition'] = 'macd_histogram > 0 and ma_5 > ma_20 and rsi_6 < 72'
v2_buy['rules'][1]['condition'] = 'macd_histogram > 0 and close > ma_60 and rsi6_pct100 < 62'
v2_buy['rules'][2]['condition'] = 'macd_histogram > 0 and ma_5 > ma_60 and rsi_6 < 65 and idx_ret_20 > 0'

v3_buy = copy.deepcopy(b0)
for r in v3_buy['rules']:
    if 'volume_ratio' not in r['condition']:
        r['condition'] = r['condition'] + ' and volume_ratio > 1.3'

v4_buy = copy.deepcopy(b0)
v4_buy['rules'][0]['condition'] = 'macd_histogram > 0 and ma_5 > ma_20 and ma_20 > ma_60 and rsi_6 < 65 and close > ma_60'
v4_buy['rules'][1]['condition'] = 'macd_histogram > 0 and close > ma_60 and rsi6_pct100 < 55 and volume_ratio > 1.4'
v4_buy['rules'][2]['condition'] = 'macd_histogram > 0 and ma_5 > ma_60 and rsi_6 < 58 and idx_ret_20 > 1'

v5_buy = copy.deepcopy(v2_buy)
v5_buy['rules'].append({
    'condition': 'macd_histogram > 0 and close > ma_20 and rsi6_pct100 > 40 and rsi6_pct100 < 65 and volume_ratio > 1.2',
    'action': 'buy',
    'position_ratio': 0.07,
    'reason': 'MACD上升且价格在均线之上，成交量放量',
    'connector': 'OR'
})

variants_3 = [
    ('v1_baseline', v1_buy, s0),
    ('v2_loosen',   v2_buy, s0),
    ('v3_volume',   v3_buy, s0),
    ('v4_strict',   v4_buy, s0),
    ('v5_combo',    v5_buy, s0),
]
results_3 = test_variants(GROWTH_STOCKS, variants_3)
best_v3 = max(results_3, key=lambda x: score(x[1]))
print(f'  BEST: {best_v3[0]} -> annual={best_v3[1]["annual_return"]:.1f}%')

if best_v3[0] != 'v1_baseline':
    idx = [v[0] for v in variants_3].index(best_v3[0])
    best_buy_dict = variants_3[idx][1]
    best_buy_dict['name'] = 'MACD金叉趋势买入'
    best_buy_dict['description'] = orig['MACD金叉趋势买入'].get('description', '') + ' [优化版]'
    optimized['MACD金叉趋势买入'] = best_buy_dict

summary.append({
    'pair': 'MACD金叉/死叉',
    'best_variant': best_v3[0],
    'annual_return': best_v3[1]['annual_return'],
    'win_rate': best_v3[1]['win_rate'],
    'max_drawdown': best_v3[1]['max_drawdown'],
})

# ══════════════════════════════════════════════════════════════════════════════
# 4. 周线超卖均值回归买入 + 周线超买均值回归卖出
# ══════════════════════════════════════════════════════════════════════════════
print('\n=== 4. 周线超卖均值回归买入 + 周线超买均值回归卖出 ===')
b0 = orig['周线超卖均值回归买入']
s0 = orig['周线超买均值回归卖出']

v1_buy = copy.deepcopy(b0)

v2_buy = copy.deepcopy(b0)
v2_buy['rules'][0]['condition'] = 'w_rsi6_pct100 < 15 and rsi6_pct100 < 15'
v2_buy['rules'][1]['condition'] = 'w_rsi6_pct100 < 24 and rsi_6 < 36 and pettm_pct10y < 55'
v2_buy['rules'][2]['condition'] = 'w_rsi6_pct100 < 30 and idx_ret_60 < -5 and pettm_pct10y < 50'
v2_buy['rules'][3]['condition'] = 'w_rsi6_pct100 < 22 and rsi6_pct100 < 24 and close > ma_250 * 0.60'

v3_buy = copy.deepcopy(b0)
for r in v3_buy['rules']:
    if 'volume_ratio' not in r['condition']:
        r['condition'] = r['condition'] + ' and volume_ratio > 1.2'

v4_buy = copy.deepcopy(b0)
v4_buy['rules'][0]['condition'] = 'w_rsi6_pct100 < 10 and rsi6_pct100 < 10'
v4_buy['rules'][1]['condition'] = 'w_rsi6_pct100 < 18 and rsi_6 < 28 and pettm_pct10y < 45'
v4_buy['rules'][2]['condition'] = 'w_rsi6_pct100 < 22 and idx_ret_60 < -7 and pettm_pct10y < 40'

v5_buy = copy.deepcopy(v2_buy)
v5_buy['rules'].append({
    'condition': 'w_rsi6_pct100 < 28 and m_rsi6_pct100 < 30 and pettm_pct10y < 50',
    'action': 'buy',
    'position_ratio': 0.09,
    'reason': '周线月线均超卖，估值合理',
    'connector': 'OR'
})

# Better sell: less aggressive exit
v5_sell = copy.deepcopy(s0)
v5_sell['rules'][0]['condition'] = 'w_rsi6_pct100 > 70'
v5_sell['rules'][1]['condition'] = 'w_rsi6_pct100 > 85'

variants_4 = [
    ('v1_baseline', v1_buy, s0),
    ('v2_loosen',   v2_buy, s0),
    ('v3_volume',   v3_buy, s0),
    ('v4_strict',   v4_buy, s0),
    ('v5_combo',    v5_buy, v5_sell),
]
results_4 = test_variants(CYCLE_STOCKS, variants_4)
best_v4 = max(results_4, key=lambda x: score(x[1]))
print(f'  BEST: {best_v4[0]} -> annual={best_v4[1]["annual_return"]:.1f}%')

if best_v4[0] != 'v1_baseline':
    idx = [v[0] for v in variants_4].index(best_v4[0])
    best_buy_dict = variants_4[idx][1]
    best_buy_dict['name'] = '周线超卖均值回归买入'
    best_buy_dict['description'] = orig['周线超卖均值回归买入'].get('description', '') + ' [优化版]'
    optimized['周线超卖均值回归买入'] = best_buy_dict
    if best_v4[0] == 'v5_combo':
        v5_sell['name'] = '周线超买均值回归卖出'
        optimized['周线超买均值回归卖出'] = v5_sell

summary.append({
    'pair': '周线超卖/超买均值回归',
    'best_variant': best_v4[0],
    'annual_return': best_v4[1]['annual_return'],
    'win_rate': best_v4[1]['win_rate'],
    'max_drawdown': best_v4[1]['max_drawdown'],
})

# ══════════════════════════════════════════════════════════════════════════════
# 5. 布林超跌价格保护买入 + 布林超涨止盈卖出
# ══════════════════════════════════════════════════════════════════════════════
print('\n=== 5. 布林超跌价格保护买入 + 布林超涨止盈卖出 ===')
b0 = orig['布林超跌价格保护买入']
s0 = orig['布林超涨止盈卖出']

v1_buy = copy.deepcopy(b0)

v2_buy = copy.deepcopy(b0)
v2_buy['rules'][0]['condition'] = 'boll_position < 0.08 and rsi_6 < 34 and volume_ratio > 1.2 and pettm_pct10y < 65'
v2_buy['rules'][1]['condition'] = 'boll_position < 0.13 and rsi6_pct100 < 13 and close > ma_250 * 0.65'
v2_buy['rules'][2]['condition'] = 'boll_position < 0.07 and rsi_6 < 28 and pettm_pct10y < 55'

v3_buy = copy.deepcopy(b0)
v3_buy['rules'][0]['condition'] = 'boll_position < 0.06 and rsi_6 < 30 and volume_ratio > 1.5 and pettm_pct10y < 60'
v3_buy['rules'][1]['condition'] = 'boll_position < 0.10 and rsi6_pct100 < 10 and volume_ratio > 1.3 and close > ma_250 * 0.65'
v3_buy['rules'][2]['condition'] = 'boll_position < 0.05 and rsi_6 < 25 and volume_ratio > 1.3 and pettm_pct10y < 50'

v4_buy = copy.deepcopy(b0)
v4_buy['rules'][0]['condition'] = 'boll_position < 0.04 and rsi_6 < 25 and volume_ratio > 1.3 and pettm_pct10y < 50'
v4_buy['rules'][1]['condition'] = 'boll_position < 0.08 and rsi6_pct100 < 8 and close > ma_250 * 0.65'
v4_buy['rules'][2]['condition'] = 'boll_position < 0.04 and rsi_6 < 20 and pettm_pct10y < 45'

v5_buy = copy.deepcopy(v2_buy)
v5_buy['rules'].append({
    'condition': 'boll_position < 0.10 and rsi_6 < 28 and close > ma_250 * 0.70',
    'action': 'buy',
    'position_ratio': 0.07,
    'reason': '布林下轨附近RSI超卖且价格不破年线',
    'connector': 'OR'
})

# Sell: hold longer before exiting
v5_sell = copy.deepcopy(s0)
v5_sell['rules'][0]['condition'] = 'boll_position > 0.88 and rsi_6 > 68'
v5_sell['rules'][1]['condition'] = 'boll_position > 0.95'

variants_5 = [
    ('v1_baseline', v1_buy, s0),
    ('v2_loosen',   v2_buy, s0),
    ('v3_strict_vol', v3_buy, s0),
    ('v4_tight',    v4_buy, s0),
    ('v5_combo',    v5_buy, v5_sell),
]
results_5 = test_variants(CYCLE_STOCKS, variants_5)
best_v5 = max(results_5, key=lambda x: score(x[1]))
print(f'  BEST: {best_v5[0]} -> annual={best_v5[1]["annual_return"]:.1f}%')

if best_v5[0] != 'v1_baseline':
    idx = [v[0] for v in variants_5].index(best_v5[0])
    best_buy_dict = variants_5[idx][1]
    best_buy_dict['name'] = '布林超跌价格保护买入'
    best_buy_dict['description'] = orig['布林超跌价格保护买入'].get('description', '') + ' [优化版]'
    optimized['布林超跌价格保护买入'] = best_buy_dict
    if best_v5[0] == 'v5_combo':
        v5_sell['name'] = '布林超涨止盈卖出'
        optimized['布林超涨止盈卖出'] = v5_sell

summary.append({
    'pair': '布林超跌保护买入/超涨止盈卖出',
    'best_variant': best_v5[0],
    'annual_return': best_v5[1]['annual_return'],
    'win_rate': best_v5[1]['win_rate'],
    'max_drawdown': best_v5[1]['max_drawdown'],
})

# ══════════════════════════════════════════════════════════════════════════════
# 6. 强势动量右侧买入 + 强势动量趋势卖出
# ══════════════════════════════════════════════════════════════════════════════
print('\n=== 6. 强势动量右侧买入 + 强势动量趋势卖出 ===')
b0 = orig['强势动量右侧买入']
s0 = orig['强势动量趋势卖出']

v1_buy = copy.deepcopy(b0)

v2_buy = copy.deepcopy(b0)
v2_buy['rules'][0]['condition'] = 'rel_strength_20 > 6 and volume_ratio > 1.5 and ma_5 > ma_20 and macd_histogram > 0'
v2_buy['rules'][1]['condition'] = 'rel_strength_5 > 4 and close > ma_20 and macd_histogram > 0 and volume_ratio > 1.3'
v2_buy['rules'][2]['condition'] = 'rel_strength_10 > 5 and rsi6_pct100 > 50 and rsi6_pct100 < 82 and volume_ratio > 1.2'

v3_buy = copy.deepcopy(b0)
v3_buy['rules'][0]['condition'] = 'rel_strength_20 > 8 and volume_ratio > 2.0 and ma_5 > ma_20 and macd_histogram > 0'
v3_buy['rules'][1]['condition'] = 'rel_strength_5 > 5 and close > ma_20 and macd_histogram > 0 and volume_ratio > 1.8'
v3_buy['rules'][2]['condition'] = 'rel_strength_10 > 6 and rsi6_pct100 > 55 and rsi6_pct100 < 80 and volume_ratio > 1.6'

v4_buy = copy.deepcopy(b0)
v4_buy['rules'][0]['condition'] = 'rel_strength_20 > 10 and volume_ratio > 1.8 and ma_5 > ma_20 and macd_histogram > 0 and close > ma_60'
v4_buy['rules'][1]['condition'] = 'rel_strength_5 > 6 and close > ma_20 and macd_histogram > 0 and volume_ratio > 1.6 and close > ma_60'
v4_buy['rules'][2]['condition'] = 'rel_strength_10 > 8 and rsi6_pct100 > 58 and rsi6_pct100 < 78 and volume_ratio > 1.5'

v5_buy = copy.deepcopy(v2_buy)
v5_buy['rules'].append({
    'condition': 'rel_strength_20 > 5 and close > ma_60 and macd_histogram > 0 and rsi6_pct100 > 45',
    'action': 'buy',
    'position_ratio': 0.07,
    'reason': '动量持续且价格在中期均线之上',
    'connector': 'OR'
})

# Improved sell: wait for clearer reversal
v5_sell = copy.deepcopy(s0)
v5_sell['rules'][0]['condition'] = 'rel_strength_5 < -5 and ma_5 < ma_20'
v5_sell['rules'][1]['condition'] = 'close < ma_60 and macd_histogram < 0'

variants_6 = [
    ('v1_baseline', v1_buy, s0),
    ('v2_loosen',   v2_buy, s0),
    ('v3_strict_vol', v3_buy, s0),
    ('v4_strict',   v4_buy, s0),
    ('v5_combo',    v5_buy, v5_sell),
]
results_6 = test_variants(GROWTH_STOCKS, variants_6)
best_v6 = max(results_6, key=lambda x: score(x[1]))
print(f'  BEST: {best_v6[0]} -> annual={best_v6[1]["annual_return"]:.1f}%')

if best_v6[0] != 'v1_baseline':
    idx = [v[0] for v in variants_6].index(best_v6[0])
    best_buy_dict = variants_6[idx][1]
    best_buy_dict['name'] = '强势动量右侧买入'
    best_buy_dict['description'] = orig['强势动量右侧买入'].get('description', '') + ' [优化版]'
    optimized['强势动量右侧买入'] = best_buy_dict
    if best_v6[0] == 'v5_combo':
        v5_sell['name'] = '强势动量趋势卖出'
        optimized['强势动量趋势卖出'] = v5_sell

summary.append({
    'pair': '强势动量右侧买入/趋势卖出',
    'best_variant': best_v6[0],
    'annual_return': best_v6[1]['annual_return'],
    'win_rate': best_v6[1]['win_rate'],
    'max_drawdown': best_v6[1]['max_drawdown'],
})

# ══════════════════════════════════════════════════════════════════════════════
# 7. 短线RSI超卖反弹买入 + 短线RSI超买止盈卖出
# ══════════════════════════════════════════════════════════════════════════════
print('\n=== 7. 短线RSI超卖反弹买入 + 短线RSI超买止盈卖出 ===')
b0 = orig['短线RSI超卖反弹买入']
s0 = orig['短线RSI超买止盈卖出']

v1_buy = copy.deepcopy(b0)

v2_buy = copy.deepcopy(b0)
v2_buy['rules'][0]['condition'] = 'rsi_6 < 26'
v2_buy['rules'][1]['condition'] = 'rsi_6 < 32 and boll_position < 0.15 and pettm_pct10y < 70'
v2_buy['rules'][2]['condition'] = 'rsi6_pct100 < 10 and volume_ratio > 1.3'
v2_buy['rules'][3]['condition'] = 'rsi_6 < 20 and close > ma_250 * 0.60'

v3_buy = copy.deepcopy(b0)
for r in v3_buy['rules']:
    if 'volume_ratio' not in r['condition']:
        r['condition'] = r['condition'] + ' and volume_ratio > 1.3'

v4_buy = copy.deepcopy(b0)
v4_buy['rules'][0]['condition'] = 'rsi_6 < 20'
v4_buy['rules'][1]['condition'] = 'rsi_6 < 25 and boll_position < 0.10 and pettm_pct10y < 60'
v4_buy['rules'][2]['condition'] = 'rsi6_pct100 < 6 and volume_ratio > 1.5'
v4_buy['rules'][3]['condition'] = 'rsi_6 < 15 and close > ma_250 * 0.60'

v5_buy = copy.deepcopy(v2_buy)
v5_buy['rules'].append({
    'condition': 'rsi_6 < 30 and close < boll_lower * 1.02 and pettm_pct10y < 65',
    'action': 'buy',
    'position_ratio': 0.05,
    'reason': 'RSI短期超卖且价格触及布林下轨',
    'connector': 'OR'
})

# Sell: hold a bit more profit before exiting
v5_sell = copy.deepcopy(s0)
v5_sell['rules'][0]['condition'] = 'rsi_6 > 72'
v5_sell['rules'][1]['condition'] = 'rsi_6 > 82'
v5_sell['rules'][2]['condition'] = 'close < ma_60 * 0.93'

variants_7 = [
    ('v1_baseline', v1_buy, s0),
    ('v2_loosen',   v2_buy, s0),
    ('v3_volume',   v3_buy, s0),
    ('v4_strict',   v4_buy, s0),
    ('v5_combo',    v5_buy, v5_sell),
]
results_7 = test_variants(CYCLE_STOCKS, variants_7)
best_v7 = max(results_7, key=lambda x: score(x[1]))
print(f'  BEST: {best_v7[0]} -> annual={best_v7[1]["annual_return"]:.1f}%')

if best_v7[0] != 'v1_baseline':
    idx = [v[0] for v in variants_7].index(best_v7[0])
    best_buy_dict = variants_7[idx][1]
    best_buy_dict['name'] = '短线RSI超卖反弹买入'
    best_buy_dict['description'] = orig['短线RSI超卖反弹买入'].get('description', '') + ' [优化版]'
    optimized['短线RSI超卖反弹买入'] = best_buy_dict
    if best_v7[0] == 'v5_combo':
        v5_sell['name'] = '短线RSI超买止盈卖出'
        optimized['短线RSI超买止盈卖出'] = v5_sell

summary.append({
    'pair': '短线RSI超卖反弹/超买止盈',
    'best_variant': best_v7[0],
    'annual_return': best_v7[1]['annual_return'],
    'win_rate': best_v7[1]['win_rate'],
    'max_drawdown': best_v7[1]['max_drawdown'],
})

# ══════════════════════════════════════════════════════════════════════════════
# 8. 防御慢牛回调买入 + 防御慢牛顶部卖出
# ══════════════════════════════════════════════════════════════════════════════
print('\n=== 8. 防御慢牛回调买入 + 防御慢牛顶部卖出 ===')
b0 = orig['防御慢牛回调买入']
s0 = orig['防御慢牛顶部卖出']

v1_buy = copy.deepcopy(b0)

v2_buy = copy.deepcopy(b0)
v2_buy['rules'][0]['condition'] = 'close < w_ma_20 * 0.975 and m_rsi6_pct100 > 30 and m_rsi6_pct100 < 75'
v2_buy['rules'][1]['condition'] = 'w_rsi6_pct100 < 32 and m_rsi6_pct100 > 35 and close > ma_250 * 0.86'
v2_buy['rules'][2]['condition'] = 'rsi6_pct100 < 18 and m_rsi6_pct100 > 33 and close > ma_250 * 0.83'

v3_buy = copy.deepcopy(b0)
for r in v3_buy['rules']:
    if 'volume_ratio' not in r['condition']:
        r['condition'] = r['condition'] + ' and volume_ratio > 1.1'

v4_buy = copy.deepcopy(b0)
v4_buy['rules'][0]['condition'] = 'close < w_ma_20 * 0.965 and m_rsi6_pct100 > 38 and m_rsi6_pct100 < 70'
v4_buy['rules'][1]['condition'] = 'w_rsi6_pct100 < 25 and m_rsi6_pct100 > 42 and close > ma_250 * 0.90'
v4_buy['rules'][2]['condition'] = 'rsi6_pct100 < 12 and m_rsi6_pct100 > 40 and close > ma_250 * 0.87'

v5_buy = copy.deepcopy(v2_buy)
v5_buy['rules'].append({
    'condition': 'close < w_ma_20 * 0.98 and w_rsi6_pct100 < 35 and close > ma_250 * 0.88',
    'action': 'buy',
    'position_ratio': 0.07,
    'reason': '防御股回调至周均线下方，周线适度超卖',
    'connector': 'OR'
})
# Remove too-restrictive exclusion
v5_buy['exclusion_rules'] = [
    {'condition': 'm_rsi6_pct100 < 18', 'reason': '月线极度超卖，可能持续下跌'},
    {'condition': 'close < ma_250 * 0.80', 'reason': '已破年线较多，暂停买入'}
]

variants_8 = [
    ('v1_baseline', v1_buy, s0),
    ('v2_loosen',   v2_buy, s0),
    ('v3_volume',   v3_buy, s0),
    ('v4_strict',   v4_buy, s0),
    ('v5_combo',    v5_buy, s0),
]
results_8 = test_variants(DEFENSE_STOCKS, variants_8)
best_v8 = max(results_8, key=lambda x: score(x[1]))
print(f'  BEST: {best_v8[0]} -> annual={best_v8[1]["annual_return"]:.1f}%')

if best_v8[0] != 'v1_baseline':
    idx = [v[0] for v in variants_8].index(best_v8[0])
    best_buy_dict = variants_8[idx][1]
    best_buy_dict['name'] = '防御慢牛回调买入'
    best_buy_dict['description'] = orig['防御慢牛回调买入'].get('description', '') + ' [优化版]'
    optimized['防御慢牛回调买入'] = best_buy_dict

summary.append({
    'pair': '防御慢牛回调买入/顶部卖出',
    'best_variant': best_v8[0],
    'annual_return': best_v8[1]['annual_return'],
    'win_rate': best_v8[1]['win_rate'],
    'max_drawdown': best_v8[1]['max_drawdown'],
})

# ══════════════════════════════════════════════════════════════════════════════
# 9. 价值动量共振买入 + 价值动量共振卖出
# ══════════════════════════════════════════════════════════════════════════════
print('\n=== 9. 价值动量共振买入 + 价值动量共振卖出 ===')
b0 = orig['价值动量共振买入']
s0 = orig['价值动量共振卖出']

v1_buy = copy.deepcopy(b0)

v2_buy = copy.deepcopy(b0)
v2_buy['rules'][0]['condition'] = 'pettm_pct10y < 30 and macd_histogram > 0 and close > ma_20 and rsi6_pct100 < 60'
v2_buy['rules'][1]['condition'] = 'pettm_pct10y < 40 and w_rsi6_pct100 < 35 and macd_histogram > 0'
v2_buy['rules'][2]['condition'] = 'pettm_pct10y < 25 and rel_strength_10 > 2 and close > ma_60'
v2_buy['rules'][3]['condition'] = 'pettm_pct10y < 35 and rsi6_pct100 > 38 and rsi6_pct100 < 65 and volume_ratio > 1.3 and close > ma_20'

v3_buy = copy.deepcopy(b0)
for r in v3_buy['rules']:
    if 'volume_ratio' not in r['condition']:
        r['condition'] = r['condition'] + ' and volume_ratio > 1.2'

v4_buy = copy.deepcopy(b0)
v4_buy['rules'][0]['condition'] = 'pettm_pct10y < 20 and macd_histogram > 0 and close > ma_20 and rsi6_pct100 < 50'
v4_buy['rules'][1]['condition'] = 'pettm_pct10y < 28 and w_rsi6_pct100 < 25 and macd_histogram > 0'
v4_buy['rules'][2]['condition'] = 'pettm_pct10y < 18 and rel_strength_10 > 4 and close > ma_60'
v4_buy['rules'][3]['condition'] = 'pettm_pct10y < 25 and rsi6_pct100 > 42 and rsi6_pct100 < 60 and volume_ratio > 1.5 and close > ma_20'

v5_buy = copy.deepcopy(v2_buy)
v5_buy['rules'].append({
    'condition': 'pettm_pct10y < 30 and close > ma_60 and rsi6_pct100 > 35 and rsi6_pct100 < 58',
    'action': 'buy',
    'position_ratio': 0.09,
    'reason': '价值合理且价格在中期均线上方，动量温和',
    'connector': 'OR'
})

# Improved sell
v5_sell = copy.deepcopy(s0)
v5_sell['rules'][0]['condition'] = 'pettm_pct10y > 80 and macd_histogram < 0'
v5_sell['rules'][1]['condition'] = 'pettm_pct10y > 88'
v5_sell['rules'][2]['condition'] = 'close < ma_60 * 0.93 and macd_histogram < 0'

variants_9 = [
    ('v1_baseline', v1_buy, s0),
    ('v2_loosen',   v2_buy, s0),
    ('v3_volume',   v3_buy, s0),
    ('v4_strict',   v4_buy, s0),
    ('v5_combo',    v5_buy, v5_sell),
]
results_9 = test_variants(VALUE_STOCKS, variants_9)
best_v9 = max(results_9, key=lambda x: score(x[1]))
print(f'  BEST: {best_v9[0]} -> annual={best_v9[1]["annual_return"]:.1f}%')

if best_v9[0] != 'v1_baseline':
    idx = [v[0] for v in variants_9].index(best_v9[0])
    best_buy_dict = variants_9[idx][1]
    best_buy_dict['name'] = '价值动量共振买入'
    best_buy_dict['description'] = orig['价值动量共振买入'].get('description', '') + ' [优化版]'
    optimized['价值动量共振买入'] = best_buy_dict
    if best_v9[0] == 'v5_combo':
        v5_sell['name'] = '价值动量共振卖出'
        optimized['价值动量共振卖出'] = v5_sell

summary.append({
    'pair': '价值动量共振买入/卖出',
    'best_variant': best_v9[0],
    'annual_return': best_v9[1]['annual_return'],
    'win_rate': best_v9[1]['win_rate'],
    'max_drawdown': best_v9[1]['max_drawdown'],
})

# ══════════════════════════════════════════════════════════════════════════════
# Write output
# ══════════════════════════════════════════════════════════════════════════════
with open('data/strategies_optimized.json', 'w', encoding='utf-8') as f:
    json.dump(optimized, f, ensure_ascii=False, indent=2)

print('\n\n' + '='*70)
print('SUMMARY TABLE')
print('='*70)
print(f'{"Strategy Pair":<35} {"Best Variant":<15} {"AnnRet%":>8} {"WinRate":>8} {"MaxDD%":>8}')
print('-'*70)
for row in summary:
    print(f'{row["pair"]:<35} {row["best_variant"]:<15} {row["annual_return"]:>7.1f}% {row["win_rate"]:>7.1%} {row["max_drawdown"]:>7.1f}%')
print('='*70)
print('\nOptimized strategies written to: data/strategies_optimized.json')
