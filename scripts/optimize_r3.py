"""
Strategy optimization Round 3: Refine V7 (Sharpe=0.54) towards Sharpe>0.6
Key changes:
- Let winners run longer (sell at w_rsi6_pct100 > 75 instead of > 68)
- Relative strength filter to exclude underperformers
- Trend confirmation layer
- Also test pure Bollinger mean-reversion and a hybrid approach
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


configs_r3 = []

# V10: V7 refined — let winners run longer + add relative strength filter
configs_r3.append({
    'name': 'AI策略v10',
    'description': 'V10: V7改进 - 延长持有时间(卖出阈值提高)+加入相对强势过滤',
    'max_pos': 0.35,
    'buy_rules': [
        # Core: MACD + RSI + 20-day line + volume + NOT underperforming market
        {'cond': 'macd_histogram > 0 and macd > macd_signal and rsi_6 > 30 and rsi_6 < 62 and close > ma_20 and volume_ratio > 1.4 and rel_strength_10 > -2',
         'pos': 0.15, 'reason': 'MACD金叉+RSI适中+20日线+放量+不弱于大盘'},
        # Recovery from oversold with momentum turning
        {'cond': 'w_rsi6_pct100 < 40 and rsi_6 > 28 and close > ma_20 and boll_position > 0.3 and boll_position < 0.6 and volume_ratio > 1.2 and macd_histogram > -0.05',
         'pos': 0.13, 'reason': '周线低位+日线回升+布林中轨区+量比+MACD接近零轴'},
        # Momentum continuation
        {'cond': 'close > ma_20 and close > ma_60 and rel_strength_20 > 3 and macd_histogram > 0 and rsi_6 < 68',
         'pos': 0.07, 'reason': '双均线上+相对强势+MACD正，趋势延续'},
    ],
    'buy_exclusions': [
        {'cond': 'rsi_6 > 72', 'reason': 'RSI过高'},
        {'cond': 'overall_score < -50', 'reason': '大盘极弱'},
        {'cond': 'rel_strength_10 < -8', 'reason': '明显跑输大盘，不做弱势股'},
    ],
    'sell_rules': [
        # Let profits run longer: sell at RSI 75+ and weekly RSI 75+
        {'cond': 'w_rsi6_pct100 > 75 and rsi_6 > 68', 'pos': 0.5, 'reason': '周线RSI75分位+日线高位，分批止盈（放宽）'},
        {'cond': 'close < ma_20 * 0.97', 'pos': 1.0, 'reason': '破20日线3%，止损清仓'},
        {'cond': 'macd < macd_signal and macd_histogram < -0.05 and close < ma_20', 'pos': 0.6, 'reason': 'MACD死叉确认+跌破20日线'},
        {'cond': 'rsi_6 > 82 and pct_chg < -0.5', 'pos': 0.5, 'reason': '超买区转跌止盈'},
        {'cond': 'w_rsi6_pct100 > 82', 'pos': 0.4, 'reason': '周线RSI达历史高位，主动减仓'},
    ],
})

# V11: Pure Mean Reversion — Bollinger + extreme RSI
configs_r3.append({
    'name': 'AI策略v11',
    'description': 'V11: 均值回归纯策略 - 布林下轨+极端超卖，反弹至中轨止盈',
    'max_pos': 0.3,
    'buy_rules': [
        {'cond': 'boll_position < 0.15 and rsi_6 < 28 and volume_ratio > 1.5 and overall_score > -55',
         'pos': 0.15, 'reason': '布林极下轨+RSI极超卖+量能，均值回归买入'},
        {'cond': 'boll_position < 0.25 and rsi_6 < 32 and close > ma_60 * 0.92 and volume_ratio > 1.3',
         'pos': 0.10, 'reason': '布林下轨区+超卖+未破长线趋势，第二档买入'},
    ],
    'buy_exclusions': [
        {'cond': 'overall_score < -60', 'reason': '极端恐慌'},
        {'cond': 'close < ma_60 * 0.85', 'reason': '深度破长线，不抄底'},
    ],
    'sell_rules': [
        {'cond': 'boll_position > 0.5', 'pos': 0.5, 'reason': '价格回到布林中轨，减仓止盈'},
        {'cond': 'boll_position > 0.75', 'pos': 0.8, 'reason': '价格接近布林上轨，大幅止盈'},
        {'cond': 'close < ma_60 * 0.90', 'pos': 1.0, 'reason': '破60日线10%，止损清仓'},
        {'cond': 'rsi_6 > 75', 'pos': 0.5, 'reason': 'RSI进入超买区止盈'},
    ],
})

# V12: V7 core + V6 trend filter — only trade when long-term trend is up
configs_r3.append({
    'name': 'AI策略v12',
    'description': 'V12: MACD+趋势双确认 - 只在长期趋势向上时交易，提高胜率',
    'max_pos': 0.35,
    'buy_rules': [
        # MACD + trend + RSI: all three aligned
        {'cond': 'macd_histogram > 0 and macd > macd_signal and close > ma_60 and rsi_6 > 32 and rsi_6 < 60 and volume_ratio > 1.3',
         'pos': 0.15, 'reason': 'MACD金叉+价格在60日线上+RSI适中，趋势中的动量买入'},
        # Pullback in uptrend + weekly RSI low
        {'cond': 'close > ma_60 and w_rsi6_pct100 < 38 and rsi_6 > 25 and rsi_6 < 55 and volume_ratio > 1.4',
         'pos': 0.13, 'reason': '长线趋势+周线超跌+日线回升，趋势回调买入'},
        # Breakout confirmation
        {'cond': 'close > ma_20 and close > ma_60 and boll_position > 0.55 and boll_position < 0.75 and volume_ratio > 2.0 and rsi_6 < 70',
         'pos': 0.07, 'reason': '突破布林中上轨+均线上方+放量，确认突破加仓'},
    ],
    'buy_exclusions': [
        {'cond': 'close < ma_60', 'reason': '价格低于60日线，不做逆势'},
        {'cond': 'rsi_6 > 72', 'reason': 'RSI偏高'},
        {'cond': 'overall_score < -45', 'reason': '大盘较弱'},
    ],
    'sell_rules': [
        {'cond': 'w_rsi6_pct100 > 72', 'pos': 0.5, 'reason': '周线RSI达历史高位，止盈'},
        {'cond': 'close < ma_60 * 0.97', 'pos': 1.0, 'reason': '破60日线3%，止损'},
        {'cond': 'macd < macd_signal and close < ma_20', 'pos': 0.7, 'reason': 'MACD死叉+破20日线'},
        {'cond': 'rsi_6 > 80 and pct_chg < -0.3', 'pos': 0.5, 'reason': '超买区止盈'},
    ],
})

# V13: The "Best of All" — combine V7 (MACD) + V10 (let run) + V12 (trend filter)
configs_r3.append({
    'name': 'AI策略v13',
    'description': 'V13: 终极综合策略 - MACD+趋势+周线低位+量能+相对强势，全方位过滤',
    'max_pos': 0.4,
    'buy_rules': [
        # Tier 1: High confidence — 4 conditions aligned
        {'cond': 'macd_histogram > 0 and close > ma_20 and w_rsi6_pct100 < 42 and rsi_6 > 28 and rsi_6 < 60 and volume_ratio > 1.5',
         'pos': 0.15, 'reason': 'MACD正+站20日线+周线低位+RSI回升+放量，高置信度买入'},
        # Tier 2: Trend continuation on pullback
        {'cond': 'close > ma_60 and boll_position < 0.5 and rel_strength_10 > 0 and rsi_6 > 30 and rsi_6 < 58 and volume_ratio > 1.2',
         'pos': 0.13, 'reason': '长线趋势+回调至布林中轨下+相对强势，回调加仓'},
        # Tier 3: Momentum breakout
        {'cond': 'rel_strength_5 > 3 and close > ma_20 and macd_histogram > 0 and volume_ratio > 2.0 and rsi_6 < 70',
         'pos': 0.07, 'reason': '近期强势突破+放量+MACD正，追涨确认'},
        # Tier 4: deep oversold with volume surge (for crash recovery)
        {'cond': 'w_rsi6_pct100 < 20 and rsi_6 > 20 and rsi_6 < 48 and volume_ratio > 2.0 and overall_score > -60',
         'pos': 0.05, 'reason': '周线极超跌+日线初步回升+大量，底部抄底小仓'},
    ],
    'buy_exclusions': [
        {'cond': 'm_rsi6_pct100 > 80', 'reason': '月线过热'},
        {'cond': 'rsi_6 > 74', 'reason': 'RSI偏高不追'},
        {'cond': 'overall_score < -58', 'reason': '大盘极度恐慌'},
        {'cond': 'rel_strength_20 < -10', 'reason': '股票长期大幅跑输大盘，不做'},
    ],
    'sell_rules': [
        # Primary exit: weekly RSI high + daily overbought
        {'cond': 'w_rsi6_pct100 > 72', 'pos': 0.5, 'reason': '周线RSI达中高位，减半止盈'},
        {'cond': 'w_rsi6_pct100 > 82', 'pos': 0.8, 'reason': '周线RSI极高，大幅止盈'},
        # Stop loss: hard
        {'cond': 'close < ma_20 * 0.965', 'pos': 1.0, 'reason': '硬止损: 破20日线3.5%'},
        # Trend reversal
        {'cond': 'macd < macd_signal and macd_histogram < 0 and close < ma_20', 'pos': 0.6, 'reason': 'MACD死叉+破20日线，趋势反转'},
        # Overbought reversal
        {'cond': 'rsi_6 > 82 and pct_chg < -0.5', 'pos': 0.5, 'reason': '超买高位转跌'},
        # Volume distribution
        {'cond': 'volume_ratio > 2.5 and pct_chg < -2.5', 'pos': 0.8, 'reason': '放量大跌，出货信号'},
    ],
})


if __name__ == '__main__':
    all_sums = []
    for cfg in configs_r3:
        strat = make_strategy(cfg)
        results = run_batch(strat)
        sm = summarize(cfg['name'] + ': ' + cfg['description'], results)
        sm['name'] = cfg['name']
        all_sums.append(sm)

    print('\n\n' + '='*72)
    print('  Round 3 Summary (ranked by Sharpe)')
    print('='*72)
    print(f'  {"Name":<15} {"Annual%":>8} {"Return%":>8} {"MaxDD%":>8} {"Trades":>7} {"WR%":>7} {"Sharpe":>8}')
    print(f'  {"-"*60}')
    ranked = sorted(all_sums, key=lambda x: x['avg_sh'], reverse=True)
    for s in ranked:
        print(f'  {s["name"]:<15} {s["avg_ann"]:>8.1f} {s["avg_ret"]:>8.1f} {s["avg_dd"]:>8.1f} {s["avg_tr"]:>7.1f} {s["avg_wr"]:>7.1f} {s["avg_sh"]:>8.2f}')
    print()
    best = ranked[0]
    print(f'  Best(Sharpe): {best["name"]}  Annual={best["avg_ann"]:.1f}%  MaxDD={best["avg_dd"]:.1f}%  Sharpe={best["avg_sh"]:.2f}')
    best_ret = max(all_sums, key=lambda x: x['avg_ret'])
    print(f'  Best(Return): {best_ret["name"]}  Return={best_ret["avg_ret"]:.1f}%  Annual={best_ret["avg_ann"]:.1f}%  Sharpe={best_ret["avg_sh"]:.2f}')
    print(f'  Reference(V7): Annual=2.1%, MaxDD=-6.0%, Sharpe=0.54')
