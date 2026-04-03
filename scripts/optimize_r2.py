"""
Strategy optimization Round 2: V6, V7, V8 - building on Round 1 insights.
V3 was best (17.9% avg, 59.6% win), driven by 300750 (+92.8%).
Now optimize: trend-following with pullback entries + better stop loss.
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


configs_r2 = []

# V6: Trend-following + pullback — refined from V3
# Key insight: buy trend pullbacks not just oversold; use MA60 as trend filter
configs_r2.append({
    'name': 'AI策略v6',
    'description': 'V6: 趋势回调策略 - 长期趋势向上+回调至中轨下方+周线低位，跟踪牛市标的',
    'max_pos': 0.4,
    'buy_rules': [
        # Rule 1: Strong uptrend pullback
        {'cond': 'close > ma_60 and ma_20 > ma_60 and boll_position < 0.45 and rsi_6 < 55 and volume_ratio > 1.5 and overall_score > -40',
         'pos': 0.15, 'reason': '长期趋势向上+回调至布林中下轨+放量确认，趋势回调买点1'},
        # Rule 2: Weekly RSI oversold in uptrend
        {'cond': 'close > ma_60 and w_rsi6_pct100 < 35 and rsi_6 > 22 and rsi_6 < 58 and volume_ratio > 1.3',
         'pos': 0.15, 'reason': '长期趋势向上+周线超跌+日线回升，周线底部买点'},
        # Rule 3: Relative strength + trend confirmation
        {'cond': 'rel_strength_10 > 2 and close > ma_20 and w_rsi6_pct100 < 45 and boll_position < 0.6 and rsi_6 < 60',
         'pos': 0.10, 'reason': '近10日相对强势+站20日线+周线低位，相对强势确认加仓'},
    ],
    'buy_exclusions': [
        {'cond': 'close < ma_60 * 0.95', 'reason': '价格大幅跌破60日线，趋势已破坏'},
        {'cond': 'm_rsi6_pct100 > 80', 'reason': '月线RSI过高，牛市晚期'},
        {'cond': 'overall_score < -55', 'reason': '大盘极度恐慌'},
        {'cond': 'rsi_6 > 70', 'reason': 'RSI已偏高，不追入'},
    ],
    'sell_rules': [
        {'cond': 'w_rsi6_pct100 > 70 and rsi_6 > 65', 'pos': 0.5, 'reason': '周线RSI70分位+日线偏高，分批止盈'},
        {'cond': 'close < ma_60 * 0.97', 'pos': 1.0, 'reason': '跌破60日线3%，趋势止损清仓'},
        {'cond': 'boll_position > 0.92 and rsi_6 > 78', 'pos': 0.5, 'reason': '布林上轨附近+超买，止盈'},
        {'cond': 'ma_5 < ma_20 and volume_ratio > 2.0 and pct_chg < -2', 'pos': 0.8, 'reason': '放量破均线，加速出逃'},
        {'cond': 'm_rsi6_pct100 > 85', 'pos': 0.3, 'reason': '月线RSI进入极高区，泡沫减仓'},
    ],
})

# V7: MACD momentum + RSI confirmation — catches breakouts
configs_r2.append({
    'name': 'AI策略v7',
    'description': 'V7: MACD动量策略 - MACD金叉+布林中轨上+量能，适合动量型标的',
    'max_pos': 0.35,
    'buy_rules': [
        # Rule 1: MACD gold cross + RSI sweet spot + volume
        {'cond': 'macd_histogram > 0 and macd > macd_signal and rsi_6 > 30 and rsi_6 < 62 and close > ma_20 and volume_ratio > 1.4',
         'pos': 0.15, 'reason': 'MACD金叉+RSI适中+站20日线+放量，动量确认'},
        # Rule 2: RSI oversold recovery + price trend
        {'cond': 'w_rsi6_pct100 < 40 and rsi_6 > 28 and close > ma_20 and boll_position > 0.3 and boll_position < 0.6 and volume_ratio > 1.2',
         'pos': 0.13, 'reason': '周线超跌+日线回升过20日线+布林中轨区间，底部买入'},
        # Rule 3: Breakout + momentum
        {'cond': 'close > ma_20 and close > ma_60 and rel_strength_20 > 3 and macd_histogram > 0 and rsi_6 < 68',
         'pos': 0.07, 'reason': '价格在双均线上+相对强势+MACD正区间，趋势延续加仓'},
    ],
    'buy_exclusions': [
        {'cond': 'rsi_6 > 72', 'reason': 'RSI过高'},
        {'cond': 'overall_score < -50', 'reason': '大盘极弱'},
        {'cond': 'macd_histogram < -0.1 and rsi_6 < 30', 'reason': 'MACD负区间且超跌，不抄底'},
    ],
    'sell_rules': [
        {'cond': 'macd < macd_signal and macd_histogram < 0 and close < ma_20', 'pos': 0.6, 'reason': 'MACD死叉+跌破20日线，趋势反转止损'},
        {'cond': 'rsi_6 > 80 and pct_chg < -0.3', 'pos': 0.5, 'reason': '超买区转跌止盈'},
        {'cond': 'close < ma_20 * 0.96', 'pos': 1.0, 'reason': '强制止损4%'},
        {'cond': 'w_rsi6_pct100 > 72', 'pos': 0.4, 'reason': '周线RSI偏高区，分批止盈'},
    ],
})

# V8: Hybrid best-of-breed — combines V3's scale-in with V6's trend filter
configs_r2.append({
    'name': 'AI策略v8',
    'description': 'V8: 最优综合策略 - 趋势过滤+周线超跌+分批建仓+动量确认+严格止损',
    'max_pos': 0.4,
    'buy_rules': [
        # Tier 1: Weekly oversold + daily bounce, no trend requirement
        {'cond': 'w_rsi6_pct100 < 28 and rsi_6 > 22 and rsi_6 < 52 and volume_ratio > 1.5 and overall_score > -50',
         'pos': 0.10, 'reason': '第1批: 周线极超跌+日线初步回升+量能，超跌建仓'},
        # Tier 2: Trend confirmed + weekly low
        {'cond': 'close > ma_20 and w_rsi6_pct100 < 40 and macd_histogram > 0 and rsi_6 < 60 and volume_ratio > 1.3',
         'pos': 0.13, 'reason': '第2批: 趋势确认(站20日线+MACD正)+周线低位，趋势加仓'},
        # Tier 3: Momentum phase
        {'cond': 'ma_5 > ma_20 and close > ma_60 and rel_strength_10 > 2 and w_rsi6_pct100 < 55 and rsi_6 < 65',
         'pos': 0.10, 'reason': '第3批: 均线多头+长线趋势+相对强势，顺势追加'},
        # Tier 4: Relative strength breakout
        {'cond': 'rel_strength_5 > 3 and rel_strength_20 > 5 and boll_position > 0.55 and boll_position < 0.8 and rsi_6 < 72 and volume_ratio > 2.0',
         'pos': 0.07, 'reason': '第4批: 近期强势突破+放量，确认上涨行情'},
    ],
    'buy_exclusions': [
        {'cond': 'm_rsi6_pct100 > 78', 'reason': '月线RSI过高，非超跌买点'},
        {'cond': 'overall_score < -60', 'reason': '极度市场恐慌'},
        {'cond': 'rsi_6 > 74', 'reason': 'RSI偏高，不追'},
    ],
    'sell_rules': [
        {'cond': 'w_rsi6_pct100 > 68', 'pos': 0.5, 'reason': '周线RSI到中高区，止盈减半'},
        {'cond': 'close < ma_20 * 0.965', 'pos': 1.0, 'reason': '破20日线3.5%，硬止损清仓'},
        {'cond': 'close < ma_60 and ma_5 < ma_20', 'pos': 0.8, 'reason': '破60日线+均线空头，趋势恶化'},
        {'cond': 'rsi_6 > 82 and pct_chg < -0.5', 'pos': 0.5, 'reason': '超买区转跌，止盈'},
        {'cond': 'volume_ratio > 2.5 and pct_chg < -3', 'pos': 0.8, 'reason': '大量暴跌，主力出货止损'},
    ],
})

# V9: V3 + 改进版 — V3 had best returns, keep structure but fix 002594/601318 losses
configs_r2.append({
    'name': 'AI策略v9',
    'description': 'V9: V3改进版 - 增加大盘过滤+PE排除+止损更紧，降低回撤',
    'max_pos': 0.35,
    'buy_rules': [
        {'cond': 'w_rsi6_pct100 < 25 and rsi_6 > 22 and rsi_6 < 50 and overall_score > -45 and pettm_pct10y < 70',
         'pos': 0.10, 'reason': '第1批: 周线极超跌+日线回升+大盘可+PE合理'},
        {'cond': 'w_rsi6_pct100 < 40 and close > ma_20 and volume_ratio > 1.6 and rsi_6 < 58 and overall_score > -30',
         'pos': 0.12, 'reason': '第2批: 突破20日线确认+周线低位+放量+大盘正面'},
        {'cond': 'ma_5 > ma_20 and w_rsi6_pct100 < 48 and rel_strength_10 > 2 and rsi_6 < 62 and volume_ratio > 1.3',
         'pos': 0.08, 'reason': '第3批: 均线多头+相对强势，趋势加仓'},
    ],
    'buy_exclusions': [
        {'cond': 'm_rsi6_pct100 > 72', 'reason': '月线趋势偏高'},
        {'cond': 'overall_score < -55', 'reason': '极端恐慌'},
        {'cond': 'pettm_pct10y > 85', 'reason': 'PE极高位'},
        {'cond': 'close < ma_60 * 0.93', 'reason': '大幅破60日线，趋势已破坏'},
    ],
    'sell_rules': [
        {'cond': 'w_rsi6_pct100 > 65', 'pos': 0.5, 'reason': '周线回到中位，减仓止盈'},
        {'cond': 'close < ma_20 * 0.965', 'pos': 1.0, 'reason': '破20日线3.5%，全额止损'},
        {'cond': 'ma_5 < ma_20 and volume_ratio > 1.8 and pct_chg < -1.5', 'pos': 0.8, 'reason': '放量破均线，快速止损'},
        {'cond': 'rsi_6 > 80', 'pos': 0.4, 'reason': '超买止盈'},
        {'cond': 'm_rsi6_pct100 > 82 and pct_chg < 0', 'pos': 0.3, 'reason': '月线高位+当日走弱，减仓'},
    ],
})


if __name__ == '__main__':
    all_sums = []
    for cfg in configs_r2:
        strat = make_strategy(cfg)
        results = run_batch(strat)
        sm = summarize(cfg['name'] + ': ' + cfg['description'], results)
        sm['name'] = cfg['name']
        all_sums.append(sm)

    print('\n\n' + '='*72)
    print('  Round 2 Summary (ranked by Annual Return)')
    print('='*72)
    print(f'  {"Name":<15} {"Annual%":>8} {"Return%":>8} {"MaxDD%":>8} {"Trades":>7} {"WR%":>7} {"Sharpe":>8}')
    print(f'  {"-"*60}')
    ranked = sorted(all_sums, key=lambda x: x['avg_ann'], reverse=True)
    for s in ranked:
        print(f'  {s["name"]:<15} {s["avg_ann"]:>8.1f} {s["avg_ret"]:>8.1f} {s["avg_dd"]:>8.1f} {s["avg_tr"]:>7.1f} {s["avg_wr"]:>7.1f} {s["avg_sh"]:>8.2f}')
    print()
    print(f'  Best: {ranked[0]["name"]}  Annual={ranked[0]["avg_ann"]:.1f}%  MaxDD={ranked[0]["avg_dd"]:.1f}%  Sharpe={ranked[0]["avg_sh"]:.2f}')
    print(f'  (R1 best v3: Annual=2.4%, MaxDD=-14.0%, Sharpe=0.26)')
