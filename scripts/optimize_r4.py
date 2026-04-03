"""
Strategy optimization Round 4: Fine-tune V10 (best so far: Sharpe=0.63)
Goals: Sharpe > 0.70, reduce 002371 drawdown, maintain return levels
Key tweaks: volume threshold, trend filter, stop loss precision
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


configs_r4 = []

# V14: V10 + volume threshold raised + buy only on confirmed up day
configs_r4.append({
    'name': 'AI策略v14',
    'description': 'V14: V10+提高量比门槛+当日收阳确认，减少弱反弹假信号',
    'max_pos': 0.35,
    'buy_rules': [
        # Rule 1: MACD + volume 1.6 (raised from 1.4) + positive candle
        {'cond': 'macd_histogram > 0 and macd > macd_signal and rsi_6 > 30 and rsi_6 < 62 and close > ma_20 and volume_ratio > 1.6 and rel_strength_10 > -2 and pct_chg > 0',
         'pos': 0.15, 'reason': 'MACD金叉+放量>1.6+收阳+20日线+不弱于大盘'},
        # Rule 2: Weekly oversold + MACD neutral zone + volume
        {'cond': 'w_rsi6_pct100 < 40 and rsi_6 > 28 and close > ma_20 and boll_position > 0.3 and boll_position < 0.6 and volume_ratio > 1.3 and macd_histogram > -0.05',
         'pos': 0.13, 'reason': '周线低位+日线回升+布林中轨区+量比+MACD接近零轴'},
        # Rule 3: Momentum continuation
        {'cond': 'close > ma_20 and close > ma_60 and rel_strength_20 > 3 and macd_histogram > 0 and rsi_6 < 68',
         'pos': 0.07, 'reason': '双均线上+相对强势+MACD正，趋势延续'},
    ],
    'buy_exclusions': [
        {'cond': 'rsi_6 > 72', 'reason': 'RSI过高'},
        {'cond': 'overall_score < -50', 'reason': '大盘极弱'},
        {'cond': 'rel_strength_10 < -8', 'reason': '明显跑输大盘'},
        {'cond': 'volume_ratio < 0.5', 'reason': '缩量严重，避免流动性不足'},
    ],
    'sell_rules': [
        {'cond': 'w_rsi6_pct100 > 75 and rsi_6 > 68', 'pos': 0.5, 'reason': '周线75分位+日线高位，分批止盈'},
        {'cond': 'close < ma_20 * 0.97', 'pos': 1.0, 'reason': '破20日线3%，止损清仓'},
        {'cond': 'macd < macd_signal and macd_histogram < -0.05 and close < ma_20', 'pos': 0.6, 'reason': 'MACD死叉确认+破20日线'},
        {'cond': 'rsi_6 > 82 and pct_chg < -0.5', 'pos': 0.5, 'reason': '超买区转跌止盈'},
        {'cond': 'w_rsi6_pct100 > 82', 'pos': 0.4, 'reason': '周线RSI极高，主动减仓'},
    ],
})

# V15: V10 + add MA60 filter to rule 1 (only buy when in long-term uptrend)
configs_r4.append({
    'name': 'AI策略v15',
    'description': 'V15: V10+MA60趋势过滤，只在长线趋势向上时触发Rule1',
    'max_pos': 0.35,
    'buy_rules': [
        # Rule 1 + MA60 trend filter
        {'cond': 'macd_histogram > 0 and macd > macd_signal and rsi_6 > 30 and rsi_6 < 62 and close > ma_20 and close > ma_60 and volume_ratio > 1.4 and rel_strength_10 > -2',
         'pos': 0.15, 'reason': 'MACD金叉+60日线趋势+RSI适中+20日线+量比+不弱于大盘'},
        # Rule 2 with looser weekly RSI threshold
        {'cond': 'w_rsi6_pct100 < 40 and rsi_6 > 28 and close > ma_20 and boll_position > 0.3 and boll_position < 0.6 and volume_ratio > 1.2',
         'pos': 0.13, 'reason': '周线低位+日线回升+布林中轨区+量比'},
        # Rule 3: Momentum
        {'cond': 'close > ma_20 and close > ma_60 and rel_strength_20 > 3 and macd_histogram > 0 and rsi_6 < 68',
         'pos': 0.07, 'reason': '双均线+相对强势+MACD正'},
    ],
    'buy_exclusions': [
        {'cond': 'rsi_6 > 72', 'reason': 'RSI过高'},
        {'cond': 'overall_score < -50', 'reason': '大盘极弱'},
        {'cond': 'rel_strength_10 < -8', 'reason': '明显跑输大盘'},
    ],
    'sell_rules': [
        {'cond': 'w_rsi6_pct100 > 75 and rsi_6 > 68', 'pos': 0.5, 'reason': '周线75分位+日线高位，止盈'},
        {'cond': 'close < ma_20 * 0.97', 'pos': 1.0, 'reason': '破20日线3%，止损'},
        {'cond': 'macd < macd_signal and macd_histogram < -0.05 and close < ma_20', 'pos': 0.6, 'reason': 'MACD死叉+破20日线'},
        {'cond': 'rsi_6 > 82 and pct_chg < -0.5', 'pos': 0.5, 'reason': '超买区转跌止盈'},
        {'cond': 'w_rsi6_pct100 > 82', 'pos': 0.4, 'reason': '周线RSI极高，减仓'},
    ],
})

# V16: V10 with position sizing tweak — larger bet on high-confidence signal
configs_r4.append({
    'name': 'AI策略v16',
    'description': 'V16: V10调仓大小 - 高置信度信号(Rule1)加大到0.20，低置信度缩小',
    'max_pos': 0.4,
    'buy_rules': [
        # Rule 1: Higher confidence = bigger position (0.20 vs 0.15)
        {'cond': 'macd_histogram > 0 and macd > macd_signal and rsi_6 > 30 and rsi_6 < 62 and close > ma_20 and volume_ratio > 1.4 and rel_strength_10 > -2',
         'pos': 0.20, 'reason': 'MACD金叉+RSI适中+20日线+量比+不弱，高置信度建仓'},
        # Rule 2: Medium confidence = medium position (0.10 vs 0.13)
        {'cond': 'w_rsi6_pct100 < 40 and rsi_6 > 28 and close > ma_20 and boll_position > 0.3 and boll_position < 0.6 and volume_ratio > 1.2 and macd_histogram > -0.05',
         'pos': 0.10, 'reason': '周线低位+日线回升+中置信度'},
        # Rule 3: Lower confidence = small position (0.05 vs 0.07)
        {'cond': 'close > ma_20 and close > ma_60 and rel_strength_20 > 3 and macd_histogram > 0 and rsi_6 < 68',
         'pos': 0.05, 'reason': '趋势延续确认，小仓加仓'},
    ],
    'buy_exclusions': [
        {'cond': 'rsi_6 > 72', 'reason': 'RSI过高'},
        {'cond': 'overall_score < -50', 'reason': '大盘极弱'},
        {'cond': 'rel_strength_10 < -8', 'reason': '明显跑输大盘'},
    ],
    'sell_rules': [
        {'cond': 'w_rsi6_pct100 > 75 and rsi_6 > 68', 'pos': 0.5, 'reason': '周线75分位+日线高位，止盈'},
        {'cond': 'close < ma_20 * 0.97', 'pos': 1.0, 'reason': '破20日线3%，止损'},
        {'cond': 'macd < macd_signal and macd_histogram < -0.05 and close < ma_20', 'pos': 0.6, 'reason': 'MACD死叉+破20日线'},
        {'cond': 'rsi_6 > 82 and pct_chg < -0.5', 'pos': 0.5, 'reason': '超买区转跌止盈'},
        {'cond': 'w_rsi6_pct100 > 82', 'pos': 0.4, 'reason': '周线RSI极高，减仓'},
    ],
})

# V17: V10 + IDX momentum filter (only buy when market has short-term tailwind)
configs_r4.append({
    'name': 'AI策略v17',
    'description': 'V17: V10+大盘短期顺风，idx_ret_5>-3时才入场（顺势而为）',
    'max_pos': 0.35,
    'buy_rules': [
        {'cond': 'macd_histogram > 0 and macd > macd_signal and rsi_6 > 30 and rsi_6 < 62 and close > ma_20 and volume_ratio > 1.4 and rel_strength_10 > -2 and idx_ret_5 > -3',
         'pos': 0.15, 'reason': 'MACD金叉+RSI适中+20日线+量比+不弱于大盘+大盘近5日未大跌'},
        {'cond': 'w_rsi6_pct100 < 40 and rsi_6 > 28 and close > ma_20 and boll_position > 0.3 and boll_position < 0.6 and volume_ratio > 1.2 and macd_histogram > -0.05',
         'pos': 0.13, 'reason': '周线低位+日线回升+布林中轨+量比'},
        {'cond': 'close > ma_20 and close > ma_60 and rel_strength_20 > 3 and macd_histogram > 0 and rsi_6 < 68 and idx_ret_20 > -5',
         'pos': 0.07, 'reason': '双均线上+相对强势+MACD正+大盘未大跌，趋势延续'},
    ],
    'buy_exclusions': [
        {'cond': 'rsi_6 > 72', 'reason': 'RSI过高'},
        {'cond': 'overall_score < -50', 'reason': '大盘极弱'},
        {'cond': 'rel_strength_10 < -8', 'reason': '明显跑输大盘'},
        {'cond': 'idx_ret_5 < -5', 'reason': '大盘5日跌幅超5%，市场极度弱势'},
    ],
    'sell_rules': [
        {'cond': 'w_rsi6_pct100 > 75 and rsi_6 > 68', 'pos': 0.5, 'reason': '周线75分位+日线高位，止盈'},
        {'cond': 'close < ma_20 * 0.97', 'pos': 1.0, 'reason': '破20日线3%，止损'},
        {'cond': 'macd < macd_signal and macd_histogram < -0.05 and close < ma_20', 'pos': 0.6, 'reason': 'MACD死叉+破20日线'},
        {'cond': 'rsi_6 > 82 and pct_chg < -0.5', 'pos': 0.5, 'reason': '超买区转跌止盈'},
        {'cond': 'w_rsi6_pct100 > 82', 'pos': 0.4, 'reason': '周线RSI极高，减仓'},
        {'cond': 'idx_ret_5 < -6 and pct_chg < -2', 'pos': 0.5, 'reason': '大盘暴跌+股票当日大跌，保护性减仓'},
    ],
})


if __name__ == '__main__':
    all_sums = []
    for cfg in configs_r4:
        strat = make_strategy(cfg)
        results = run_batch(strat)
        sm = summarize(cfg['name'] + ': ' + cfg['description'], results)
        sm['name'] = cfg['name']
        all_sums.append(sm)

    print('\n\n' + '='*72)
    print('  Round 4 Summary (ranked by Sharpe)')
    print('='*72)
    print(f'  {"Name":<15} {"Annual%":>8} {"Return%":>8} {"MaxDD%":>8} {"Trades":>7} {"WR%":>7} {"Sharpe":>8}')
    print(f'  {"-"*60}')
    ranked = sorted(all_sums, key=lambda x: x['avg_sh'], reverse=True)
    for s in ranked:
        print(f'  {s["name"]:<15} {s["avg_ann"]:>8.1f} {s["avg_ret"]:>8.1f} {s["avg_dd"]:>8.1f} {s["avg_tr"]:>7.1f} {s["avg_wr"]:>7.1f} {s["avg_sh"]:>8.2f}')
    print()
    best = ranked[0]
    print(f'  Best(Sharpe): {best["name"]}  Annual={best["avg_ann"]:.1f}%  MaxDD={best["avg_dd"]:.1f}%  Sharpe={best["avg_sh"]:.2f}')
    print(f'  Reference(V10): Annual=2.9%, MaxDD=-7.0%, Sharpe=0.63')
