"""
Iterative strategy optimizer: designs and backtests a high-performance strategy.
Tests multiple parameter sets across 6 A-share stocks (2019-2024 for full cycle).
"""
import sys, warnings, json
warnings.filterwarnings('ignore')
sys.path.insert(0, '.')
from quant_system.backtest import backtest_engine
from quant_system.strategy import QuantStrategy, merge_buy_sell_strategies

STOCKS = ['002594', '600900', '002371', '600309', '300750', '601318']
PERIOD = ('20190101', '20241231')  # Full cycle: 2019 bull + 2020 crash+recovery + 2021-2024 bear/recovery
CAPITAL = 500000

def make_strategy(version_config):
    """Build buy+sell strategy from config dict."""
    buy_s = QuantStrategy(version_config['name'] + '_买入', version_config['description'])
    buy_s.max_position_ratio = version_config.get('max_pos', 0.3)
    for r in version_config['buy_rules']:
        buy_s.add_rule(r['cond'], 'buy', r.get('pos', 0.15), r.get('reason', ''))
    for r in version_config.get('buy_exclusions', []):
        buy_s.add_exclusion_rule(r['cond'], r.get('reason', ''))

    sell_s = QuantStrategy(version_config['name'] + '_卖出', '配套卖出')
    sell_s.max_position_ratio = 1.0
    for r in version_config['sell_rules']:
        sell_s.add_rule(r['cond'], 'sell', r.get('pos', 0.5), r.get('reason', ''))

    return merge_buy_sell_strategies(buy_s, sell_s)

def run_batch(strategy, label=''):
    results = []
    for code in STOCKS:
        try:
            r = backtest_engine.run_backtest(code, strategy, PERIOD[0], PERIOD[1], CAPITAL)
            wr = r.win_rate if r.win_rate <= 100 else r.win_rate / 100
            results.append({
                'code': code,
                'ret': r.total_return_pct,
                'ann': r.annual_return,
                'dd': r.max_drawdown_pct,
                'trades': r.total_trades,
                'wr': wr,
                'sharpe': r.sharpe_ratio,
            })
        except Exception as e:
            results.append({'code': code, 'ret': 0, 'ann': 0, 'dd': 0, 'trades': 0, 'wr': 0, 'sharpe': 0, 'error': str(e)})
    return results

def print_summary(label, results):
    print(f'\n{"="*70}')
    print(f'  {label}')
    print(f'{"="*70}')
    print(f'  {"股票":<8} {"总收益%":>8} {"年化%":>7} {"回撤%":>8} {"交易数":>7} {"胜率%":>7} {"Sharpe":>8}')
    print(f'  {"-"*57}')
    valid = [r for r in results if r.get('trades', 0) > 0]
    for r in results:
        flag = '' if r.get('trades', 0) > 0 else ' [0交易]'
        print(f'  {r["code"]:<8} {r["ret"]:>8.1f} {r["ann"]:>7.1f} {r["dd"]:>8.1f} {r["trades"]:>7} {r["wr"]:>7.1f} {r["sharpe"]:>8.2f}{flag}')
    if valid:
        avg_ret = sum(r['ret'] for r in valid) / len(valid)
        avg_ann = sum(r['ann'] for r in valid) / len(valid)
        avg_dd = sum(r['dd'] for r in valid) / len(valid)
        avg_tr = sum(r['trades'] for r in valid) / len(valid)
        avg_wr = sum(r['wr'] for r in valid) / len(valid)
        avg_sh = sum(r['sharpe'] for r in valid) / len(valid)
        print(f'  {"-"*57}')
        print(f'  {"均值(有效)":<8} {avg_ret:>8.1f} {avg_ann:>7.1f} {avg_dd:>8.1f} {avg_tr:>7.1f} {avg_wr:>7.1f} {avg_sh:>8.2f}')
        print(f'  有效标的: {len(valid)}/{len(results)}')
    return {
        'avg_ret': sum(r['ret'] for r in valid)/len(valid) if valid else 0,
        'avg_ann': sum(r['ann'] for r in valid)/len(valid) if valid else 0,
        'avg_dd': sum(r['dd'] for r in valid)/len(valid) if valid else 0,
        'avg_tr': sum(r['trades'] for r in valid)/len(valid) if valid else 0,
        'avg_wr': sum(r['wr'] for r in valid)/len(valid) if valid else 0,
        'avg_sh': sum(r['sharpe'] for r in valid)/len(valid) if valid else 0,
    }


# ─── STRATEGY VERSIONS ────────────────────────────────────────────────────────

configs = []

# V1: Baseline - oversold bounce with volume
configs.append({
    'name': 'AI策略v1',
    'description': 'Baseline: 周线超跌+日线动量恢复+放量',
    'max_pos': 0.3,
    'buy_rules': [
        {'cond': 'w_rsi6_pct100 < 35 and rsi_6 > 22 and rsi_6 < 60 and volume_ratio > 1.5 and overall_score > -50',
         'pos': 0.15, 'reason': '周线超跌+日线动量回升+放量+大盘可'},
        {'cond': 'close > ma_20 and rel_strength_5 > 0 and w_rsi6_pct100 < 45 and rsi_6 < 58 and boll_position < 0.6',
         'pos': 0.15, 'reason': '20日线上+近期相对强势+周线低位+布林中下轨'},
    ],
    'buy_exclusions': [
        {'cond': 'm_rsi6_pct100 > 75', 'reason': '月线RSI过高，非超跌环境'},
        {'cond': 'pettm_pct10y > 80', 'reason': 'PE历史高位'},
        {'cond': 'overall_score < -60', 'reason': '大盘极度恐慌'},
    ],
    'sell_rules': [
        {'cond': 'w_rsi6_pct100 > 65', 'pos': 0.5, 'reason': '周线RSI回到中高位，止盈'},
        {'cond': 'close < ma_20 * 0.96', 'pos': 0.8, 'reason': '破20日线4%，止损'},
        {'cond': 'rsi_6 < 35 and close < ma_5', 'pos': 0.5, 'reason': '日线再度超卖+跌破5日线，趋势恶化止损'},
        {'cond': 'rsi_6 > 78 and boll_position > 0.85 and pct_chg < -0.3', 'pos': 0.5, 'reason': '高位转跌止盈'},
    ],
})

# V2: Add MA trend filter + tighter stop loss
configs.append({
    'name': 'AI策略v2',
    'description': 'V2: 增加均线趋势过滤，ma_5>ma_20确认短期趋势向上',
    'max_pos': 0.3,
    'buy_rules': [
        {'cond': 'w_rsi6_pct100 < 35 and rsi_6 > 25 and rsi_6 < 60 and volume_ratio > 1.5 and overall_score > -40 and close > ma_20',
         'pos': 0.15, 'reason': '周线超跌+日线动量回升+放量+大盘可+站20日线'},
        {'cond': 'ma_5 > ma_20 and w_rsi6_pct100 < 40 and rsi_6 < 58 and volume_ratio > 1.3 and rel_strength_10 > -2',
         'pos': 0.15, 'reason': '均线多头+周线低位+量比适中+近期不太弱'},
    ],
    'buy_exclusions': [
        {'cond': 'm_rsi6_pct100 > 70', 'reason': '月线RSI偏高'},
        {'cond': 'pettm_pct10y > 75', 'reason': 'PE历史高位'},
        {'cond': 'overall_score < -55', 'reason': '大盘极度恐慌'},
        {'cond': 'rsi_6 > 68', 'reason': 'RSI已进入偏高区，不追'},
    ],
    'sell_rules': [
        {'cond': 'w_rsi6_pct100 > 65', 'pos': 0.5, 'reason': '周线RSI回到中高位，止盈'},
        {'cond': 'close < ma_20 * 0.965', 'pos': 1.0, 'reason': '破20日线3.5%，全部止损'},
        {'cond': 'ma_5 < ma_20 and rsi_6 < 40', 'pos': 0.5, 'reason': '均线空头+RSI回落，趋势反转'},
        {'cond': 'rsi_6 > 80 and pct_chg < -0.5', 'pos': 0.5, 'reason': '高位转跌止盈'},
    ],
})

# V3: 三段式建仓 - scale in on dips
configs.append({
    'name': 'AI策略v3',
    'description': 'V3: 三段式分批建仓; 第1批超跌+量能; 第2批趋势确认; 强止损',
    'max_pos': 0.4,
    'buy_rules': [
        {'cond': 'w_rsi6_pct100 < 25 and rsi_6 > 20 and rsi_6 < 50 and overall_score > -55',
         'pos': 0.1, 'reason': '第1批: 周线极超跌+日线初步回升'},
        {'cond': 'w_rsi6_pct100 < 40 and close > ma_20 and volume_ratio > 1.5 and rsi_6 < 60',
         'pos': 0.1, 'reason': '第2批: 突破20日线+周线低位+放量'},
        {'cond': 'ma_5 > ma_20 and w_rsi6_pct100 < 50 and rel_strength_10 > 2 and rsi_6 < 65',
         'pos': 0.1, 'reason': '第3批: 均线多头+相对强势确认'},
    ],
    'buy_exclusions': [
        {'cond': 'm_rsi6_pct100 > 72', 'reason': '月线趋势偏高'},
        {'cond': 'overall_score < -60', 'reason': '极端市场恐慌'},
    ],
    'sell_rules': [
        {'cond': 'w_rsi6_pct100 > 70', 'pos': 0.5, 'reason': '周线RSI到中高位，分批止盈'},
        {'cond': 'close < ma_20 * 0.96', 'pos': 1.0, 'reason': '破20日线止损清仓'},
        {'cond': 'ma_5 < ma_20 and volume_ratio > 1.8 and pct_chg < -1.5', 'pos': 0.8, 'reason': '放量跌破均线，加速出逃'},
        {'cond': 'rsi_6 > 80', 'pos': 0.4, 'reason': '超买止盈'},
        {'cond': 'm_rsi6_pct100 > 80 and pct_chg < 0', 'pos': 0.3, 'reason': '月线过热且当日走弱'},
    ],
})

# V4: 趋势+动量共振策略 - focus on momentum stocks
configs.append({
    'name': 'AI策略v4',
    'description': 'V4: 趋势动量策略; 买入上升趋势中的回调低点; 适合成长型标的',
    'max_pos': 0.25,
    'buy_rules': [
        {'cond': 'close > ma_60 and close > ma_20 and ma_20 > ma_60 and boll_position < 0.45 and rsi_6 < 55 and volume_ratio > 1.2',
         'pos': 0.12, 'reason': '长期均线看涨+回调至布林中轨下方+量能适当'},
        {'cond': 'rel_strength_20 > 3 and w_rsi6_pct100 < 50 and rsi_6 > 30 and rsi_6 < 60 and close > ma_20',
         'pos': 0.13, 'reason': '20日相对强势+周线低位+日线适中+站20日线'},
    ],
    'buy_exclusions': [
        {'cond': 'rsi_6 > 70', 'reason': 'RSI过高'},
        {'cond': 'overall_score < -50', 'reason': '大盘极弱'},
        {'cond': 'boll_position > 0.8', 'reason': '布林带上轨附近不追'},
    ],
    'sell_rules': [
        {'cond': 'close < ma_20', 'pos': 0.5, 'reason': '跌破20日线减仓'},
        {'cond': 'close < ma_60 * 0.97', 'pos': 1.0, 'reason': '破60日线止损清仓'},
        {'cond': 'boll_position > 0.9 and rsi_6 > 75', 'pos': 0.5, 'reason': '布林上轨+超买止盈'},
        {'cond': 'rel_strength_5 < -5 and close < ma_5', 'pos': 0.6, 'reason': '急剧跑输大盘+跌破5日线'},
    ],
})

# V5: 精简版 - high precision, fewer trades
configs.append({
    'name': 'AI策略v5',
    'description': 'V5: 精简高胜率; 严格条件减少交易次数，但每笔胜率更高',
    'max_pos': 0.25,
    'buy_rules': [
        {'cond': 'w_rsi6_pct100 < 30 and rsi_6 > 28 and rsi_6 < 55 and close > ma_20 and volume_ratio > 1.8 and overall_score > -30 and rel_strength_5 >= 0',
         'pos': 0.20, 'reason': '严格5条件全满足: 周线低+日线回升+站20日线+放量+大盘正+不弱于大盘'},
    ],
    'buy_exclusions': [
        {'cond': 'm_rsi6_pct100 > 65', 'reason': '月线偏高'},
        {'cond': 'pettm_pct10y > 70', 'reason': 'PE偏高'},
        {'cond': 'overall_score < -50', 'reason': '大盘极弱'},
    ],
    'sell_rules': [
        {'cond': 'w_rsi6_pct100 > 60', 'pos': 0.6, 'reason': '周线RSI回到中高位，主动止盈'},
        {'cond': 'close < ma_20 * 0.97', 'pos': 1.0, 'reason': '破20日线3%止损'},
        {'cond': 'rsi_6 > 78 and pct_chg < -0.3', 'pos': 0.5, 'reason': '超买区转跌止盈'},
    ],
})


if __name__ == '__main__':
    all_summaries = []
    for cfg in configs:
        strat = make_strategy(cfg)
        results = run_batch(strat, cfg['name'])
        summary = print_summary(cfg['name'] + ': ' + cfg['description'], results)
        summary['name'] = cfg['name']
        all_summaries.append(summary)

    print('\n\n' + '='*70)
    print('  综合对比排名 (按年化收益率)')
    print('='*70)
    print(f'  {"策略":<15} {"年化%":>7} {"收益%":>8} {"回撤%":>8} {"交易数":>7} {"胜率%":>7} {"Sharpe":>8}')
    print(f'  {"-"*58}')
    ranked = sorted(all_summaries, key=lambda x: x['avg_ann'], reverse=True)
    for s in ranked:
        print(f'  {s["name"]:<15} {s["avg_ann"]:>7.1f} {s["avg_ret"]:>8.1f} {s["avg_dd"]:>8.1f} {s["avg_tr"]:>7.1f} {s["avg_wr"]:>7.1f} {s["avg_sh"]:>8.2f}')
    print()
    best = ranked[0]
    print(f'  最优策略: {best["name"]}  年化 {best["avg_ann"]:.1f}%  回撤 {best["avg_dd"]:.1f}%')
