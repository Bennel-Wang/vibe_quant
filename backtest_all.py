import sys, os
sys.path.insert(0, 'D:\\Git_project\\vibecoding_quant')
os.chdir('D:\\Git_project\\vibecoding_quant')

from quant_system.backtest import BacktestEngine
from quant_system.strategy import QuantStrategy
import json

engine = BacktestEngine()

START = '20190101'
END = '20250101'

def run_bt(stock, rules, max_pos, description="test"):
    strategy_dict = {
        "name": "test",
        "description": description,
        "max_position_ratio": max_pos,
        "market_regime": [],
        "rules": rules,
        "exclusion_rules": []
    }
    try:
        s = QuantStrategy.from_dict(strategy_dict)
        result = engine.run_backtest(stock, s, START, END)
        return result.annual_return, result.total_trades, result.win_rate
    except Exception as e:
        print(f"  ERROR {stock}: {e}")
        return None, 0, 0

def test_versions(pair_name, stocks, max_pos, versions):
    print(f"\n{'='*60}")
    print(f"PAIR: {pair_name}")
    print(f"{'='*60}")
    results = {}
    for vname, rules in versions.items():
        totals = []
        for stock in stocks:
            ann, trades, wr = run_bt(stock, rules, max_pos)
            if ann is not None:
                print(f"  {vname} {stock}: annual={ann:.1f}% trades={trades} win={wr:.0%}")
                totals.append((ann, trades, wr))
            else:
                print(f"  {vname} {stock}: FAILED")
        if totals:
            avg_ann = sum(x[0] for x in totals) / len(totals)
            min_trades = min(x[1] for x in totals)
            avg_wr = sum(x[2] for x in totals) / len(totals)
            results[vname] = {
                'avg_annual': avg_ann,
                'min_trades': min_trades,
                'avg_win_rate': avg_wr,
                'raw': totals,
                'rules': rules
            }
            print(f"  {vname} SUMMARY: avg_annual={avg_ann:.1f}% min_trades={min_trades} avg_win={avg_wr:.0%}")
    
    # Pick best: trades >= 3 AND win_rate >= 35%
    valid = {k: v for k, v in results.items() if v['min_trades'] >= 3 and v['avg_win_rate'] >= 0.35}
    if not valid:
        print(f"  WARNING: No version passes constraints, relaxing to trades>=2 and win>=25%")
        valid = {k: v for k, v in results.items() if v['min_trades'] >= 2 and v['avg_win_rate'] >= 0.25}
    if not valid:
        valid = results
    
    best = max(valid.items(), key=lambda x: x[1]['avg_annual'])
    print(f"\n  WINNER: {best[0]} with avg_annual={best[1]['avg_annual']:.1f}%")
    return best[0], best[1]['rules']

###############################################################################
# PAIR 1: 4421极限底部买入
###############################################################################
pair1_stocks = ['600519', '000858', '600309']
pair1_max_pos = 0.50

pair1_versions = {
    'v1': [{"condition": "m_rsi6_pct100 < 12 and w_rsi6_pct100 < 12", "action": "buy", "position_ratio": 0.12, "reason": "月线周线RSI双低", "connector": "OR"}],
    'v2': [{"condition": "w_rsi6_pct100 < 18 and idx_ret_60 < -8", "action": "buy", "position_ratio": 0.12, "reason": "周线超卖+市场大跌", "connector": "OR"}],
    'v3': [{"condition": "m_rsi6_pct100 < 15 and pettm_pct10y < 25 and idx_ret_20 < -4", "action": "buy", "position_ratio": 0.12, "reason": "月线超卖+PE低+市场跌", "connector": "OR"}],
    'v4': [
        {"condition": "m_rsi6_pct100 < 8", "action": "buy", "position_ratio": 0.14, "reason": "月线极值超卖", "connector": "OR"},
        {"condition": "w_rsi6_pct100 < 10 and idx_ret_60 < -10", "action": "buy", "position_ratio": 0.10, "reason": "周线极值+市场大跌", "connector": "OR"}
    ],
    'v5': [{"condition": "w_rsi6_pct100 < 20 and rsi6_pct100 < 12 and volume_ratio > 1.5", "action": "buy", "position_ratio": 0.12, "reason": "周线超卖+日线极低+放量", "connector": "OR"}],
}

p1_winner, p1_rules = test_versions("4421极限底部买入", pair1_stocks, pair1_max_pos, pair1_versions)

###############################################################################
# PAIR 2: 低PE价值底部买入
###############################################################################
pair2_stocks = ['600519', '000858', '600309']
pair2_max_pos = 0.45

pair2_versions = {
    'v1': [{"condition": "pettm_pct10y < 20", "action": "buy", "position_ratio": 0.12, "reason": "PE历史低位", "connector": "OR"}],
    'v2': [{"condition": "pettm_pct10y < 30 and rsi_6 < 40", "action": "buy", "position_ratio": 0.12, "reason": "PE低+RSI超卖", "connector": "OR"}],
    'v3': [{"condition": "pettm_pct10y < 25 and (close < ma_60 or rsi6_pct100 < 30)", "action": "buy", "position_ratio": 0.12, "reason": "PE低+价格或RSI低", "connector": "OR"}],
    'v4': [{"condition": "pettm_pct10y < 35 and w_rsi6_pct100 < 30 and idx_ret_20 < -2", "action": "buy", "position_ratio": 0.12, "reason": "PE低+周线超卖+市场弱", "connector": "OR"}],
    'v5': [
        {"condition": "pettm_pct10y < 15", "action": "buy", "position_ratio": 0.15, "reason": "PE极低历史底部", "connector": "OR"},
        {"condition": "pettm_pct10y < 30 and rsi6_pct100 < 15", "action": "buy", "position_ratio": 0.12, "reason": "PE低+日线极低", "connector": "OR"}
    ],
}

p2_winner, p2_rules = test_versions("低PE价值底部买入", pair2_stocks, pair2_max_pos, pair2_versions)

###############################################################################
# PAIR 3: MACD金叉趋势买入
###############################################################################
pair3_stocks = ['300750', '002594', '002371', '300274']
pair3_max_pos = 0.35

pair3_versions = {
    'v1': [{"condition": "macd_histogram > 0 and close > ma_20 and ma_5 > ma_20", "action": "buy", "position_ratio": 0.10, "reason": "MACD金叉+均线多头", "connector": "OR"}],
    'v2': [{"condition": "macd_histogram > 0 and macd_histogram > macd_signal and close > ma_60", "action": "buy", "position_ratio": 0.10, "reason": "MACD金叉+60日均线上方", "connector": "OR"}],
    'v3': [{"condition": "macd_histogram > 0 and close > ma_20 and rsi6_pct100 > 40 and rsi6_pct100 < 72", "action": "buy", "position_ratio": 0.10, "reason": "MACD金叉+RSI适中", "connector": "OR"}],
    'v4': [{"condition": "macd_histogram > 0 and ma_5 > ma_20 and ma_20 > ma_60 and volume_ratio > 1.3", "action": "buy", "position_ratio": 0.10, "reason": "MACD金叉+多头均线+放量", "connector": "OR"}],
    'v5': [{"condition": "macd_histogram > 0 and rel_strength_20 > 0 and close > ma_20", "action": "buy", "position_ratio": 0.10, "reason": "MACD金叉+相对强势", "connector": "OR"}],
}

p3_winner, p3_rules = test_versions("MACD金叉趋势买入", pair3_stocks, pair3_max_pos, pair3_versions)

###############################################################################
# PAIR 4: 周线超卖均值回归买入
###############################################################################
pair4_stocks = ['600519', '600309', '002920']
pair4_max_pos = 0.40

pair4_versions = {
    'v1': [{"condition": "w_rsi6_pct100 < 15", "action": "buy", "position_ratio": 0.12, "reason": "周线RSI历史低位", "connector": "OR"}],
    'v2': [{"condition": "w_rsi6_pct100 < 20 and rsi_6 < 35", "action": "buy", "position_ratio": 0.12, "reason": "周线超卖+日线超卖", "connector": "OR"}],
    'v3': [{"condition": "w_rsi6_pct100 < 18 and close > ma_250 * 0.70", "action": "buy", "position_ratio": 0.12, "reason": "周线超卖+价格不低于年线70%", "connector": "OR"}],
    'v4': [{"condition": "w_rsi6_pct100 < 22 and w_rsi_6 < 30 and idx_ret_20 > -15", "action": "buy", "position_ratio": 0.12, "reason": "周线双指标超卖+市场未极端", "connector": "OR"}],
    'v5': [
        {"condition": "w_rsi6_pct100 < 12", "action": "buy", "position_ratio": 0.15, "reason": "周线RSI历史极低", "connector": "OR"},
        {"condition": "w_rsi6_pct100 < 25 and rsi6_pct100 < 10 and volume_ratio > 1.4", "action": "buy", "position_ratio": 0.12, "reason": "周线+日线双超卖+放量", "connector": "OR"}
    ],
}

p4_winner, p4_rules = test_versions("周线超卖均值回归买入", pair4_stocks, pair4_max_pos, pair4_versions)

###############################################################################
# PAIR 5: 布林超跌价格保护买入
###############################################################################
pair5_stocks = ['600519', '600309', '002572']
pair5_max_pos = 0.25

pair5_versions = {
    'v1': [{"condition": "boll_position < 0.08 and rsi_6 < 30", "action": "buy", "position_ratio": 0.08, "reason": "布林下轨+RSI超卖", "connector": "OR"}],
    'v2': [{"condition": "boll_position < 0.05", "action": "buy", "position_ratio": 0.08, "reason": "布林极下轨", "connector": "OR"}],
    'v3': [{"condition": "boll_position < 0.10 and rsi6_pct100 < 12", "action": "buy", "position_ratio": 0.08, "reason": "布林下轨+RSI历史低位", "connector": "OR"}],
    'v4': [{"condition": "boll_position < 0.08 and rsi_6 < 28 and volume_ratio > 1.5", "action": "buy", "position_ratio": 0.08, "reason": "布林下轨+RSI超卖+放量", "connector": "OR"}],
    'v5': [{"condition": "boll_position < 0.12 and rsi_6 < 25 and close > ma_250 * 0.65", "action": "buy", "position_ratio": 0.08, "reason": "布林下轨+RSI超卖+年线保护", "connector": "OR"}],
}

p5_winner, p5_rules = test_versions("布林超跌价格保护买入", pair5_stocks, pair5_max_pos, pair5_versions)

###############################################################################
# PAIR 6: 强势动量右侧买入
###############################################################################
pair6_stocks = ['300750', '002594', '002371', '300274']
pair6_max_pos = 0.35

pair6_versions = {
    'v1': [{"condition": "rel_strength_20 > 5 and volume_ratio > 1.5 and close > ma_20", "action": "buy", "position_ratio": 0.10, "reason": "相对强势+放量+均线上方", "connector": "OR"}],
    'v2': [{"condition": "rel_strength_10 > 8 and macd_histogram > 0 and close > ma_60", "action": "buy", "position_ratio": 0.10, "reason": "10日强势+MACD金叉+60均线上", "connector": "OR"}],
    'v3': [{"condition": "rel_strength_5 > 6 and rel_strength_20 > 3 and volume_ratio > 1.8", "action": "buy", "position_ratio": 0.10, "reason": "短中期双强势+大放量", "connector": "OR"}],
    'v4': [{"condition": "rel_strength_20 > 10 and rsi6_pct100 > 55 and rsi6_pct100 < 80 and close > ma_20", "action": "buy", "position_ratio": 0.10, "reason": "强势动量+RSI适中+均线上", "connector": "OR"}],
    'v5': [{"condition": "rel_strength_10 > 5 and ma_5 > ma_20 and ma_20 > ma_60 and volume_ratio > 1.4", "action": "buy", "position_ratio": 0.10, "reason": "10日强势+均线多头+放量", "connector": "OR"}],
}

p6_winner, p6_rules = test_versions("强势动量右侧买入", pair6_stocks, pair6_max_pos, pair6_versions)

###############################################################################
# PAIR 7: 短线RSI超卖反弹买入
###############################################################################
pair7_stocks = ['600519', '600309', '002920']
pair7_max_pos = 0.20

pair7_versions = {
    'v1': [{"condition": "rsi_6 < 20", "action": "buy", "position_ratio": 0.06, "reason": "日线RSI超卖<20", "connector": "OR"}],
    'v2': [{"condition": "rsi_6 < 25 and boll_position < 0.15", "action": "buy", "position_ratio": 0.07, "reason": "RSI超卖+布林下轨", "connector": "OR"}],
    'v3': [{"condition": "rsi6_pct100 < 8", "action": "buy", "position_ratio": 0.06, "reason": "RSI历史极低位", "connector": "OR"}],
    'v4': [{"condition": "rsi_6 < 22 and volume_ratio > 1.3", "action": "buy", "position_ratio": 0.06, "reason": "RSI超卖+放量", "connector": "OR"}],
    'v5': [
        {"condition": "rsi_6 < 18", "action": "buy", "position_ratio": 0.07, "reason": "RSI极度超卖", "connector": "OR"},
        {"condition": "rsi6_pct100 < 5 and close > ma_250 * 0.60", "action": "buy", "position_ratio": 0.06, "reason": "RSI历史极值+年线保护", "connector": "OR"}
    ],
}

p7_winner, p7_rules = test_versions("短线RSI超卖反弹买入", pair7_stocks, pair7_max_pos, pair7_versions)

###############################################################################
# PAIR 8: 防御慢牛回调买入
###############################################################################
pair8_stocks = ['600900', '600009']
pair8_max_pos = 0.30

pair8_versions = {
    'v1': [{"condition": "w_rsi6_pct100 < 25 and m_rsi6_pct100 > 38", "action": "buy", "position_ratio": 0.10, "reason": "周线超卖+月线趋势向上", "connector": "OR"}],
    'v2': [{"condition": "close < w_ma_20 and m_rsi6_pct100 > 35 and m_rsi6_pct100 < 75", "action": "buy", "position_ratio": 0.10, "reason": "跌破周均线+月线中性偏强", "connector": "OR"}],
    'v3': [{"condition": "rsi6_pct100 < 18 and m_rsi6_pct100 > 40 and close > ma_250 * 0.85", "action": "buy", "position_ratio": 0.10, "reason": "日线超卖+月线强+年线保护", "connector": "OR"}],
    'v4': [{"condition": "w_rsi6_pct100 < 20 and close > ma_250 * 0.88 and idx_ret_60 > -10", "action": "buy", "position_ratio": 0.10, "reason": "周线超卖+年线保护+市场不极端", "connector": "OR"}],
    'v5': [{"condition": "boll_position < 0.15 and m_rsi6_pct100 > 40 and m_rsi6_pct100 < 72", "action": "buy", "position_ratio": 0.10, "reason": "布林下轨+月线中性", "connector": "OR"}],
}

p8_winner, p8_rules = test_versions("防御慢牛回调买入", pair8_stocks, pair8_max_pos, pair8_versions)

###############################################################################
# PAIR 9: 价值动量共振买入
###############################################################################
pair9_stocks = ['600519', '000858', '600309']
pair9_max_pos = 0.45

pair9_versions = {
    'v1': [{"condition": "pettm_pct10y < 30 and macd_histogram > 0 and close > ma_20", "action": "buy", "position_ratio": 0.12, "reason": "PE低+MACD金叉+均线上方", "connector": "OR"}],
    'v2': [{"condition": "pettm_pct10y < 25 and w_rsi6_pct100 < 35 and rsi6_pct100 > 20", "action": "buy", "position_ratio": 0.12, "reason": "PE低+周线超卖+日线回暖", "connector": "OR"}],
    'v3': [{"condition": "pettm_pct10y < 35 and rel_strength_10 > 0 and rsi6_pct100 < 55", "action": "buy", "position_ratio": 0.12, "reason": "PE低+相对强势+RSI适中", "connector": "OR"}],
    'v4': [{"condition": "pettm_pct10y < 20 and close > ma_20", "action": "buy", "position_ratio": 0.12, "reason": "PE极低+均线上方", "connector": "OR"}],
    'v5': [{"condition": "pettm_pct10y < 30 and ma_5 > ma_20 and volume_ratio > 1.2", "action": "buy", "position_ratio": 0.12, "reason": "PE低+均线多头+放量", "connector": "OR"}],
}

p9_winner, p9_rules = test_versions("价值动量共振买入", pair9_stocks, pair9_max_pos, pair9_versions)

###############################################################################
# PRINT FINAL SUMMARY
###############################################################################
print("\n" + "="*70)
print("FINAL WINNERS SUMMARY")
print("="*70)
winners = {
    "4421极限底部买入": (p1_winner, p1_rules, pair1_max_pos),
    "低PE价值底部买入": (p2_winner, p2_rules, pair2_max_pos),
    "MACD金叉趋势买入": (p3_winner, p3_rules, pair3_max_pos),
    "周线超卖均值回归买入": (p4_winner, p4_rules, pair4_max_pos),
    "布林超跌价格保护买入": (p5_winner, p5_rules, pair5_max_pos),
    "强势动量右侧买入": (p6_winner, p6_rules, pair6_max_pos),
    "短线RSI超卖反弹买入": (p7_winner, p7_rules, pair7_max_pos),
    "防御慢牛回调买入": (p8_winner, p8_rules, pair8_max_pos),
    "价值动量共振买入": (p9_winner, p9_rules, pair9_max_pos),
}
for name, (version, rules, max_pos) in winners.items():
    print(f"  {name}: {version}")

###############################################################################
# BUILD strategies_new.json
###############################################################################
strategies = {}

# --- 4421极限底部买入 (best version) ---
strategies["4421极限底部买入"] = {
    "name": "4421极限底部买入",
    "description": "4421极限底部买入：月线+周线RSI双极低，捕捉历史极端超卖底部",
    "max_position_ratio": 0.50,
    "market_regime": [],
    "rules": p1_rules,
    "exclusion_rules": []
}

# --- 4421极限顶部卖出 ---
strategies["4421极限顶部卖出"] = {
    "name": "4421极限顶部卖出",
    "description": "4421极限顶部卖出：月线+周线RSI双极高或PE泡沫，分批减仓清仓",
    "max_position_ratio": 0.50,
    "market_regime": [],
    "rules": [
        {"condition": "m_rsi6_pct100 > 88 and w_rsi6_pct100 > 82", "action": "sell", "position_ratio": 0.35, "reason": "月线周线RSI双高，泡沫确认，减仓", "connector": "OR"},
        {"condition": "m_rsi6_pct100 > 92 and pettm_pct10y > 80", "action": "sell", "position_ratio": 0.40, "reason": "月线极高+PE泡沫，大幅减仓", "connector": "OR"},
        {"condition": "m_rsi6_pct100 > 95", "action": "sell", "position_ratio": 0.50, "reason": "月线RSI历史极值，清仓", "connector": "OR"}
    ],
    "exclusion_rules": []
}

# --- 低PE价值底部买入 ---
strategies["低PE价值底部买入"] = {
    "name": "低PE价值底部买入",
    "description": "低PE价值底部买入：PE历史低位时买入，捕捉价值回归机会",
    "max_position_ratio": 0.45,
    "market_regime": [],
    "rules": p2_rules,
    "exclusion_rules": []
}

# --- 高PE泡沫卖出 ---
strategies["高PE泡沫卖出"] = {
    "name": "高PE泡沫卖出",
    "description": "高PE泡沫卖出：PE历史高位泡沫信号，结合月线RSI分批卖出",
    "max_position_ratio": 0.45,
    "market_regime": [],
    "rules": [
        {"condition": "pettm_pct10y > 80 and m_rsi6_pct100 > 75", "action": "sell", "position_ratio": 0.35, "reason": "PE历史高位+月线高位，泡沫信号，减仓", "connector": "OR"},
        {"condition": "pettm_pct10y > 88", "action": "sell", "position_ratio": 0.45, "reason": "PE历史极高，大幅减仓", "connector": "OR"},
        {"condition": "m_rsi6_pct100 > 92", "action": "sell", "position_ratio": 0.40, "reason": "月线RSI历史极值，清仓", "connector": "OR"}
    ],
    "exclusion_rules": []
}

# --- MACD金叉趋势买入 ---
strategies["MACD金叉趋势买入"] = {
    "name": "MACD金叉趋势买入",
    "description": "MACD金叉趋势买入：MACD金叉+均线多头排列，顺势右侧入场",
    "max_position_ratio": 0.35,
    "market_regime": [],
    "rules": p3_rules,
    "exclusion_rules": []
}

# --- MACD死叉趋势卖出 ---
strategies["MACD死叉趋势卖出"] = {
    "name": "MACD死叉趋势卖出",
    "description": "MACD死叉趋势卖出：MACD死叉+均线空头排列，趋势结束止损出场",
    "max_position_ratio": 0.35,
    "market_regime": [],
    "rules": [
        {"condition": "macd_histogram < 0 and close < ma_20", "action": "sell", "position_ratio": 0.50, "reason": "MACD死叉+跌破20日均线，趋势结束，减仓", "connector": "OR"},
        {"condition": "macd_histogram < 0 and ma_5 < ma_20", "action": "sell", "position_ratio": 0.40, "reason": "MACD死叉+短期均线走弱，清仓", "connector": "OR"},
        {"condition": "close < ma_60", "action": "sell", "position_ratio": 0.30, "reason": "跌破60日均线，趋势破坏，止损", "connector": "OR"}
    ],
    "exclusion_rules": []
}

# --- 周线超卖均值回归买入 ---
strategies["周线超卖均值回归买入"] = {
    "name": "周线超卖均值回归买入",
    "description": "周线超卖均值回归买入：周线RSI历史低位，等待均值回归反弹",
    "max_position_ratio": 0.40,
    "market_regime": [],
    "rules": p4_rules,
    "exclusion_rules": []
}

# --- 周线超买均值回归卖出 ---
strategies["周线超买均值回归卖出"] = {
    "name": "周线超买均值回归卖出",
    "description": "周线超买均值回归卖出：周线RSI历史高位，均值回归完成，分批止盈",
    "max_position_ratio": 0.40,
    "market_regime": [],
    "rules": [
        {"condition": "w_rsi6_pct100 > 80", "action": "sell", "position_ratio": 0.50, "reason": "周线RSI历史高位(80%以上)，均值回归完成，减仓", "connector": "OR"},
        {"condition": "w_rsi6_pct100 > 88 and rsi6_pct100 > 75", "action": "sell", "position_ratio": 0.60, "reason": "周线+日线双高，超强超买，大幅减仓", "connector": "OR"},
        {"condition": "w_rsi6_pct100 > 92", "action": "sell", "position_ratio": 0.70, "reason": "周线极值，清仓", "connector": "OR"}
    ],
    "exclusion_rules": []
}

# --- 布林超跌价格保护买入 ---
strategies["布林超跌价格保护买入"] = {
    "name": "布林超跌价格保护买入",
    "description": "布林超跌价格保护买入：布林带下轨极超跌，短线反弹机会",
    "max_position_ratio": 0.25,
    "market_regime": [],
    "rules": p5_rules,
    "exclusion_rules": []
}

# --- 布林超涨止盈卖出 ---
strategies["布林超涨止盈卖出"] = {
    "name": "布林超涨止盈卖出",
    "description": "布林超涨止盈卖出：布林带上轨超涨，RSI超买，短线止盈",
    "max_position_ratio": 0.25,
    "market_regime": [],
    "rules": [
        {"condition": "boll_position > 0.88 and rsi_6 > 70", "action": "sell", "position_ratio": 0.60, "reason": "布林上轨+RSI超买，短线目标到达，止盈", "connector": "OR"},
        {"condition": "boll_position > 0.92", "action": "sell", "position_ratio": 0.70, "reason": "布林极上轨，强烈止盈", "connector": "OR"},
        {"condition": "rsi_6 > 80", "action": "sell", "position_ratio": 0.50, "reason": "RSI极度超买，止盈", "connector": "OR"}
    ],
    "exclusion_rules": []
}

# --- 强势动量右侧买入 ---
strategies["强势动量右侧买入"] = {
    "name": "强势动量右侧买入",
    "description": "强势动量右侧买入：相对强势+放量突破，右侧跟随强势股",
    "max_position_ratio": 0.35,
    "market_regime": [],
    "rules": p6_rules,
    "exclusion_rules": []
}

# --- 强势动量趋势卖出 ---
strategies["强势动量趋势卖出"] = {
    "name": "强势动量趋势卖出",
    "description": "强势动量趋势卖出：动量逆转+MACD死叉，强势结束减仓止损",
    "max_position_ratio": 0.35,
    "market_regime": [],
    "rules": [
        {"condition": "rel_strength_20 < -3 and macd_histogram < 0", "action": "sell", "position_ratio": 0.50, "reason": "动量逆转+MACD死叉，强势结束，减仓", "connector": "OR"},
        {"condition": "close < ma_20 and volume_ratio > 1.5", "action": "sell", "position_ratio": 0.40, "reason": "跌破20日均线+放量，止损", "connector": "OR"},
        {"condition": "rel_strength_20 < -8", "action": "sell", "position_ratio": 0.60, "reason": "相对强势大幅走弱，清仓", "connector": "OR"}
    ],
    "exclusion_rules": []
}

# --- 短线RSI超卖反弹买入 ---
strategies["短线RSI超卖反弹买入"] = {
    "name": "短线RSI超卖反弹买入",
    "description": "短线RSI超卖反弹买入：日线RSI极低超卖，短线反弹交易",
    "max_position_ratio": 0.20,
    "market_regime": [],
    "rules": p7_rules,
    "exclusion_rules": []
}

# --- 短线RSI超买止盈卖出 ---
strategies["短线RSI超买止盈卖出"] = {
    "name": "短线RSI超买止盈卖出",
    "description": "短线RSI超买止盈卖出：RSI极度超买+布林上轨，短线快速止盈",
    "max_position_ratio": 0.20,
    "market_regime": [],
    "rules": [
        {"condition": "rsi_6 > 70 and rsi6_pct100 > 72", "action": "sell", "position_ratio": 0.60, "reason": "RSI超买+历史高位，短线止盈", "connector": "OR"},
        {"condition": "rsi_6 > 80", "action": "sell", "position_ratio": 0.70, "reason": "RSI极度超买，强制止盈", "connector": "OR"},
        {"condition": "boll_position > 0.85 and rsi_6 > 65", "action": "sell", "position_ratio": 0.50, "reason": "布林上轨+RSI高位，止盈", "connector": "OR"}
    ],
    "exclusion_rules": []
}

# --- 防御慢牛回调买入 ---
strategies["防御慢牛回调买入"] = {
    "name": "防御慢牛回调买入",
    "description": "防御慢牛回调买入：慢牛趋势中的回调买入，月线中性偏强时介入",
    "max_position_ratio": 0.30,
    "market_regime": [],
    "rules": p8_rules,
    "exclusion_rules": []
}

# --- 防御慢牛顶部卖出 ---
strategies["防御慢牛顶部卖出"] = {
    "name": "防御慢牛顶部卖出",
    "description": "防御慢牛顶部卖出：月线周线双高信号，防御股顶部分批卖出",
    "max_position_ratio": 0.30,
    "market_regime": [],
    "rules": [
        {"condition": "m_rsi6_pct100 > 85 and w_rsi6_pct100 > 80", "action": "sell", "position_ratio": 0.50, "reason": "月线周线双高，防御股顶部信号，减仓", "connector": "OR"},
        {"condition": "close < ma_250 * 0.88", "action": "sell", "position_ratio": 0.40, "reason": "跌破年线12%，趋势破坏，止损", "connector": "OR"},
        {"condition": "m_rsi6_pct100 > 90", "action": "sell", "position_ratio": 0.60, "reason": "月线极值，清仓", "connector": "OR"}
    ],
    "exclusion_rules": []
}

# --- 价值动量共振买入 ---
strategies["价值动量共振买入"] = {
    "name": "价值动量共振买入",
    "description": "价值动量共振买入：PE低估+动量信号共振，价值与趋势双重确认",
    "max_position_ratio": 0.45,
    "market_regime": [],
    "rules": p9_rules,
    "exclusion_rules": []
}

# --- 价值动量共振卖出 ---
strategies["价值动量共振卖出"] = {
    "name": "价值动量共振卖出",
    "description": "价值动量共振卖出：动量死叉+PE回归高位，价值泡沫止盈",
    "max_position_ratio": 0.45,
    "market_regime": [],
    "rules": [
        {"condition": "macd_histogram < 0 and pettm_pct10y > 70", "action": "sell", "position_ratio": 0.45, "reason": "动量死叉+PE已回归，共振退出", "connector": "OR"},
        {"condition": "pettm_pct10y > 85 and m_rsi6_pct100 > 78", "action": "sell", "position_ratio": 0.50, "reason": "PE泡沫+月线高位，价值泡沫，减仓", "connector": "OR"},
        {"condition": "close < ma_60 and macd_histogram < 0", "action": "sell", "position_ratio": 0.35, "reason": "跌破60日均线+MACD死叉，止损", "connector": "OR"}
    ],
    "exclusion_rules": []
}

# Save JSON
output_path = r'D:\Git_project\vibecoding_quant\data\strategies_new.json'
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(strategies, f, ensure_ascii=False, indent=2)

print(f"\n{'='*70}")
print(f"strategies_new.json written to: {output_path}")
print(f"Total strategies: {len(strategies)}")
print("Keys:", list(strategies.keys()))
