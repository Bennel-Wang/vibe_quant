"""
专项策略优化 - Round 3 (最终验证)
将 R1/R2 各自最佳策略在完整 2019-2024 验证
同时微调出 v3 改进版本

最终候选:
  A4_相对强势 (R2: tr=0.27, vr=1.24)
  A5_周线反转 (R2: tr=0.29, vr=1.09)
  B1_熊市超跌 (R1: tr=0.07, vr=0.78) — 需重建
  C2_震荡RSI  (R1: tr=0.40, vr=0.61) — 需重建
  C5_均值回归 (R2: tr=0.15, vr=0.71)
  + 各自 v3 改进版
"""
import sys, warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, '.')
from quant_system.backtest import backtest_engine
from quant_system.strategy import QuantStrategy, merge_buy_sell_strategies

STOCKS_A = ['002594', '300750', '002371']
STOCKS_B = ['600900', '601318']
STOCKS_C = ['600309', '601318', '600900']
FULL_PERIOD = ('20190101', '20241231')
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

def run_batch(strategy, stocks, period):
    results = []
    for code in stocks:
        try:
            r = backtest_engine.run_backtest(code, strategy, period[0], period[1], CAPITAL)
            wr = r.win_rate if r.win_rate <= 100 else r.win_rate / 100
            results.append({'code': code, 'ret': r.total_return_pct, 'ann': r.annual_return,
                'dd': r.max_drawdown_pct, 'trades': r.total_trades, 'wr': wr, 'sharpe': r.sharpe_ratio})
        except Exception as e:
            results.append({'code': code, 'ret': 0, 'ann': 0, 'dd': 0, 'trades': 0, 'wr': 0, 'sharpe': 0})
    return results

def show(label, results):
    valid = [r for r in results if r.get('trades', 0) > 0]
    print(f'\n  {label}')
    print(f'  {"Code":<8} {"Ret%":>7} {"Ann%":>7} {"DD%":>7} {"Trd":>5} {"WR%":>6} {"Shp":>6}')
    print(f'  {"-"*48}')
    for r in results:
        flag = ' [0T]' if r.get('trades', 0) == 0 else ''
        print(f'  {r["code"]:<8} {r["ret"]:>7.1f} {r["ann"]:>7.1f} {r["dd"]:>7.1f} {r["trades"]:>5} {r["wr"]:>6.1f} {r["sharpe"]:>6.2f}{flag}')
    if valid:
        n = len(valid)
        avg = lambda k: sum(r[k] for r in valid) / n
        sm = {'ret': avg('ret'), 'ann': avg('ann'), 'dd': avg('dd'), 'tr': avg('trades'), 'wr': avg('wr'), 'sh': avg('sharpe')}
        print(f'  {"AVG":<8} {sm["ret"]:>7.1f} {sm["ann"]:>7.1f} {sm["dd"]:>7.1f} {sm["tr"]:>5.0f} {sm["wr"]:>6.1f} {sm["sh"]:>6.2f}')
        return sm
    return None


# ──────────────────────────────────────────────────────────────
# A4 原版 (相对强势)
# ──────────────────────────────────────────────────────────────
A4 = {
    'name': 'A4_相对强势',
    'description': '【成长股·全天候】不依赖大盘方向，个股相对强势+MACD确认。适用BYD/宁德/北方华创类。',
    'max_pos': 0.40,
    'buy_rules': [
        {'cond': 'rel_strength_10 > 2 and macd_histogram > 0 and rsi_6 > 32 and rsi_6 < 62 and close > ma_20 and volume_ratio > 1.3',
         'pos': 0.18, 'reason': '10日跑赢大盘2%+MACD正+RSI适中+站20线+放量'},
        {'cond': 'w_rsi6_pct100 < 30 and rsi_6 > 25 and rsi_6 < 50 and close > ma_60 * 0.95 and volume_ratio > 1.2',
         'pos': 0.12, 'reason': '周线极超跌+日线回升+未深度破长线'},
        {'cond': 'rel_strength_20 > 5 and close > ma_20 and macd_histogram > 0 and rsi_6 < 65',
         'pos': 0.05, 'reason': '20日跑赢大盘5%+趋势延续'},
    ],
    'buy_exclusions': [
        {'cond': 'rsi_6 > 72', 'reason': 'RSI过高'},
        {'cond': 'overall_score < -55', 'reason': '大盘极端恐慌'},
        {'cond': 'rel_strength_10 < -5', 'reason': '跑输大盘5%'},
    ],
    'sell_rules': [
        {'cond': 'w_rsi6_pct100 > 78 and rsi_6 > 68', 'pos': 0.5, 'reason': '周线高位+日线高位止盈'},
        {'cond': 'close < ma_20 * 0.96', 'pos': 1.0, 'reason': '破20线4%止损'},
        {'cond': 'macd < macd_signal and close < ma_20', 'pos': 0.6, 'reason': 'MACD死叉+破20线'},
        {'cond': 'rel_strength_5 < -8', 'pos': 0.5, 'reason': '5日急速跑输大盘8%减仓'},
        {'cond': 'rsi_6 > 82 and pct_chg < -0.5', 'pos': 0.5, 'reason': '超买转跌'},
    ],
}

# A4v3: 微调仓位+止盈阈值
A4v3 = {
    'name': 'A4v3_强势动量',
    'description': '【成长股·全天候】A4改进：加大主力仓位(22%)，提高止盈阈值让利润跑(RSI>80)。',
    'max_pos': 0.45,
    'buy_rules': [
        {'cond': 'rel_strength_10 > 2 and macd_histogram > 0 and rsi_6 > 32 and rsi_6 < 62 and close > ma_20 and volume_ratio > 1.3',
         'pos': 0.22, 'reason': '相对强势+MACD金叉+RSI适中+站20线+放量'},
        {'cond': 'w_rsi6_pct100 < 30 and rsi_6 > 25 and rsi_6 < 50 and close > ma_60 * 0.95 and volume_ratio > 1.2',
         'pos': 0.12, 'reason': '周线极超跌+回升+未深度破长线'},
        {'cond': 'rel_strength_20 > 5 and close > ma_20 and macd_histogram > 0 and rsi_6 < 65',
         'pos': 0.06, 'reason': '20日跑赢5%+趋势延续加仓'},
    ],
    'buy_exclusions': [
        {'cond': 'rsi_6 > 72', 'reason': 'RSI过高'},
        {'cond': 'overall_score < -55', 'reason': '极端恐慌'},
        {'cond': 'rel_strength_10 < -5', 'reason': '跑输大盘'},
    ],
    'sell_rules': [
        {'cond': 'w_rsi6_pct100 > 80 and rsi_6 > 70', 'pos': 0.5, 'reason': '延迟止盈(RSI>80)'},
        {'cond': 'w_rsi6_pct100 > 88', 'pos': 0.6, 'reason': '周线极高位大幅减仓'},
        {'cond': 'close < ma_20 * 0.96', 'pos': 1.0, 'reason': '破20线4%止损'},
        {'cond': 'macd < macd_signal and close < ma_20', 'pos': 0.6, 'reason': 'MACD死叉+破20线'},
        {'cond': 'rel_strength_5 < -8', 'pos': 0.5, 'reason': '急速跑输大盘减仓'},
        {'cond': 'rsi_6 > 85 and pct_chg < -0.5', 'pos': 0.5, 'reason': '极超买转跌'},
    ],
}

# ──────────────────────────────────────────────────────────────
# A5 原版 (周线反转)
# ──────────────────────────────────────────────────────────────
A5 = {
    'name': 'A5_周线反转',
    'description': '【成长股·抄底】周线月线双低位时抄底成长股。适合大幅回调后买入。',
    'max_pos': 0.35,
    'buy_rules': [
        {'cond': 'w_rsi6_pct100 < 20 and rsi_6 < 40 and rsi_6 > 20 and volume_ratio > 1.5 and close > ma_60 * 0.90',
         'pos': 0.18, 'reason': '周线极低+日线低区回升+放量+未深度破长线'},
        {'cond': 'w_rsi6_pct100 < 30 and m_rsi6_pct100 < 35 and rsi_6 > 28 and close > ma_60 * 0.92',
         'pos': 0.10, 'reason': '周线月线双低+日线回升+长线尚可'},
    ],
    'buy_exclusions': [
        {'cond': 'rsi_6 > 55', 'reason': '不在低位'},
        {'cond': 'close < ma_60 * 0.85', 'reason': '深度破位不抄'},
    ],
    'sell_rules': [
        {'cond': 'w_rsi6_pct100 > 65 and rsi_6 > 60', 'pos': 0.5, 'reason': '周线中高位止盈'},
        {'cond': 'w_rsi6_pct100 > 80', 'pos': 0.6, 'reason': '周线高位大幅止盈'},
        {'cond': 'close < ma_60 * 0.88', 'pos': 1.0, 'reason': '破60线12%止损'},
        {'cond': 'rsi_6 > 75 and pct_chg < -1', 'pos': 0.5, 'reason': '高位转跌'},
    ],
}


# ──────────────────────────────────────────────────────────────
# B1 原版 (R1最佳防御策略)
# ──────────────────────────────────────────────────────────────
B1 = {
    'name': 'B1_熊市超跌',
    'description': '【防御股·熊市】大盘下跌时防御股超跌反弹。极端恐慌时买入低波股。',
    'max_pos': 0.30,
    'buy_rules': [
        {'cond': 'idx_ret_20 < -3 and rsi_6 < 25 and boll_position < 0.15 and volume_ratio > 1.5',
         'pos': 0.15, 'reason': '大盘跌+RSI极超卖+布林极下轨+放量'},
        {'cond': 'idx_ret_10 < -2 and w_rsi6_pct100 < 15 and rsi_6 < 32 and rsi_6 > 18',
         'pos': 0.10, 'reason': '大盘10日跌+周线极低分位+日线回升'},
    ],
    'buy_exclusions': [
        {'cond': 'idx_ret_20 > 2', 'reason': '非熊市环境'},
        {'cond': 'close < ma_60 * 0.85', 'reason': '跌太深'},
    ],
    'sell_rules': [
        {'cond': 'rsi_6 > 58 and boll_position > 0.5', 'pos': 0.5, 'reason': '反弹到中轨止盈'},
        {'cond': 'w_rsi6_pct100 > 60', 'pos': 0.6, 'reason': '周线回到中位'},
        {'cond': 'close < ma_60 * 0.92', 'pos': 1.0, 'reason': '破60线8%止损'},
        {'cond': 'rsi_6 > 70', 'pos': 0.5, 'reason': '反弹到RSI70'},
    ],
}

# B1v3: 放宽买入 + 更快止盈
B1v3 = {
    'name': 'B1v3_防御反弹',
    'description': '【防御股·熊市】B1改进：放宽买入(RSI<30)，加入不依赖大盘的极端超卖规则。快进快出。',
    'max_pos': 0.30,
    'buy_rules': [
        {'cond': 'idx_ret_20 < -2 and rsi_6 < 30 and boll_position < 0.20 and volume_ratio > 1.3',
         'pos': 0.15, 'reason': '大盘跌+RSI超卖+布林下轨+放量'},
        {'cond': 'idx_ret_10 < -1.5 and w_rsi6_pct100 < 20 and rsi_6 < 35 and rsi_6 > 15',
         'pos': 0.10, 'reason': '大盘10日跌+周线极低+日线回升'},
        {'cond': 'rsi_6 < 20 and boll_position < 0.08 and volume_ratio > 1.8',
         'pos': 0.05, 'reason': '极端超卖(不管大盘)'},
    ],
    'buy_exclusions': [
        {'cond': 'close < ma_60 * 0.82', 'reason': '跌太深'},
        {'cond': 'rsi_6 > 50', 'reason': '不在超卖区'},
    ],
    'sell_rules': [
        {'cond': 'rsi_6 > 52 and boll_position > 0.45', 'pos': 0.5, 'reason': '快速止盈-回中轨即卖'},
        {'cond': 'w_rsi6_pct100 > 50', 'pos': 0.5, 'reason': '周线回中位'},
        {'cond': 'rsi_6 > 65', 'pos': 0.5, 'reason': '反弹到RSI65'},
        {'cond': 'close < ma_60 * 0.90', 'pos': 1.0, 'reason': '止损'},
    ],
}


# ──────────────────────────────────────────────────────────────
# C2 原版 (R1最稳 震荡RSI)
# ──────────────────────────────────────────────────────────────
C2 = {
    'name': 'C2_震荡RSI',
    'description': '【周期/价值股·横盘】震荡市RSI超买超卖做波段。RSI<28买，RSI>65卖。',
    'max_pos': 0.25,
    'buy_rules': [
        {'cond': 'abs(idx_ret_20) < 5 and rsi_6 < 28 and boll_position < 0.30 and volume_ratio > 1.0',
         'pos': 0.12, 'reason': '横盘+RSI极超卖+布林下轨区'},
        {'cond': 'abs(idx_ret_20) < 5 and rsi_6 < 35 and rsi_6 > 20 and w_rsi6_pct100 < 25 and close > ma_60 * 0.95',
         'pos': 0.08, 'reason': '横盘+RSI低区+周线历史低+未破长线'},
    ],
    'buy_exclusions': [
        {'cond': 'rsi_6 > 55', 'reason': 'RSI不够低'},
        {'cond': 'close < ma_60 * 0.90', 'reason': '破长线不做'},
    ],
    'sell_rules': [
        {'cond': 'rsi_6 > 65 and boll_position > 0.60', 'pos': 0.6, 'reason': 'RSI高区+布林中上轨'},
        {'cond': 'rsi_6 > 72', 'pos': 0.5, 'reason': 'RSI超买'},
        {'cond': 'w_rsi6_pct100 > 68', 'pos': 0.4, 'reason': '周线RSI中高位'},
        {'cond': 'close < ma_20 * 0.96', 'pos': 1.0, 'reason': '止损'},
    ],
}

# C2v3: 微调—放宽横盘范围+加入KDJ确认
C2v3 = {
    'name': 'C2v3_震荡波段',
    'description': '【周期/价值股·横盘】C2改进：放宽横盘判定(idx_ret_20<6)，加KDJ辅助。',
    'max_pos': 0.28,
    'buy_rules': [
        {'cond': 'abs(idx_ret_20) < 6 and rsi_6 < 30 and kdj_j < 25 and boll_position < 0.28 and volume_ratio > 1.0',
         'pos': 0.14, 'reason': '横盘+RSI+KDJ双超卖+布林下轨'},
        {'cond': 'abs(idx_ret_20) < 6 and rsi_6 < 35 and w_rsi6_pct100 < 25 and close > ma_60 * 0.95',
         'pos': 0.08, 'reason': '横盘+RSI低+周线历史低+长线ok'},
    ],
    'buy_exclusions': [
        {'cond': 'rsi_6 > 55', 'reason': 'RSI不够低'},
        {'cond': 'close < ma_60 * 0.90', 'reason': '破长线'},
    ],
    'sell_rules': [
        {'cond': 'rsi_6 > 62 and boll_position > 0.58', 'pos': 0.6, 'reason': 'RSI中高+布林中上轨'},
        {'cond': 'rsi_6 > 70', 'pos': 0.5, 'reason': 'RSI高位'},
        {'cond': 'kdj_j > 80', 'pos': 0.4, 'reason': 'KDJ超买'},
        {'cond': 'close < ma_20 * 0.96', 'pos': 1.0, 'reason': '止损'},
    ],
}


# C5 原版 (均值回归)
C5 = {
    'name': 'C5_均值回归',
    'description': '【周期/价值股·全天候】快进快出均值回归。布林极底买，中轨即卖，追求高胜率。',
    'max_pos': 0.25,
    'buy_rules': [
        {'cond': 'boll_position < 0.15 and rsi_6 < 28 and volume_ratio > 1.1',
         'pos': 0.12, 'reason': '布林极底+RSI极超卖+有量'},
        {'cond': 'boll_position < 0.25 and rsi_6 < 33 and kdj_j < 20',
         'pos': 0.08, 'reason': '布林下方+RSI低+KDJ极低'},
    ],
    'buy_exclusions': [
        {'cond': 'rsi_6 > 45', 'reason': '不在低位'},
        {'cond': 'close < ma_60 * 0.88', 'reason': '破长线'},
    ],
    'sell_rules': [
        {'cond': 'boll_position > 0.45 and rsi_6 > 48', 'pos': 0.7, 'reason': '回到中轨即止盈'},
        {'cond': 'rsi_6 > 58', 'pos': 0.5, 'reason': 'RSI回到中位'},
        {'cond': 'close < ma_60 * 0.90', 'pos': 1.0, 'reason': '止损'},
        {'cond': 'pct_chg < -3.5', 'pos': 0.8, 'reason': '单日暴跌止损'},
    ],
}


# ══════════════════════════════════════════════════════════════
#  执行: 全段 2019-2024
# ══════════════════════════════════════════════════════════════
if __name__ == '__main__':
    all_results = []
    tests = [
        ('A(成长)', [A4, A4v3, A5], STOCKS_A),
        ('B(防御)', [B1, B1v3], STOCKS_B),
        ('C(震荡)', [C2, C2v3, C5], STOCKS_C),
    ]

    for cat, configs, stocks in tests:
        print(f'\n{"#"*72}')
        print(f'  类别 {cat}  股票: {stocks}  期间: 2019-2024')
        print(f'{"#"*72}')

        for cfg in configs:
            strat = make_strategy(cfg)
            print(f'\n{"="*60}')
            print(f'  {cfg["name"]}')
            print(f'{"="*60}')
            res = run_batch(strat, stocks, FULL_PERIOD)
            sm = show('[全段 2019-2024]', res)
            if sm:
                all_results.append({'name': cfg['name'], 'cat': cat, **sm})

    # 排名
    print(f'\n\n{"#"*72}')
    print(f'  ROUND 3 最终排名 (全段 2019-2024)')
    print(f'{"#"*72}')
    print(f'  {"Name":<18} {"Cat":<10} {"Ret%":>7} {"Ann%":>7} {"DD%":>7} {"Trd":>5} {"WR%":>6} {"Shp":>6}')
    print(f'  {"-"*62}')
    ranked = sorted(all_results, key=lambda x: x['sh'], reverse=True)
    for r in ranked:
        print(f'  {r["name"]:<18} {r["cat"]:<10} {r["ret"]:>7.1f} {r["ann"]:>7.1f} {r["dd"]:>7.1f} {r["tr"]:>5.0f} {r["wr"]:>6.1f} {r["sh"]:>6.2f}')

    print(f'\n  每类最佳:')
    for cat in ['A(成长)', 'B(防御)', 'C(震荡)']:
        cat_best = [r for r in ranked if r['cat'] == cat]
        if cat_best:
            b = cat_best[0]
            print(f'    {cat}: {b["name"]}  Sharpe={b["sh"]:.2f}  Ann={b["ann"]:.1f}%  DD={b["dd"]:.1f}%')
