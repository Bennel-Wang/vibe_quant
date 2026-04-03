"""
专项策略优化 - Round 1
三类策略 × 训练集(2019-2022) + 验证集(2023-2024)
防过拟合: 训练/验证分离，验证集 Sharpe 不能严重衰减

策略 A: 牛市动量 — 适用大盘上涨+成长股 (BYD/宁德时代/北方华创)
策略 B: 熊市防御 — 适用大盘下跌+防御股 (长江电力/中国平安)
策略 C: 震荡套利 — 适用大盘横盘+周期价值股 (万华化学/中国平安)
"""
import sys, os, warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, '.')
from quant_system.backtest import backtest_engine
from quant_system.strategy import QuantStrategy, merge_buy_sell_strategies

# 每类策略用适配的股票测试
STOCKS_A = ['002594', '300750', '002371']          # 成长/动量型
STOCKS_B = ['600900', '601318']                     # 防御/价值型
STOCKS_C = ['600309', '601318', '600900']           # 周期/价值型

TRAIN_PERIOD  = ('20190101', '20221231')   # 训练集: 4年
VALID_PERIOD  = ('20230101', '20241231')   # 验证集: 2年
CAPITAL = 500000

# ─── 工具函数 ─────────────────────────────────────────────
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
            print(f'    ERROR {code}: {e}')
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
        print(f'  {"AVG":<8} {avg("ret"):>7.1f} {avg("ann"):>7.1f} {avg("dd"):>7.1f} {avg("trades"):>5.0f} {avg("wr"):>6.1f} {avg("sharpe"):>6.2f}')
        return {'ret': avg('ret'), 'ann': avg('ann'), 'dd': avg('dd'), 'tr': avg('trades'), 'wr': avg('wr'), 'sh': avg('sharpe')}
    return None


# ══════════════════════════════════════════════════════════════
#  策略 A: 牛市动量 — 大盘向上时做成长股
# ══════════════════════════════════════════════════════════════
configs_A = []

# A1: MACD趋势动量 — 经典动量，要求大盘20日涨幅>0
configs_A.append({
    'name': 'A1_牛市动量',
    'description': '大盘上涨环境+成长股MACD动量策略。要求idx_ret_20>0确认牛市环境。',
    'max_pos': 0.45,
    'buy_rules': [
        {'cond': 'idx_ret_20 > 0 and macd_histogram > 0 and macd > macd_signal and rsi_6 > 32 and rsi_6 < 60 and close > ma_20 and volume_ratio > 1.4',
         'pos': 0.22, 'reason': '大盘向上+MACD金叉+RSI适中+站20线+放量'},
        {'cond': 'idx_ret_20 > 0 and close > ma_20 and close > ma_60 and rel_strength_20 > 3 and rsi_6 < 65',
         'pos': 0.08, 'reason': '牛市中趋势延续+相对强势'},
    ],
    'buy_exclusions': [
        {'cond': 'idx_ret_20 < -2', 'reason': '大盘20日下跌超2%，非牛市环境'},
        {'cond': 'rsi_6 > 72', 'reason': 'RSI过高'},
        {'cond': 'rel_strength_10 < -5', 'reason': '跑输大盘'},
    ],
    'sell_rules': [
        {'cond': 'w_rsi6_pct100 > 78 and rsi_6 > 70', 'pos': 0.5, 'reason': '周线高位+日线高位止盈'},
        {'cond': 'close < ma_20 * 0.96', 'pos': 1.0, 'reason': '破20日线4%止损'},
        {'cond': 'macd < macd_signal and close < ma_20', 'pos': 0.7, 'reason': 'MACD死叉+破20线'},
        {'cond': 'idx_ret_10 < -5', 'pos': 0.6, 'reason': '大盘10日跌>5%，系统性风险减仓'},
        {'cond': 'rsi_6 > 82 and pct_chg < -1', 'pos': 0.5, 'reason': '超买转跌'},
    ],
})

# A2: 更激进的牛市追涨 — 突破+放量+强势
configs_A.append({
    'name': 'A2_牛市突破',
    'description': '牛市环境中追涨突破策略。要求股价突破布林上轨+大幅放量。',
    'max_pos': 0.40,
    'buy_rules': [
        {'cond': 'idx_ret_20 > 1 and boll_position > 0.65 and volume_ratio > 1.8 and rsi_6 > 45 and rsi_6 < 72 and close > ma_20 and macd_histogram > 0',
         'pos': 0.20, 'reason': '牛市+布林中上轨+放量突破+MACD正'},
        {'cond': 'idx_ret_20 > 0 and rel_strength_5 > 4 and volume_ratio > 2.0 and rsi_6 < 70 and close > ma_60',
         'pos': 0.10, 'reason': '近5日强势+大量+长线趋势向上'},
    ],
    'buy_exclusions': [
        {'cond': 'idx_ret_20 < 0', 'reason': '大盘20日不涨，非突破环境'},
        {'cond': 'rsi_6 > 75', 'reason': 'RSI偏高'},
        {'cond': 'volume_ratio < 1.3', 'reason': '量能不足，突破不可靠'},
    ],
    'sell_rules': [
        {'cond': 'w_rsi6_pct100 > 80', 'pos': 0.5, 'reason': '周线RSI历史高位'},
        {'cond': 'close < ma_20 * 0.95', 'pos': 1.0, 'reason': '破20日线5%止损'},
        {'cond': 'volume_ratio > 3.5 and pct_chg < -3', 'pos': 0.8, 'reason': '放量暴跌出货'},
        {'cond': 'rsi_6 > 85 and pct_chg < 0', 'pos': 0.5, 'reason': '极超买回落'},
        {'cond': 'idx_ret_5 < -4', 'pos': 0.5, 'reason': '大盘急跌，先撤'},
    ],
})

# A3: 牛市回调买入 — 趋势回撤时接
configs_A.append({
    'name': 'A3_牛市回调',
    'description': '大盘整体向上但短期回调时买入成长股。利用回调获得更好价位。',
    'max_pos': 0.40,
    'buy_rules': [
        {'cond': 'idx_ret_60 > 3 and idx_ret_5 < -1 and rsi_6 < 42 and close > ma_60 and volume_ratio > 1.2 and macd_histogram > -0.1',
         'pos': 0.18, 'reason': '长线牛市+短线回调+RSI低+长线趋势未破'},
        {'cond': 'idx_ret_20 > 1 and w_rsi6_pct100 < 35 and rsi_6 > 25 and close > ma_60 and boll_position < 0.4',
         'pos': 0.12, 'reason': '大盘中线向上+周线超卖+布林下方+长线趋势'},
    ],
    'buy_exclusions': [
        {'cond': 'idx_ret_60 < 0', 'reason': '60日大盘不涨，非长期牛市'},
        {'cond': 'close < ma_60', 'reason': '价格在60日线下方，趋势已破'},
        {'cond': 'rsi_6 > 65', 'reason': '不够低'},
    ],
    'sell_rules': [
        {'cond': 'w_rsi6_pct100 > 75 and rsi_6 > 68', 'pos': 0.5, 'reason': '回到高位止盈'},
        {'cond': 'close < ma_60 * 0.97', 'pos': 1.0, 'reason': '破60日线3%止损'},
        {'cond': 'macd < macd_signal and close < ma_20', 'pos': 0.6, 'reason': 'MACD死叉+破20线'},
        {'cond': 'rsi_6 > 78', 'pos': 0.4, 'reason': 'RSI高位减仓'},
    ],
})


# ══════════════════════════════════════════════════════════════
#  策略 B: 熊市防御 — 大盘下跌时做防御股
# ══════════════════════════════════════════════════════════════
configs_B = []

# B1: 熊市超跌反弹 — 极端超卖后抢反弹
configs_B.append({
    'name': 'B1_熊市超跌',
    'description': '大盘下跌时防御股超跌反弹策略。在极端恐慌时买入低波防御股。',
    'max_pos': 0.30,
    'buy_rules': [
        {'cond': 'idx_ret_20 < -3 and rsi_6 < 25 and boll_position < 0.15 and volume_ratio > 1.5',
         'pos': 0.15, 'reason': '大盘跌+RSI极超卖+布林极下轨+放量=恐慌性超卖'},
        {'cond': 'idx_ret_10 < -2 and w_rsi6_pct100 < 15 and rsi_6 < 32 and rsi_6 > 18',
         'pos': 0.10, 'reason': '大盘10日跌+周线极低分位+日线开始回升'},
    ],
    'buy_exclusions': [
        {'cond': 'idx_ret_20 > 2', 'reason': '大盘20日涨>2%，非熊市环境'},
        {'cond': 'close < ma_60 * 0.85', 'reason': '跌太深，趋势完全破坏'},
    ],
    'sell_rules': [
        {'cond': 'rsi_6 > 58 and boll_position > 0.5', 'pos': 0.5, 'reason': '反弹到布林中轨+RSI回升止盈'},
        {'cond': 'w_rsi6_pct100 > 60', 'pos': 0.6, 'reason': '周线回到中位，反弹完成'},
        {'cond': 'close < ma_60 * 0.92', 'pos': 1.0, 'reason': '破60日线8%止损'},
        {'cond': 'rsi_6 > 70', 'pos': 0.5, 'reason': '反弹到RSI70，不贪'},
    ],
})

# B2: 熊市布林底部 — 纯技术面均值回归
configs_B.append({
    'name': 'B2_熊市布林底',
    'description': '熊市中防御股触碰布林下轨时做均值回归。小仓位、快进快出。',
    'max_pos': 0.25,
    'buy_rules': [
        {'cond': 'boll_position < 0.10 and rsi_6 < 22 and volume_ratio > 1.3 and close > ma_60 * 0.90',
         'pos': 0.12, 'reason': '布林极下轨+RSI极低+放量+未深度破长线'},
        {'cond': 'boll_position < 0.20 and rsi_6 < 30 and w_rsi6_pct100 < 20 and volume_ratio > 1.0',
         'pos': 0.08, 'reason': '布林下轨区+超卖+周线历史低位'},
    ],
    'buy_exclusions': [
        {'cond': 'overall_score < -65', 'reason': '极端恐慌不抄底'},
        {'cond': 'close < ma_60 * 0.85', 'reason': '深度破位不接'},
    ],
    'sell_rules': [
        {'cond': 'boll_position > 0.50', 'pos': 0.6, 'reason': '回到布林中轨止盈'},
        {'cond': 'rsi_6 > 55', 'pos': 0.5, 'reason': 'RSI回到中位'},
        {'cond': 'close < ma_60 * 0.88', 'pos': 1.0, 'reason': '止损'},
        {'cond': 'pct_chg < -4', 'pos': 0.8, 'reason': '单日暴跌止损'},
    ],
})

# B3: 熊市高股息防御 — 利用低PE+高安全边际
configs_B.append({
    'name': 'B3_熊市价值',
    'description': '熊市中买入低估值防御股。PE历史低位+技术面企稳+大盘恐慌。',
    'max_pos': 0.30,
    'buy_rules': [
        {'cond': 'idx_ret_20 < -2 and pettm_pct10y < 30 and rsi_6 < 35 and close > ma_60 * 0.95',
         'pos': 0.15, 'reason': '大盘跌+PE历史低位+RSI低+价格未深度破长线'},
        {'cond': 'pettm_pct10y < 20 and w_rsi6_pct100 < 25 and volume_ratio > 1.0',
         'pos': 0.10, 'reason': 'PE极低分位+周线超跌=深度价值'},
    ],
    'buy_exclusions': [
        {'cond': 'pettm_pct10y > 60', 'reason': 'PE不够低'},
        {'cond': 'idx_ret_20 > 3', 'reason': '大盘向上非熊市'},
    ],
    'sell_rules': [
        {'cond': 'pettm_pct10y > 50 and rsi_6 > 55', 'pos': 0.5, 'reason': 'PE回到中位+RSI回升'},
        {'cond': 'w_rsi6_pct100 > 65', 'pos': 0.5, 'reason': '周线RSI回到中高位'},
        {'cond': 'close < ma_60 * 0.90', 'pos': 1.0, 'reason': '破60日线10%止损'},
        {'cond': 'rsi_6 > 68', 'pos': 0.4, 'reason': 'RSI高位减仓'},
    ],
})


# ══════════════════════════════════════════════════════════════
#  策略 C: 震荡套利 — 大盘横盘时做区间交易
# ══════════════════════════════════════════════════════════════
configs_C = []

# C1: 布林通道区间操作
configs_C.append({
    'name': 'C1_震荡布林',
    'description': '大盘横盘时利用布林带做区间交易。在通道下沿买、上沿卖。',
    'max_pos': 0.30,
    'buy_rules': [
        {'cond': 'abs(idx_ret_20) < 4 and boll_position < 0.20 and rsi_6 < 32 and volume_ratio > 1.2',
         'pos': 0.15, 'reason': '大盘横盘+布林下轨+RSI超卖+放量'},
        {'cond': 'abs(idx_ret_20) < 5 and boll_position < 0.30 and rsi_6 < 38 and w_rsi6_pct100 < 30',
         'pos': 0.08, 'reason': '大盘横盘+布林偏下+日线偏低+周线低位'},
    ],
    'buy_exclusions': [
        {'cond': 'idx_ret_20 < -6', 'reason': '大盘20日跌>6%，非横盘是趋势下跌'},
        {'cond': 'idx_ret_20 > 6', 'reason': '大盘20日涨>6%，非横盘是趋势上涨'},
        {'cond': 'rsi_6 > 60', 'reason': '不在低位'},
    ],
    'sell_rules': [
        {'cond': 'boll_position > 0.75 and rsi_6 > 62', 'pos': 0.6, 'reason': '布林上轨区+RSI回升止盈'},
        {'cond': 'rsi_6 > 72', 'pos': 0.5, 'reason': 'RSI超买区减仓'},
        {'cond': 'close < ma_20 * 0.96', 'pos': 1.0, 'reason': '破20日线4%止损'},
        {'cond': 'w_rsi6_pct100 > 70', 'pos': 0.4, 'reason': '周线高位减仓'},
    ],
})

# C2: RSI超买超卖区间操作
configs_C.append({
    'name': 'C2_震荡RSI',
    'description': '震荡市中利用RSI超买超卖做波段。RSI<30买入，RSI>65卖出。',
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
})

# C3: KDJ+布林组合
configs_C.append({
    'name': 'C3_震荡KDJ',
    'description': '震荡市中KDJ金叉+布林下方买入。KDJ死叉+布林上方卖出。',
    'max_pos': 0.25,
    'buy_rules': [
        {'cond': 'abs(idx_ret_20) < 5 and kdj_j < 20 and boll_position < 0.25 and volume_ratio > 1.1',
         'pos': 0.12, 'reason': '横盘+KDJ极超卖+布林下轨+有量'},
        {'cond': 'abs(idx_ret_20) < 5 and kdj_j < 30 and rsi_6 < 35 and close > ma_60 * 0.95',
         'pos': 0.08, 'reason': '横盘+KDJ超卖+RSI低+未破长线'},
    ],
    'buy_exclusions': [
        {'cond': 'kdj_j > 50', 'reason': 'KDJ不在低位'},
        {'cond': 'close < ma_60 * 0.90', 'reason': '趋势破坏'},
    ],
    'sell_rules': [
        {'cond': 'kdj_j > 80 and boll_position > 0.65', 'pos': 0.6, 'reason': 'KDJ超买+布林中上轨'},
        {'cond': 'kdj_j > 90', 'pos': 0.5, 'reason': 'KDJ极超买'},
        {'cond': 'rsi_6 > 70', 'pos': 0.4, 'reason': 'RSI高位'},
        {'cond': 'close < ma_20 * 0.96', 'pos': 1.0, 'reason': '止损'},
    ],
})


# ══════════════════════════════════════════════════════════════
#  执行回测
# ══════════════════════════════════════════════════════════════
if __name__ == '__main__':
    all_results = []

    for cat, configs, stocks in [
        ('A(牛市动量)', configs_A, STOCKS_A),
        ('B(熊市防御)', configs_B, STOCKS_B),
        ('C(震荡套利)', configs_C, STOCKS_C),
    ]:
        print(f'\n{"#"*72}')
        print(f'  类别 {cat}  测试股票: {stocks}')
        print(f'{"#"*72}')

        for cfg in configs:
            strat = make_strategy(cfg)
            print(f'\n{"="*60}')
            print(f'  {cfg["name"]}: {cfg["description"][:60]}')
            print(f'{"="*60}')

            # 训练集
            tr = run_batch(strat, stocks, TRAIN_PERIOD)
            tr_sm = show(f'[训练集 2019-2022]', tr)

            # 验证集
            vr = run_batch(strat, stocks, VALID_PERIOD)
            vr_sm = show(f'[验证集 2023-2024]', vr)

            # 对比
            if tr_sm and vr_sm:
                decay = vr_sm['sh'] / tr_sm['sh'] if tr_sm['sh'] > 0.01 else float('inf')
                status = 'PASS' if decay > 0.5 else 'OVERFIT'
                print(f'\n  >>> Sharpe: 训练={tr_sm["sh"]:.2f}  验证={vr_sm["sh"]:.2f}  衰减比={decay:.2f}  [{status}]')
                all_results.append({
                    'name': cfg['name'], 'cat': cat,
                    'tr_sh': tr_sm['sh'], 'tr_ann': tr_sm['ann'], 'tr_dd': tr_sm['dd'],
                    'vr_sh': vr_sm['sh'], 'vr_ann': vr_sm['ann'], 'vr_dd': vr_sm['dd'],
                    'decay': decay, 'status': status
                })
            elif tr_sm:
                print(f'\n  >>> 验证集无有效交易')
                all_results.append({
                    'name': cfg['name'], 'cat': cat,
                    'tr_sh': tr_sm['sh'], 'tr_ann': tr_sm['ann'], 'tr_dd': tr_sm['dd'],
                    'vr_sh': 0, 'vr_ann': 0, 'vr_dd': 0, 'decay': 0, 'status': 'NO_VALID'
                })

    # 总结
    print(f'\n\n{"#"*72}')
    print(f'  ROUND 1 总结 — 训练(2019-2022) vs 验证(2023-2024)')
    print(f'{"#"*72}')
    print(f'  {"Name":<16} {"Cat":<12} {"Tr_Sh":>6} {"Vr_Sh":>6} {"Decay":>6} {"Tr_Ann%":>8} {"Vr_Ann%":>8} {"Status":<8}')
    print(f'  {"-"*70}')
    for r in sorted(all_results, key=lambda x: x.get('vr_sh', 0), reverse=True):
        print(f'  {r["name"]:<16} {r["cat"]:<12} {r["tr_sh"]:>6.2f} {r["vr_sh"]:>6.2f} {r["decay"]:>6.2f} {r["tr_ann"]:>8.1f} {r["vr_ann"]:>8.1f} {r["status"]:<8}')
