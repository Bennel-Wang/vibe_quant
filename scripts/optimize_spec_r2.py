"""
专项策略优化 - Round 2
基于 Round 1 发现:
  - A类(牛市)全部失败 → 完全重新设计，降低对"牛市环境"的依赖
  - B1(熊市超跌) 验证集Sharpe=0.78 → 微调优化
  - C2(震荡RSI) 训练0.40 验证0.61 → 最稳策略，微调仓位
  - C3(震荡KDJ) 验证集不错 → 与C2结合

防过拟合：训练集=2021-2024，验证集=2019-2020（完全交换时段）
"""
import sys, os, warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, '.')
from quant_system.backtest import backtest_engine
from quant_system.strategy import QuantStrategy, merge_buy_sell_strategies

STOCKS_A = ['002594', '300750', '002371']
STOCKS_B = ['600900', '601318']
STOCKS_C = ['600309', '601318', '600900']

TRAIN_PERIOD  = ('20210101', '20241231')   # 训练集: 换时段
VALID_PERIOD  = ('20190101', '20201231')   # 验证集: 换时段
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
        print(f'  {"AVG":<8} {avg("ret"):>7.1f} {avg("ann"):>7.1f} {avg("dd"):>7.1f} {avg("trades"):>5.0f} {avg("wr"):>6.1f} {avg("sharpe"):>6.2f}')
        return {'ret': avg('ret'), 'ann': avg('ann'), 'dd': avg('dd'), 'tr': avg('trades'), 'wr': avg('wr'), 'sh': avg('sharpe')}
    return None


# ══════════════════════════════════════════════════════════════
#  策略 A 重新设计: 成长股动量 — 不再要求大盘必须上涨
#  核心变化: 只要个股强于大盘就做，不绑定大盘方向
# ══════════════════════════════════════════════════════════════
configs_A = []

# A4: 相对强势动量 — 股票自身强势才做(不依赖大盘)
configs_A.append({
    'name': 'A4_相对强势',
    'description': '【成长股专用】不依赖大盘方向，只要个股相对大盘强势+MACD确认就做。适用成长型标的(BYD/宁德/北方华创类)。',
    'max_pos': 0.40,
    'buy_rules': [
        # 核心: 相对强势 + 技术确认，不管大盘方向
        {'cond': 'rel_strength_10 > 2 and macd_histogram > 0 and rsi_6 > 32 and rsi_6 < 62 and close > ma_20 and volume_ratio > 1.3',
         'pos': 0.18, 'reason': '10日跑赢大盘2%+MACD正+RSI适中+站20线+放量'},
        # 深度回调后的恢复（不要求大盘方向）
        {'cond': 'w_rsi6_pct100 < 30 and rsi_6 > 25 and rsi_6 < 50 and close > ma_60 * 0.95 and volume_ratio > 1.2',
         'pos': 0.12, 'reason': '周线极超跌+日线开始回升+未深度破长线'},
        # 强势趋势延续
        {'cond': 'rel_strength_20 > 5 and close > ma_20 and macd_histogram > 0 and rsi_6 < 65',
         'pos': 0.05, 'reason': '20日持续跑赢大盘5%+趋势延续'},
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
        {'cond': 'rel_strength_5 < -8', 'pos': 0.5, 'reason': '5日急速跑输大盘8%，减仓'},
        {'cond': 'rsi_6 > 82 and pct_chg < -0.5', 'pos': 0.5, 'reason': '超买区转跌'},
    ],
})

# A5: 周线底部反转 — 周线月线双低时抄底成长股
configs_A.append({
    'name': 'A5_周线反转',
    'description': '【成长股专用】周线月线双低位时抄底。适合在成长股大幅回调后买入。不依赖大盘方向。',
    'max_pos': 0.35,
    'buy_rules': [
        {'cond': 'w_rsi6_pct100 < 20 and rsi_6 < 40 and rsi_6 > 20 and volume_ratio > 1.5 and close > ma_60 * 0.90',
         'pos': 0.18, 'reason': '周线极低+日线低区但回升+放量+未深度破长线'},
        {'cond': 'w_rsi6_pct100 < 30 and m_rsi6_pct100 < 35 and rsi_6 > 28 and close > ma_60 * 0.92',
         'pos': 0.10, 'reason': '周线月线双低+日线回升+长线尚可'},
    ],
    'buy_exclusions': [
        {'cond': 'rsi_6 > 55', 'reason': '不在低位'},
        {'cond': 'close < ma_60 * 0.85', 'reason': '深度破位不抄'},
    ],
    'sell_rules': [
        {'cond': 'w_rsi6_pct100 > 65 and rsi_6 > 60', 'pos': 0.5, 'reason': '周线回到中高位止盈'},
        {'cond': 'w_rsi6_pct100 > 80', 'pos': 0.6, 'reason': '周线高位大幅止盈'},
        {'cond': 'close < ma_60 * 0.88', 'pos': 1.0, 'reason': '破60线12%止损'},
        {'cond': 'rsi_6 > 75 and pct_chg < -1', 'pos': 0.5, 'reason': '高位转跌'},
    ],
})


# ══════════════════════════════════════════════════════════════
#  策略 B 优化: 基于 B1 (验证Sharpe=0.78) 微调
# ══════════════════════════════════════════════════════════════
configs_B = []

# B4: B1改进 — 放宽买入条件 + 优化止盈
configs_B.append({
    'name': 'B4_熊市超跌v2',
    'description': '【防御股专用】大盘跌时防御股(长江电力/平安)超跌反弹。放宽B1买入阈值增加交易次数。',
    'max_pos': 0.30,
    'buy_rules': [
        # 放宽: rsi<25→<30, boll<0.15→<0.20
        {'cond': 'idx_ret_20 < -2 and rsi_6 < 30 and boll_position < 0.20 and volume_ratio > 1.3',
         'pos': 0.15, 'reason': '大盘跌+RSI超卖+布林下轨+放量'},
        {'cond': 'idx_ret_10 < -1.5 and w_rsi6_pct100 < 20 and rsi_6 < 35 and rsi_6 > 15',
         'pos': 0.10, 'reason': '大盘10日跌+周线极低分位+日线回升'},
        # 新增: 不管大盘，只要个股极端超卖
        {'cond': 'rsi_6 < 20 and boll_position < 0.08 and volume_ratio > 1.8',
         'pos': 0.05, 'reason': '极端超卖+布林极底+大量=恐慌底部'},
    ],
    'buy_exclusions': [
        {'cond': 'close < ma_60 * 0.82', 'reason': '跌太深趋势破坏'},
        {'cond': 'rsi_6 > 50', 'reason': '不在超卖区'},
    ],
    'sell_rules': [
        {'cond': 'rsi_6 > 55 and boll_position > 0.50', 'pos': 0.5, 'reason': '反弹到中轨+RSI回升'},
        {'cond': 'w_rsi6_pct100 > 55', 'pos': 0.5, 'reason': '周线回到中位'},
        {'cond': 'rsi_6 > 68', 'pos': 0.5, 'reason': '反弹到RSI68'},
        {'cond': 'close < ma_60 * 0.90', 'pos': 1.0, 'reason': '破60线10%止损'},
    ],
})

# B5: 价值低估+超跌 — PE低位 + 技术超卖
configs_B.append({
    'name': 'B5_低估超跌',
    'description': '【防御股专用】PE低位+技术超卖双确认。不依赖大盘方向，PE历史低位就是安全边际。',
    'max_pos': 0.30,
    'buy_rules': [
        {'cond': 'pettm_pct10y < 25 and rsi_6 < 35 and boll_position < 0.30 and volume_ratio > 1.0',
         'pos': 0.15, 'reason': 'PE历史低位+RSI超卖+布林下方'},
        {'cond': 'pettm_pct10y < 15 and w_rsi6_pct100 < 25',
         'pos': 0.10, 'reason': 'PE极低+周线极低=极度低估'},
    ],
    'buy_exclusions': [
        {'cond': 'pettm_pct10y > 50', 'reason': 'PE不够低'},
    ],
    'sell_rules': [
        {'cond': 'pettm_pct10y > 55 and rsi_6 > 55', 'pos': 0.5, 'reason': 'PE回到中高位+RSI中位'},
        {'cond': 'w_rsi6_pct100 > 70', 'pos': 0.5, 'reason': '周线高位'},
        {'cond': 'rsi_6 > 72', 'pos': 0.4, 'reason': 'RSI高位'},
        {'cond': 'close < ma_60 * 0.88', 'pos': 1.0, 'reason': '止损'},
    ],
})


# ══════════════════════════════════════════════════════════════
#  策略 C 优化: 基于 C2 (最稳定) 和 C3 微调
# ══════════════════════════════════════════════════════════════
configs_C = []

# C4: C2改进 — RSI区间操作+加入KDJ确认
configs_C.append({
    'name': 'C4_震荡RSIv2',
    'description': '【周期/价值股专用】震荡市RSI区间操作+KDJ确认。C2改进版：加入KDJ辅助提升信号质量。',
    'max_pos': 0.30,
    'buy_rules': [
        # C2核心 + KDJ确认
        {'cond': 'abs(idx_ret_20) < 5 and rsi_6 < 28 and kdj_j < 25 and boll_position < 0.30 and volume_ratio > 1.0',
         'pos': 0.15, 'reason': '横盘+RSI+KDJ双超卖+布林下轨'},
        {'cond': 'abs(idx_ret_20) < 5 and rsi_6 < 35 and w_rsi6_pct100 < 25 and close > ma_60 * 0.95',
         'pos': 0.08, 'reason': '横盘+RSI低+周线历史低+未破长线'},
    ],
    'buy_exclusions': [
        {'cond': 'rsi_6 > 55', 'reason': 'RSI不够低'},
        {'cond': 'close < ma_60 * 0.90', 'reason': '破长线不做'},
    ],
    'sell_rules': [
        {'cond': 'rsi_6 > 62 and boll_position > 0.60', 'pos': 0.6, 'reason': 'RSI中高+布林中上轨'},
        {'cond': 'rsi_6 > 70', 'pos': 0.5, 'reason': 'RSI超买'},
        {'cond': 'kdj_j > 80 and rsi_6 > 60', 'pos': 0.4, 'reason': 'KDJ超买+RSI高'},
        {'cond': 'close < ma_20 * 0.96', 'pos': 1.0, 'reason': '止损'},
    ],
})

# C5: 纯均值回归 — 布林+RSI+更快止盈
configs_C.append({
    'name': 'C5_均值回归',
    'description': '【周期/价值股专用】快进快出均值回归策略。低位买、中位即卖，追求高胜率。',
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
        # 快速止盈 — 回到中轨即走
        {'cond': 'boll_position > 0.45 and rsi_6 > 48', 'pos': 0.7, 'reason': '回到中轨即止盈'},
        {'cond': 'rsi_6 > 58', 'pos': 0.5, 'reason': 'RSI回到中位'},
        {'cond': 'close < ma_60 * 0.90', 'pos': 1.0, 'reason': '止损'},
        {'cond': 'pct_chg < -3.5', 'pos': 0.8, 'reason': '单日暴跌止损'},
    ],
})


# ══════════════════════════════════════════════════════════════
#  执行
# ══════════════════════════════════════════════════════════════
if __name__ == '__main__':
    all_results = []

    for cat, configs, stocks in [
        ('A(成长动量)', configs_A, STOCKS_A),
        ('B(防御超跌)', configs_B, STOCKS_B),
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

            tr = run_batch(strat, stocks, TRAIN_PERIOD)
            tr_sm = show(f'[训练集 2021-2024]', tr)

            vr = run_batch(strat, stocks, VALID_PERIOD)
            vr_sm = show(f'[验证集 2019-2020]', vr)

            if tr_sm and vr_sm:
                decay = vr_sm['sh'] / tr_sm['sh'] if tr_sm['sh'] > 0.01 else float('inf')
                status = 'PASS' if decay > 0.5 else ('OVERFIT' if tr_sm['sh'] > 0 else 'BOTH_NEG')
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

    print(f'\n\n{"#"*72}')
    print(f'  ROUND 2 总结 — 训练(2021-2024) vs 验证(2019-2020)')
    print(f'{"#"*72}')
    print(f'  {"Name":<16} {"Cat":<12} {"Tr_Sh":>6} {"Vr_Sh":>6} {"Decay":>6} {"Tr_Ann%":>8} {"Vr_Ann%":>8} {"Status":<8}')
    print(f'  {"-"*70}')
    for r in sorted(all_results, key=lambda x: min(x.get('tr_sh', 0), x.get('vr_sh', 0)), reverse=True):
        print(f'  {r["name"]:<16} {r["cat"]:<12} {r["tr_sh"]:>6.2f} {r["vr_sh"]:>6.2f} {r["decay"]:>6.2f} {r["tr_ann"]:>8.1f} {r["vr_ann"]:>8.1f} {r["status"]:<8}')
