"""
验证策略-大盘匹配的回测脚本
在不同大盘环境时段回测对应策略，验证匹配逻辑的有效性
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from quant_system.backtest import backtest_engine
from quant_system.strategy import strategy_manager

# 确保策略加载
strategy_manager.reload_from_file()

# ===== 大盘环境对应的测试时段 =====
# 乐观时段: 2020.04-2021.02 (疫情后V型反弹大牛市)
# 混沌时段: 2023.01-2023.12 (全年震荡无方向)
# 悲观时段: 2022.01-2022.10 (持续单边下跌)
# 极度悲观: 2018.06-2019.01 (贸易战+去杠杆)

REGIME_PERIODS = {
    'optimistic': [
        ('2020-04-01', '2021-02-28', '疫后反弹牛市'),
        ('2024-09-01', '2024-12-31', '924行情'),
    ],
    'chaotic': [
        ('2023-01-01', '2023-12-31', '2023全年震荡'),
        ('2021-06-01', '2022-01-01', '2021下半年震荡'),
    ],
    'pessimistic': [
        ('2022-01-01', '2022-10-31', '2022单边下跌'),
        ('2024-05-01', '2024-08-31', '2024中期回调'),
    ],
    'extremely_pessimistic': [
        ('2018-06-01', '2019-01-31', '贸易战去杠杆'),
        ('2023-12-01', '2024-02-28', '2024年初急跌'),
    ],
}

# 策略-大盘映射(买入策略名)
REGIME_STRATEGIES = {
    'optimistic': [
        'RSI超卖反弹买入', 'MACD金叉买入', '短线突破买入',
        '中线1反身性买入', '成长股强势动量买入',
    ],
    'chaotic': [
        '中线2三重保护买入', '震荡市RSI波段买入',
        '布林带下轨买入', 'KDJ超卖买入',
    ],
    'pessimistic': [
        '防御股熊市超跌买入',
    ],
    'extremely_pessimistic': [
        '长线4421极限买入',
    ],
}

# 股票类型代表股
REGIME_STOCKS = {
    'optimistic': [
        ('002594.SZ', '比亚迪', '成长'),
        ('300750.SZ', '宁德时代', '成长'),
        ('600519.SH', '贵州茅台', '防御'),
    ],
    'chaotic': [
        ('600309.SH', '万华化学', '周期'),
        ('600900.SH', '长江电力', '防御'),
        ('002572.SZ', '索菲亚', '周期'),
    ],
    'pessimistic': [
        ('600900.SH', '长江电力', '防御'),
        ('600519.SH', '贵州茅台', '防御'),
        ('000858.SZ', '五粮液', '防御'),
    ],
    'extremely_pessimistic': [
        ('002594.SZ', '比亚迪', '成长'),
        ('600519.SH', '贵州茅台', '防御'),
        ('300750.SZ', '宁德时代', '成长'),
    ],
}

def run_one(code, strategy_name, start, end):
    """运行单次回测，返回结果字典"""
    strat = strategy_manager.get_strategy(strategy_name)
    if not strat:
        return None
    try:
        sd = start.replace('-', '')
        ed = end.replace('-', '')
        result = backtest_engine.run_backtest(code, strat, sd, ed, initial_capital=1000000)
        return {
            'sharpe': round(result.sharpe_ratio, 2),
            'return_pct': round(result.total_return_pct, 2),
            'annual': round(result.annual_return * 100, 2),
            'max_dd': round(result.max_drawdown_pct, 2),
            'trades': result.total_trades,
            'win_rate': round(result.win_rate, 1),
        }
    except Exception as e:
        return {'error': str(e)[:80]}

def main():
    results = {}
    
    for regime, periods in REGIME_PERIODS.items():
        regime_label = {'optimistic':'乐观','chaotic':'混沌','pessimistic':'悲观','extremely_pessimistic':'极度悲观'}[regime]
        print(f"\n{'='*60}")
        print(f"  大盘环境: {regime_label}")
        print(f"{'='*60}")
        
        strategies = REGIME_STRATEGIES[regime]
        stocks = REGIME_STOCKS[regime]
        
        for period_start, period_end, period_name in periods:
            print(f"\n  --- {period_name} ({period_start} ~ {period_end}) ---")
            
            for strategy_name in strategies:
                for code, name, stype in stocks:
                    result = run_one(code, strategy_name, period_start, period_end)
                    if result and 'error' not in result:
                        key = f"{regime}|{strategy_name}|{code}"
                        results.setdefault(key, []).append(result)
                        print(f"    {strategy_name:20s} + {name:6s}({stype}): "
                              f"Sharpe={result['sharpe']:6.2f}  "
                              f"Ret={result['return_pct']:7.2f}%  "
                              f"DD={result['max_dd']:6.2f}%  "
                              f"WR={result['win_rate']:5.1f}%  "
                              f"Trades={result['trades']}")
                    elif result:
                        print(f"    {strategy_name:20s} + {name:6s}: ERROR: {result.get('error','')}")
    
    # 汇总: 找出每个大盘环境下表现最好的策略
    print(f"\n\n{'='*60}")
    print(f"  === 最优策略汇总 ===")
    print(f"{'='*60}")
    
    for regime in REGIME_PERIODS:
        regime_label = {'optimistic':'乐观','chaotic':'混沌','pessimistic':'悲观','extremely_pessimistic':'极度悲观'}[regime]
        print(f"\n  [{regime_label}]:")
        
        strategy_avg = {}
        for key, res_list in results.items():
            if key.startswith(regime + '|'):
                parts = key.split('|')
                sname = parts[1]
                avg_sharpe = sum(r['sharpe'] for r in res_list) / len(res_list)
                avg_ret = sum(r['return_pct'] for r in res_list) / len(res_list)
                strategy_avg.setdefault(sname, {'sharpes': [], 'returns': []})
                strategy_avg[sname]['sharpes'].append(avg_sharpe)
                strategy_avg[sname]['returns'].append(avg_ret)
        
        for sname, data in sorted(strategy_avg.items(), key=lambda x: -sum(x[1]['sharpes'])/len(x[1]['sharpes'])):
            avg_s = sum(data['sharpes']) / len(data['sharpes'])
            avg_r = sum(data['returns']) / len(data['returns'])
            print(f"    {sname:24s}: avg_Sharpe={avg_s:6.2f}  avg_Ret={avg_r:7.2f}%")

if __name__ == '__main__':
    main()
