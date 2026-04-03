"""Run comprehensive backtests on new strategies and output results."""
import sys, json, warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, '.')
from quant_system.backtest import backtest_engine
from quant_system.strategy import QuantStrategy, merge_buy_sell_strategies

with open('data/strategies.json', encoding='utf-8') as f:
    sdata = json.load(f)

pairs = [
    ('短线突破买入', '短线趋势卖出'),
    ('中线1反身性买入', '中线1反身性卖出'),
    ('中线2三重保护买入', '中线2估值修复卖出'),
    ('长线4421极限买入', '长线泡沫卖出'),
    ('超级长线价值买入', '超级长线卖出'),
]

# A-share only (these strategies are for A-shares)
test_stocks = ['600519', '002594', '600900', '000858', '002371', '600309']
periods = [('20210101', '20241231')]

print('='*80)
print('新策略回测结果汇总')
print('='*80)
print(f'{"策略组合":<20} {"股票":<8} {"周期":<12} {"总收益%":>8} {"年化%":>7} '
      f'{"最大回撤%":>9} {"交易次数":>8} {"胜率%":>7}')
print('-'*80)

all_results = []
for buy_name, sell_name in pairs:
    buy_strat = QuantStrategy.from_dict(sdata[buy_name])
    sell_strat = QuantStrategy.from_dict(sdata[sell_name])
    merged = merge_buy_sell_strategies(buy_strat, sell_strat)
    
    for code in test_stocks:
        for start, end in periods:
            try:
                result = backtest_engine.run_backtest(
                    code, merged, start, end, 500000
                )
                # win_rate is stored as 0-100 in BacktestResult
                win_rate_pct = result.win_rate if result.win_rate <= 100 else result.win_rate / 100
                print(f'{buy_name:<20} {code:<8} {start[:4]}-{end[:4]} '
                      f'{result.total_return_pct:>8.1f} {result.annual_return:>7.1f} '
                      f'{result.max_drawdown_pct:>9.1f} {result.total_trades:>8} '
                      f'{win_rate_pct:>7.1f}')
                all_results.append({
                    'strategy': buy_name,
                    'code': code,
                    'start': start,
                    'end': end,
                    'total_return': result.total_return_pct,
                    'annual_return': result.annual_return,
                    'max_drawdown': result.max_drawdown_pct,
                    'total_trades': result.total_trades,
                    'win_rate': win_rate_pct,
                    'sharpe': result.sharpe_ratio,
                })
            except Exception as e:
                print(f'{buy_name:<20} {code:<8} ERROR: {e}')
    print()

# Summary
print('='*80)
if all_results:
    import pandas as pd
    df_res = pd.DataFrame(all_results)
    print('\n按策略汇总（均值）:')
    print(f'{"策略":<22} {"平均收益%":>10} {"平均年化%":>10} {"平均回撤%":>10} {"平均交易次数":>12} {"平均胜率%":>10}')
    print('-'*70)
    for strat in df_res['strategy'].unique():
        sub = df_res[df_res['strategy'] == strat]
        print(f'{strat:<22} {sub["total_return"].mean():>10.1f} {sub["annual_return"].mean():>10.1f} '
              f'{sub["max_drawdown"].mean():>10.1f} {sub["total_trades"].mean():>12.1f} '
              f'{sub["win_rate"].mean():>10.1f}')
    
    # Zero trade strategies
    zero_trade = df_res[df_res['total_trades'] == 0]
    if not zero_trade.empty:
        print(f'\n[WARNING] 0交易次数 (条件太严或指标缺失): {zero_trade["strategy"].unique().tolist()}')

print('\nDone.')
