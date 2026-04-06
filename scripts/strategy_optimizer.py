"""
Strategy optimizer: runs backtests for all strategy pairs across multiple stocks,
reports results, and saves a summary. Pure read/analyze — no Python code changes.
Usage: python scripts/strategy_optimizer.py
"""
import sys, os, json, warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from quant_system.backtest import BacktestEngine
from quant_system.strategy import strategy_manager, QuantStrategy
from quant_system.config_manager import config

# ── Test universe ──────────────────────────────────────────────────────────
TEST_STOCKS = [
    '600519',   # 贵州茅台 (高价消费白马)
    '000858',   # 五粮液
    '601318',   # 中国平安 (金融蓝筹)
    '002594',   # 比亚迪 (成长+新能源)
    '300750',   # 宁德时代 (成长高波动)
    '600900',   # 长江电力 (防御低波动)
]
START_DATE = '2021-01-01'
END_DATE   = '2025-12-31'
CAPITAL    = 1_000_000

engine = BacktestEngine()
strategy_manager.reload_from_file()

def make_combined(buy_key, sell_key):
    """Merge buy+sell strategy rules into a single QuantStrategy for backtesting."""
    buy_s  = strategy_manager.get_strategy(buy_key)
    sell_s = strategy_manager.get_strategy(sell_key)
    if not buy_s or not sell_s:
        return None
    combined = QuantStrategy(f'{buy_key}+{sell_key}', '')
    combined.rules = list(buy_s.rules) + list(sell_s.rules)
    combined.exclusion_rules = list(buy_s.exclusion_rules) + list(sell_s.exclusion_rules)
    return combined

def run_pair(buy_key, sell_key, stocks=TEST_STOCKS):
    combined = make_combined(buy_key, sell_key)
    if not combined:
        return None
    results = []
    for code in stocks:
        try:
            r = engine.run_backtest(code, combined, START_DATE, END_DATE,
                                    initial_capital=CAPITAL, per_trade_ratio=0.1)
            if r and r.total_trades > 0:
                results.append({
                    'code': code,
                    'trades': r.total_trades,
                    'return_pct': round(r.total_return_pct, 2),
                    'annual': round(r.annual_return, 2),
                    'drawdown': round(r.max_drawdown_pct, 2),
                    'win_rate': round(r.win_rate, 2),
                    'sharpe': round(r.sharpe_ratio, 2),
                })
        except Exception as e:
            pass
    return results

def avg_return(results):
    if not results:
        return -999
    return round(sum(r['return_pct'] for r in results) / len(results), 2)

def summarize(results, label=''):
    if not results:
        return f'{label}: no trades'
    avg_r  = avg_return(results)
    avg_an = round(sum(r['annual'] for r in results)/len(results), 2)
    avg_dd = round(sum(r['drawdown'] for r in results)/len(results), 2)
    avg_wr = round(sum(r['win_rate'] for r in results)/len(results), 2)
    avg_sh = round(sum(r['sharpe'] for r in results)/len(results), 2)
    n = len(results)
    return (f'{label}: n={n} | avg_return={avg_r}% | annual={avg_an}% '
            f'| drawdown={avg_dd}% | win_rate={avg_wr}% | sharpe={avg_sh}')

# ── Strategy pairs (by position index, matching web_app.py pairing) ───────
all_strats = strategy_manager.list_strategies()
user_buy  = [(k, strategy_manager.get_strategy(k)) for k in all_strats
             if k.endswith('买入') and strategy_manager.get_strategy(k) and strategy_manager.get_strategy(k).rules]
user_sell = [(k, strategy_manager.get_strategy(k)) for k in all_strats
             if k.endswith('卖出') and strategy_manager.get_strategy(k) and strategy_manager.get_strategy(k).rules]

pairs = []
for i in range(max(len(user_buy), len(user_sell))):
    bk = user_buy[i][0]  if i < len(user_buy)  else None
    sk = user_sell[i][0] if i < len(user_sell) else None
    pairs.append((bk, sk))

if __name__ == '__main__':
    print('Running backtest for all strategy pairs...')
    print(f'Stocks: {TEST_STOCKS}')
    print(f'Period: {START_DATE} ~ {END_DATE}')
    print()

    summary = {}
    for bk, sk in pairs:
        if not bk or not sk:
            continue
        label = f'{bk} + {sk}'
        results = run_pair(bk, sk)
        s = summarize(results, label)
        summary[label] = {'results': results, 'avg_return': avg_return(results)}
        print(s)
        if results:
            for r in results:
                print(f'    {r["code"]}: {r["trades"]}trades ret={r["return_pct"]}% annual={r["annual"]}% '
                      f'dd={r["drawdown"]}% wr={r["win_rate"]}% sharpe={r["sharpe"]}')
        print()

    # Sort by avg return
    ranked = sorted([(k, v['avg_return']) for k, v in summary.items() if v['avg_return'] > -999],
                    key=lambda x: x[1], reverse=True)
    print('=== RANKING BY AVG RETURN ===')
    for rank, (label, ret) in enumerate(ranked, 1):
        print(f'  #{rank} {ret:+.1f}%  {label}')
