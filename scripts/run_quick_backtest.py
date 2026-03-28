import sys, traceback
sys.path.insert(0, r'C:\Users\jinwa\Desktop\vibequation')
from quant_system.backtest import backtest_engine
from quant_system.strategy import strategy_manager

try:
    strat = strategy_manager.get_strategy('rsi')
    print('using strategy:', strat.name if strat else 'None')
    res = backtest_engine.run_backtest('600519', strat, '20230101', '20231231')
    print('Backtest finished')
    print('trades_len=', len(res.trades))
    print('duration_seconds=', getattr(res, 'duration_seconds', None))
    print('equity_rows=', len(res.equity_curve))
except Exception:
    traceback.print_exc()
