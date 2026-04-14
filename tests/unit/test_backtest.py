import pandas as pd

from quant_system.backtest import BacktestEngine
from quant_system.strategy import QuantStrategy


def test_run_backtest_uses_full_history_for_indicator_calculation(monkeypatch):
    engine = BacktestEngine()
    calls = {'history': [], 'indicators': []}

    stock_df = pd.DataFrame([
        {'date': '2025-04-11', 'open': 10.0, 'high': 10.5, 'low': 9.8, 'close': 10.0, 'volume': 1000},
        {'date': '2025-04-14', 'open': 10.2, 'high': 10.6, 'low': 10.0, 'close': 10.3, 'volume': 1200},
    ])
    benchmark_df = pd.DataFrame([
        {'date': '2025-04-11', 'close': 3000.0},
        {'date': '2025-04-14', 'close': 3010.0},
    ])

    def fake_history(code, start_date='', end_date='', freq='day', adjust=True):
        calls['history'].append((code, start_date, end_date, freq))
        if code == '000001.SH':
            return benchmark_df.copy()
        if code == '000300.SH':
            return benchmark_df.copy()
        return stock_df.copy()

    def fake_indicators(code, start_date=None, end_date=None, freq='day'):
        calls['indicators'].append((code, start_date, end_date, freq))
        df = stock_df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df['amount'] = 0.0
        df['pct_chg'] = 0.0
        df['change'] = 0.0
        df['pe_ttm'] = 10.0
        df['pb'] = 1.0
        return df

    monkeypatch.setattr('quant_system.backtest.unified_data.get_historical_data', fake_history)
    monkeypatch.setattr('quant_system.backtest.technical_indicators.calculate_all_indicators', fake_indicators)
    monkeypatch.setattr(engine, '_merge_weekly_monthly', lambda code, df: df)

    strategy = QuantStrategy('空策略', '测试')
    result = engine.run_backtest('300274.SZ', strategy, '20250413', '20250414', initial_capital=100000)

    assert result.code == '300274.SZ'
    assert calls['indicators'] == [('300274.SZ', None, '20250414', 'day')]
    assert ('300274.SZ', '', '20250414', 'day') in calls['history']
    assert ('000001.SH', '', '20250414', 'day') in calls['history']
