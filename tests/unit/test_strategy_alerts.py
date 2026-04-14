from types import SimpleNamespace

from quant_system.web_app import app


def test_strategy_alerts_compute_uses_backtest_snapshot_signal(monkeypatch):
    calls = []

    def fake_get_stock_by_code(code):
        return SimpleNamespace(name='阳光电源', full_code='300274.SZ')

    def fake_snapshot(code, buy_strategy_name, end_date_str, sell_strategy_name=None):
        calls.append((code, buy_strategy_name, end_date_str, sell_strategy_name))
        if len(calls) == 1:
            return {'return_pct': 12.34, 'signal': '观望'}
        return {'return_pct': 11.11, 'signal': '建议买入'}

    def fail_scheduler_signal(*args, **kwargs):
        raise AssertionError('scheduler signal path should not be used')

    monkeypatch.setattr('quant_system.web_app.stock_manager.get_stock_by_code', fake_get_stock_by_code)
    monkeypatch.setattr('quant_system.web_app._backtest_one_strategy_snapshot_with_end', fake_snapshot)
    monkeypatch.setattr('quant_system.web_app.scheduler._signal_from_decision', fail_scheduler_signal)

    client = app.test_client()
    response = client.post('/api/strategy/alerts/compute', json={
        'code': '300274.SZ',
        'buy_strategy': '低等策略1_4421极限底部买入',
        'sell_strategy': '价值动量共振卖出',
    })

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['success'] is True
    assert payload['return_today'] == 12.34
    assert payload['return_yesterday'] == 11.11
    assert payload['signal'] == '观望'
    assert len(calls) == 2
    assert calls[0][3] == '价值动量共振卖出'
    assert calls[1][3] == '价值动量共振卖出'
