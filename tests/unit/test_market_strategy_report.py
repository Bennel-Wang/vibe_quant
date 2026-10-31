from quant_system.scheduler import TradingScheduler
from quant_system.strategy_matcher import (
    format_market_strategy_market_section,
    format_market_strategy_stock_sections,
)
from quant_system.web_app import app


def test_format_market_strategy_market_section_includes_scores_and_detail():
    content = format_market_strategy_market_section({
        'regime_label': '悲观',
        'regime_emoji': '🔴',
        'description': 'T分<28，V分<55，全部空仓',
        't_score': 23.9,
        'v_score': 50.0,
        'idx_ret_5': -2.48,
        'idx_ret_20': -8.57,
        'idx_ret_60': -5.99,
        'ma_trend': '空头排列',
        'rsi_level': '超卖',
        'detail': '测试详情',
        'date': '20261030',
    })

    assert '### 📊 大盘分析' in content
    assert '**悲观**（T=24 / V=50）' in content
    assert 'T分<28，V分<55，全部空仓' in content
    assert '分析日期: 20261030' in content
    assert '20日涨幅: -8.57%' in content
    assert '均线排列: 空头排列' in content
    assert 'RSI状态: 超卖' in content
    assert '测试详情' in content


def test_format_market_strategy_stock_sections_keeps_all_grouped_stocks():
    stocks = [
        {
            'code': f'{i:06d}',
            'name': f'观望股{i}',
            'action': 'watch',
            'scores': {'t_score': 40 + i, 'v_score': 20 + i},
            'reason': f'观望原因{i}',
        }
        for i in range(1, 12)
    ] + [
        {
            'code': f'{100000 + i:06d}',
            'name': f'空仓股{i}',
            'action': 'empty',
            'scores': {'t_score': 10 + i, 'v_score': 5 + i},
            'reason': f'空仓原因{i}',
        }
        for i in range(1, 14)
    ]

    content = format_market_strategy_stock_sections(stocks)

    assert '观望股11' in content
    assert '空仓股13' in content
    assert '### 🟡 观望（11 只）' in content
    assert '### ⚪ 空仓（13 只）' in content


def test_scheduler_market_strategy_report_includes_untruncated_lists(monkeypatch):
    scheduler = TradingScheduler()
    sent = {}
    stocks = [
        {
            'code': f'{i:06d}',
            'name': f'观望股{i}',
            'action': 'watch',
            'scores': {'t_score': 40 + i, 'v_score': 20 + i},
            'reason': f'观望原因{i}',
        }
        for i in range(1, 11)
    ] + [
        {
            'code': '300274',
            'name': '阳光电源',
            'action': 'watch',
            'scores': {'t_score': 48.3, 'v_score': 33.5},
            'reason': '乐观市，T分=48（趋势一般，等待更强信号）',
        }
    ]

    def fake_analyze_all_stocks():
        return {
            'market': {
                'regime_label': '乐观',
                'regime_emoji': '🟢',
                't_score': 58,
                'v_score': 24,
                'detail': '测试详情',
            },
            'stocks': stocks,
        }

    def fake_send(title, content):
        sent['title'] = title
        sent['content'] = content

    monkeypatch.setattr('quant_system.scheduler.strategy_matcher.analyze_all_stocks', fake_analyze_all_stocks)
    monkeypatch.setattr('quant_system.scheduler.notification_manager.send_markdown_message', fake_send)

    scheduler.run_market_strategy_analysis(force=True)

    assert '### 📊 大盘分析' in sent['content']
    assert '**乐观**（T=58 / V=24）' in sent['content']
    assert '测试详情' in sent['content']
    assert '阳光电源(300274)' in sent['content']
    assert '### 🟡 观望（11 只）' in sent['content']


def test_market_strategy_send_api_includes_untruncated_lists(monkeypatch):
    sent = {}
    stocks = [
        {
            'code': f'{i:06d}',
            'name': f'观望股{i}',
            'action': 'watch',
            'scores': {'t_score': 40 + i, 'v_score': 20 + i},
            'reason': f'观望原因{i}',
        }
        for i in range(1, 11)
    ] + [
        {
            'code': '300274',
            'name': '阳光电源',
            'action': 'watch',
            'scores': {'t_score': 48.3, 'v_score': 33.5},
            'reason': '乐观市，T分=48（趋势一般，等待更强信号）',
        }
    ]

    def fake_send(title, content):
        sent['title'] = title
        sent['content'] = content

    monkeypatch.setattr('quant_system.web_app.notification_manager.send_markdown_message', fake_send)

    client = app.test_client()
    response = client.post('/api/market/strategy-match/send', json={
        'data': {
            'market': {'regime_label': '乐观', 't_score': 58, 'v_score': 24, 'detail': '测试详情'},
            'stocks': stocks,
        }
    })

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['success'] is True
    assert '### 📊 大盘分析' in sent['content']
    assert '**乐观**（T=58 / V=24）' in sent['content']
    assert '阳光电源(300274)' in sent['content']
    assert '### 🟡 观望（11 只）' in sent['content']
