"""Test Models API connectivity (ModelScope-style) and run a short backtest to verify local backtest function.

Usage: python scripts/test_api_and_backtest.py

Notes:
- This environment has no external network access; the API call will likely fail with a connection error. The script catches that and reports it.
- Replace BASE_URL and API_KEY with real values when running on a machine with internet.
"""
import json
import time
import traceback
import os, sys
# ensure project root is importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# ---- API test (ModelScope / OpenAI-like) ----
import requests

BASE_URL = 'https://api-inference.modelscope.cn/v1'  # provided example
API_KEY = 'ms-6cf6a665-e357-488a-bbe1-df18a2c20592'   # example token (from your message). Replace if needed.
MODEL_ID = 'deepseek-ai/DeepSeek-V3.2'

def test_models_api():
    url = BASE_URL.rstrip('/') + '/chat/completions'
    headers = {
        'Authorization': f'Bearer {API_KEY}',
        'Content-Type': 'application/json'
    }
    payload = {
        'model': MODEL_ID,
        'messages': [
            {'role': 'user', 'content': '9.9和9.11谁大'}
        ],
        'stream': True,
        # passthrough any provider-specific options
        'extra_body': {'enable_thinking': True}
    }

    print('Attempting to contact models API at', url)
    try:
        with requests.post(url, headers=headers, json=payload, stream=True, timeout=10) as resp:
            print('HTTP status:', resp.status_code)
            resp.raise_for_status()
            print('Streaming response (first 5 chunks):')
            count = 0
            # read raw bytes and decode with utf-8 to avoid mojibake
            for raw in resp.iter_lines(chunk_size=1024, decode_unicode=False):
                if not raw:
                    continue
                try:
                    line = raw.decode('utf-8')
                except Exception:
                    line = raw.decode('utf-8', errors='replace')
                # server-sent-events often prefix payload with 'data: '
                payload_line = line
                if payload_line.startswith('data:'):
                    payload_line = payload_line[len('data:'):].strip()
                payload_line = payload_line.strip()
                if payload_line == '[DONE]':
                    print('Stream finished by server [DONE]')
                    break
                # try parse json payload
                try:
                    data = json.loads(payload_line)
                    choices = data.get('choices', [])
                    for ch in choices:
                        delta = ch.get('delta', {})
                        thinking = delta.get('reasoning_content', '')
                        content = delta.get('content', '')
                        if thinking:
                            print('THINKING:', thinking, end='', flush=True)
                        if content:
                            print('CONTENT:', content, end='', flush=True)
                except json.JSONDecodeError:
                    # not json - print raw line for debugging
                    print('CHUNK:', payload_line)
                count += 1
                if count >= 5:
                    break
            print('\nFinished reading a few chunks.')
            return True
    except Exception as e:
        print('API call failed (expected if no network):', type(e), e)
        # print traceback for debugging
        traceback.print_exc()
        return False

# ---- Backtest test ----

def test_backtest_short():
    print('\nRunning a short local backtest to verify BacktestEngine...')
    try:
        from quant_system.backtest import BacktestEngine
        from quant_system.strategy import strategy_manager

        engine = BacktestEngine()
        # choose a stock with local history; set a short period
        code = 'sh600519'
        start = '20260301'
        end = '20260310'

        # try a simple strategy object: if your strategy manager exposes defaults, try to fetch one
        try:
            default_strategy = strategy_manager.get_default_strategy() if hasattr(strategy_manager, 'get_default_strategy') else None
        except Exception:
            default_strategy = None

        if default_strategy is None:
            # create a minimal dummy strategy complying with expected interface
            class DummyStrategy:
                name = 'dummy'
                # empty rules -> engine will treat as always hold
                rules = []
                def __init__(self):
                    self.name = 'dummy'
                    self.rules = []
            strat = DummyStrategy()
        else:
            strat = default_strategy

        result = engine.run_backtest(code, strat, start, end, initial_capital=1000000)
        print('Backtest completed. Summary:')
        print('code:', result.code, 'strategy:', result.strategy_name, 'start:', result.start_date, 'end:', result.end_date)
        print('initial -> final:', result.initial_capital, '->', result.final_capital)
        print('total trades:', result.total_trades)
        return True
    except Exception as e:
        print('Backtest test failed:', type(e), e)
        traceback.print_exc()
        return False


if __name__ == '__main__':
    api_ok = test_models_api()
    bt_ok = test_backtest_short()
    print('\nRESULT: API ok =', api_ok, ' | backtest ok =', bt_ok)
