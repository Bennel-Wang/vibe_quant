import logging
import os
import sys
import traceback
from pprint import pprint

# ensure project root is importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('debug_strategy_ai')

from quant_system.strategy import StrategyParser, ai_decision_maker, strategy_manager


def dump_tmp_files():
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    for name in ('tmp_ai_last_response.txt', 'tmp_ai_last_decision_response.txt'):
        p = os.path.join(data_dir, name)
        if os.path.exists(p):
            logger.info(f"Found {name}, content:\n" + open(p, 'r', encoding='utf-8').read()[:2000])


def test_parse():
    try:
        sp = StrategyParser()
        logger.info(f"AI provider: {sp.ai_client.provider}, token present: {bool(getattr(sp.ai_client, 'token', None))}")
        prompt = '当 rsi_6 < 30 时买入; 当 rsi_6 > 70 时卖出'
        logger.info('Calling parse_natural_language with prompt: ' + prompt)
        rules = sp.parse_natural_language(prompt)
        logger.info('Parsed rules:')
        for r in rules:
            logger.info(str(r))
    except Exception as e:
        logger.exception('parse_natural_language failed')


def test_ai_decision():
    try:
        logger.info('Calling ai_decision_maker.make_decision for sh600519')
        dec = ai_decision_maker.make_decision('sh600519')
        logger.info('AI decision:')
        logger.info(str(dec))
    except Exception:
        logger.exception('ai_decision_maker failed')


def test_run_strategy():
    try:
        logger.info("Running built-in 'rsi' strategy on sh600519")
        res = strategy_manager.run_strategy('rsi', 'sh600519')
        logger.info('Strategy decision:')
        logger.info(str(res))
    except Exception:
        logger.exception('strategy_manager.run_strategy failed')


if __name__ == '__main__':
    try:
        dump_tmp_files()
        test_parse()
        test_ai_decision()
        test_run_strategy()
    except Exception:
        traceback.print_exc()
