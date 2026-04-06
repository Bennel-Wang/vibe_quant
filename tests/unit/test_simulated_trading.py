"""
模拟交易引擎单元测试

API 说明 (SimulatedBroker):
  - 构造: SimulatedBroker(account_id=int, initial_capital=float)
  - 下单: submit_order(code, side, shares, order_type, limit_price,
                        current_price, day_high, day_low) -> Order
  - 资金: broker.available_cash
  - 持仓: broker.positions[code].shares  (SimPosition 对象)
  - 持久化: save_state() / load_state()
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


# ─── 辅助 ──────────────────────────────────────────────────────────────────────

def _buy(broker, code, shares, price=100.0, high=None, low=None):
    """便捷市价买入"""
    return broker.submit_order(
        code=code, side='buy', shares=shares,
        order_type='market',
        current_price=price,
        day_high=high or price * 1.02,
        day_low=low or price * 0.98,
    )


def _sell(broker, code, shares, price=100.0, high=None, low=None):
    """便捷市价卖出"""
    return broker.submit_order(
        code=code, side='sell', shares=shares,
        order_type='market',
        current_price=price,
        day_high=high or price * 1.02,
        day_low=low or price * 0.98,
    )


@pytest.fixture
def broker(tmp_path):
    """创建一个使用临时目录的干净 SimulatedBroker 实例"""
    from quant_system.simulated_trading import SimulatedBroker
    b = SimulatedBroker.__new__(SimulatedBroker)
    # 手动初始化，避免依赖全局 config 的 data_storage.data_dir
    b.account_id = 9999
    b.initial_capital = 500_000.0
    b.available_cash = 500_000.0
    b.frozen_cash = 0.0
    b.positions = {}
    b.orders = {}
    b.trade_history = []
    b._order_seq = 0
    b.COMMISSION_RATE = 0.0003
    b.MIN_COMMISSION = 5.0
    b.STAMP_DUTY = 0.001
    b.SLIPPAGE = 0.001
    b._state_path = str(tmp_path / 'sim_broker_test.json')
    return b


# ─── 初始化 ──────────────────────────────────────────────────────────────────

class TestSimulatedBrokerInit:
    def test_initial_cash(self, broker):
        assert broker.available_cash == 500_000.0

    def test_empty_positions(self, broker):
        assert broker.positions == {}

    def test_empty_orders(self, broker):
        assert broker.orders == {}


# ─── 市价买入 ────────────────────────────────────────────────────────────────

class TestMarketBuy:
    def test_buy_executes(self, broker):
        """市价买单应成功成交"""
        order = _buy(broker, '600519.SH', 100, price=100.0)
        assert order.status == 'filled'
        assert '600519.SH' in broker.positions
        assert broker.positions['600519.SH'].shares == 100

    def test_buy_reduces_cash(self, broker):
        """买入后资金应减少"""
        initial_cash = broker.available_cash
        _buy(broker, '600519.SH', 100, price=100.0)
        assert broker.available_cash < initial_cash

    def test_buy_insufficient_cash(self, broker):
        """资金不足时市价买单应被拒绝"""
        order = _buy(broker, '600519.SH', 1000, price=10000.0)
        assert order.status == 'rejected'
        assert '600519.SH' not in broker.positions


# ─── 市价卖出 ────────────────────────────────────────────────────────────────

class TestMarketSell:
    def test_sell_executes(self, broker):
        """持有股票后市价卖出应成功"""
        _buy(broker, 'TEST', 200)
        order = _sell(broker, 'TEST', 200)
        assert order.status == 'filled'
        assert 'TEST' not in broker.positions

    def test_sell_without_position(self, broker):
        """无持仓时卖出应被拒绝"""
        order = _sell(broker, 'NOSTOCK', 100)
        assert order.status == 'rejected'

    def test_sell_partial(self, broker):
        """卖出部分持仓应正确更新数量"""
        _buy(broker, 'TEST', 300)
        _sell(broker, 'TEST', 100)
        assert broker.positions['TEST'].shares == 200


# ─── 限价单 ──────────────────────────────────────────────────────────────────

class TestLimitOrders:
    def test_limit_buy_fills_when_low_below(self, broker):
        """日内最低价 <= 限价时限价买单应成交"""
        order = broker.submit_order(
            code='TEST', side='buy', shares=100, order_type='limit',
            limit_price=100.0,
            current_price=102.0, day_high=104.0, day_low=99.0,
        )
        assert order.status == 'filled'

    def test_limit_buy_pending_when_low_above(self, broker):
        """日内最低价 > 限价时限价买单应挂单"""
        order = broker.submit_order(
            code='TEST', side='buy', shares=100, order_type='limit',
            limit_price=100.0,
            current_price=105.0, day_high=108.0, day_low=103.0,
        )
        assert order.status == 'pending'

    def test_limit_sell_fills_when_high_above(self, broker):
        """日内最高价 >= 限价时限价卖单应成交"""
        _buy(broker, 'TEST', 100)
        order = broker.submit_order(
            code='TEST', side='sell', shares=100, order_type='limit',
            limit_price=105.0,
            current_price=100.0, day_high=110.0, day_low=98.0,
        )
        assert order.status == 'filled'


# ─── 佣金 ────────────────────────────────────────────────────────────────────

class TestCommission:
    def test_commission_deducted(self, broker):
        """资金减少量应 > 纯股票市价（说明佣金已扣除）"""
        cash_before = broker.available_cash
        _buy(broker, 'TEST', 100, price=100.0)
        # 纯股票市值 = 100 * 100 = 10000
        assert cash_before - broker.available_cash > 10_000.0


# ─── 持久化 ──────────────────────────────────────────────────────────────────

class TestPersistence:
    def test_save_and_load(self, tmp_path):
        """保存状态后重新加载，持仓和资金应一致"""
        from quant_system.simulated_trading import SimulatedBroker
        state_file = str(tmp_path / 'sim_persist.json')

        def _make_broker():
            b = SimulatedBroker.__new__(SimulatedBroker)
            b.account_id = 42
            b.initial_capital = 200_000.0
            b.available_cash = 200_000.0
            b.frozen_cash = 0.0
            b.positions = {}
            b.orders = {}
            b.trade_history = []
            b._order_seq = 0
            b.COMMISSION_RATE = 0.0003
            b.MIN_COMMISSION = 5.0
            b.STAMP_DUTY = 0.001
            b.SLIPPAGE = 0.001
            b._state_path = state_file
            return b

        b1 = _make_broker()
        _buy(b1, 'XTEST', 200, price=50.0)
        b1.save_state()
        cash_after = b1.available_cash

        b2 = _make_broker()
        b2.load_state()
        assert 'XTEST' in b2.positions
        assert b2.positions['XTEST'].shares == 200
        assert abs(b2.available_cash - cash_after) < 0.01
