"""
风控模块单元测试
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


@pytest.fixture
def risk_mgr():
    """创建独立的 RiskManager 实例（不使用全局单例）"""
    from quant_system.risk_manager import RiskManager
    rm = RiskManager()
    rm.update_capital(1_000_000, 1_000_000)
    return rm


class TestPositionLimit:
    def test_buy_within_limit(self, risk_mgr):
        """在限额内买入应通过"""
        result = risk_mgr.check_position_limit('600519.SH', 100, 1800.0)
        assert result.passed

    def test_buy_exceed_single_stock_limit(self, risk_mgr):
        """超过单只股票仓位限制（默认 30%）应被拒绝"""
        # 1000 股 × 1800 = 180W，超过 30W（30% of 100W）
        result = risk_mgr.check_position_limit('600519.SH', 1000, 1800.0)
        assert not result.passed
        assert result.action == 'reject'

    def test_suggested_position_is_within_limit(self, risk_mgr):
        """建议仓位应在限额之内"""
        result = risk_mgr.check_position_limit('600519.SH', 5000, 100.0)
        if not result.passed:
            # 建议仓位 × 价格 应 <= max_single_stock_ratio × total_capital
            suggested_value = result.suggested_position * 100.0
            max_value = risk_mgr.total_capital * risk_mgr.max_single_stock_ratio
            assert suggested_value <= max_value + 1  # 允许 1 元误差（整百股取整）


class TestStopLoss:
    def test_stop_loss_triggered(self, risk_mgr):
        """亏损超过止损比例应触发止损"""
        risk_mgr.update_position('TEST', 100, 100.0, 93.0)  # -7% < -5%
        result = risk_mgr.check_stop_loss_take_profit('TEST')
        assert result is not None
        assert result.action == 'sell'
        assert '止损' in result.message

    def test_take_profit_triggered(self, risk_mgr):
        """盈利超过止盈比例应触发止盈"""
        risk_mgr.update_position('TEST', 100, 100.0, 115.0)  # +15% > +10%
        result = risk_mgr.check_stop_loss_take_profit('TEST')
        assert result is not None
        assert result.action == 'sell'
        assert '止盈' in result.message

    def test_no_trigger_within_range(self, risk_mgr):
        """在止损止盈范围内不应触发"""
        risk_mgr.update_position('TEST', 100, 100.0, 103.0)  # +3%
        result = risk_mgr.check_stop_loss_take_profit('TEST')
        assert result is None


class TestTrailingStop:
    def test_trailing_stop_triggered(self):
        """从最高价回撤超过阈值应触发跟踪止损"""
        from quant_system.risk_manager import RiskManager
        rm = RiskManager()
        rm.trailing_stop_ratio = 0.05  # 5% 跟踪止损
        rm.update_capital(1_000_000, 1_000_000)
        rm.update_position('TEST', 100, 100.0, 120.0)
        # 模拟价格从 120 跌到 112（回撤 6.7%）
        rm._price_highs['TEST'] = 120.0
        rm.update_position('TEST', 100, 100.0, 112.0)
        result = rm.check_trailing_stop('TEST')
        assert result is not None
        assert result.action == 'sell'

    def test_trailing_stop_not_triggered(self):
        """回撤未达阈值不应触发"""
        from quant_system.risk_manager import RiskManager
        rm = RiskManager()
        rm.trailing_stop_ratio = 0.05
        rm.update_capital(1_000_000, 1_000_000)
        rm._price_highs['TEST'] = 120.0
        rm.update_position('TEST', 100, 100.0, 118.0)  # 回撤仅 1.7%
        result = rm.check_trailing_stop('TEST')
        assert result is None

    def test_trailing_stop_disabled(self, risk_mgr):
        """trailing_stop_ratio=0 时应始终返回 None"""
        risk_mgr.trailing_stop_ratio = 0.0
        risk_mgr._price_highs['TEST'] = 200.0
        risk_mgr.update_position('TEST', 100, 100.0, 50.0)  # 大跌
        result = risk_mgr.check_trailing_stop('TEST')
        assert result is None


class TestVaR:
    def test_var_empty_positions(self, risk_mgr):
        """无持仓时 VaR 应为 0"""
        result = risk_mgr.calculate_portfolio_var()
        assert result['var_1d'] == 0.0
        assert result['var_pct'] == 0.0

    def test_var_with_returns(self, risk_mgr):
        """传入历史收益率时 VaR 应大于 0"""
        import numpy as np
        import pandas as pd
        risk_mgr.update_position('TEST', 1000, 100.0, 100.0)
        risk_mgr.update_capital(100_000, 0)

        np.random.seed(0)
        daily_returns = pd.Series(np.random.normal(-0.001, 0.02, 252))
        result = risk_mgr.calculate_portfolio_var(
            daily_returns=daily_returns,
            portfolio_value=100_000
        )
        assert result['var_1d'] > 0
        assert result['cvar_1d'] >= result['var_1d']


class TestCash:
    def test_buy_insufficient_cash(self, risk_mgr):
        """资金不足时买入应被拒绝"""
        risk_mgr.update_capital(1_000_000, 1000)   # 仅 1000 元可用
        result = risk_mgr.check_risk_before_trade('600519.SH', 'buy', 100, 1800.0)
        assert not result.passed

    def test_sell_no_position(self, risk_mgr):
        """没有持仓时卖出应被拒绝"""
        result = risk_mgr.check_risk_before_trade('600519.SH', 'sell', 100, 1800.0)
        assert not result.passed
