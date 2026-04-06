"""
技术指标计算单元测试

测试对象：
  - FreshTechnicalIndicators: calculate_simple_ma, calculate_simple_rsi
  - TechnicalIndicators: calculate_rsi, calculate_ma, calculate_all_indicators_from_df
  - utils/ohlcv.py: resample_to_weekly, resample_to_monthly
"""
import pytest
import numpy as np
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_ohlcv():
    """生成 200 根模拟日线数据"""
    np.random.seed(42)
    n = 200
    dates = pd.date_range('2023-01-01', periods=n, freq='B')
    close = 100 + np.cumsum(np.random.randn(n) * 0.5)
    close = np.clip(close, 10, None)
    return pd.DataFrame({
        'date': dates,
        'open': close * (1 + np.random.uniform(-0.005, 0.005, n)),
        'high': close * (1 + np.random.uniform(0, 0.01, n)),
        'low': close * (1 - np.random.uniform(0, 0.01, n)),
        'close': close,
        'volume': np.random.randint(1_000_000, 10_000_000, n).astype(float),
    })


@pytest.fixture
def fresh_calc():
    """返回 FreshTechnicalIndicators 实例"""
    from quant_system.indicators import FreshTechnicalIndicators
    return FreshTechnicalIndicators()


@pytest.fixture
def tech_calc():
    """返回 TechnicalIndicators 实例"""
    from quant_system.indicators import TechnicalIndicators
    return TechnicalIndicators()


# ─── FreshTechnicalIndicators.calculate_simple_ma ────────────────────────────

class TestSimpleMA:
    def test_length_matches_input(self, fresh_calc, sample_ohlcv):
        """MA 长度应与输入相同"""
        ma = fresh_calc.calculate_simple_ma(sample_ohlcv['close'], 5)
        assert len(ma) == len(sample_ohlcv)

    def test_ma5_value_at_window(self, fresh_calc, sample_ohlcv):
        """第 5 根 MA5（min_periods=1）应等于前 5 根均值"""
        ma = fresh_calc.calculate_simple_ma(sample_ohlcv['close'], 5)
        expected = sample_ohlcv['close'].iloc[:5].mean()
        assert abs(float(ma.iloc[4]) - expected) < 1e-6

    def test_short_series_no_crash(self, fresh_calc):
        """数据长度小于周期时不应抛异常（返回 NaN 序列）"""
        prices = pd.Series([10.0, 11.0, 12.0])
        result = fresh_calc.calculate_simple_ma(prices, 20)
        # 对于长度不足的情况，接受 NaN 或 rolling(min_periods=1) 的结果
        assert len(result) == 3

    def test_monotone_input_monotone_ma(self, fresh_calc):
        """单调递增序列的 MA 也应单调递增"""
        prices = pd.Series(range(1, 101), dtype=float)
        ma = fresh_calc.calculate_simple_ma(prices, 5)
        diffs = ma.diff().dropna()
        assert (diffs >= 0).all()


# ─── FreshTechnicalIndicators.calculate_simple_rsi ───────────────────────────

class TestSimpleRSI:
    def test_rsi_value_range(self, fresh_calc, sample_ohlcv):
        """RSI 值域应在 [0, 100]"""
        rsi = fresh_calc.calculate_simple_rsi(sample_ohlcv['close'], 14)
        valid = rsi.dropna()
        assert (valid >= 0).all() and (valid <= 100).all()

    def test_uptrend_rsi_high(self, fresh_calc):
        """单调上涨序列 RSI 应接近 100"""
        prices = pd.Series(range(1, 51), dtype=float)
        rsi = fresh_calc.calculate_simple_rsi(prices, 14)
        valid = rsi.dropna()
        assert (valid > 80).all()

    def test_downtrend_rsi_low(self, fresh_calc):
        """单调下跌序列 RSI 应接近 0"""
        prices = pd.Series(range(50, 0, -1), dtype=float)
        rsi = fresh_calc.calculate_simple_rsi(prices, 14)
        valid = rsi.dropna()
        assert (valid < 20).all()

    def test_constant_no_crash(self, fresh_calc):
        """价格不变时不应崩溃"""
        prices = pd.Series([50.0] * 30)
        rsi = fresh_calc.calculate_simple_rsi(prices, 14)
        assert len(rsi) == 30


# ─── TechnicalIndicators.calculate_rsi / calculate_ma ────────────────────────

class TestTechnicalIndicatorsRSI:
    def test_rsi_range(self, tech_calc, sample_ohlcv):
        rsi = tech_calc.calculate_rsi(sample_ohlcv['close'], 14)
        valid = rsi.dropna()
        assert (valid >= 0).all() and (valid <= 100).all()


class TestTechnicalIndicatorsMA:
    def test_returns_dict(self, tech_calc, sample_ohlcv):
        result = tech_calc.calculate_ma(sample_ohlcv['close'], [5, 20])
        assert isinstance(result, dict)
        assert 5 in result and 20 in result

    def test_lengths(self, tech_calc, sample_ohlcv):
        result = tech_calc.calculate_ma(sample_ohlcv['close'], [5, 20])
        assert len(result[5]) == len(sample_ohlcv)
        assert len(result[20]) == len(sample_ohlcv)


# ─── TechnicalIndicators.calculate_all_indicators_from_df ────────────────────

class TestAllIndicatorsFromDF:
    def test_macd_columns(self, tech_calc, sample_ohlcv):
        result = tech_calc.calculate_all_indicators_from_df(sample_ohlcv.copy())
        assert 'macd' in result.columns
        assert 'macd_signal' in result.columns
        assert 'macd_histogram' in result.columns

    def test_histogram_is_macd_minus_signal(self, tech_calc, sample_ohlcv):
        result = tech_calc.calculate_all_indicators_from_df(sample_ohlcv.copy())
        diff = (result['macd'] - result['macd_signal']).round(8)
        hist = result['macd_histogram'].round(8)
        pd.testing.assert_series_equal(diff, hist, check_names=False)

    def test_bollinger_order(self, tech_calc, sample_ohlcv):
        result = tech_calc.calculate_all_indicators_from_df(sample_ohlcv.copy())
        assert 'boll_upper' in result.columns
        assert 'boll_lower' in result.columns
        valid = result.dropna(subset=['boll_upper', 'boll_lower'])
        assert (valid['boll_upper'] >= valid['boll_lower']).all()

    def test_rsi_columns(self, tech_calc, sample_ohlcv):
        result = tech_calc.calculate_all_indicators_from_df(sample_ohlcv.copy())
        for p in [6, 12, 24]:
            assert f'rsi_{p}' in result.columns


# ─── utils/ohlcv.py ──────────────────────────────────────────────────────────

class TestOHLCVUtils:
    def test_resample_weekly(self, sample_ohlcv):
        from quant_system.utils.ohlcv import resample_to_weekly
        weekly = resample_to_weekly(sample_ohlcv)
        assert not weekly.empty
        assert 'close' in weekly.columns
        assert len(weekly) < len(sample_ohlcv)

    def test_resample_monthly(self, sample_ohlcv):
        from quant_system.utils.ohlcv import resample_to_monthly
        monthly = resample_to_monthly(sample_ohlcv)
        assert not monthly.empty
        assert len(monthly) < len(sample_ohlcv) / 4

    def test_resample_empty_df(self):
        from quant_system.utils.ohlcv import resample_to_weekly
        result = resample_to_weekly(pd.DataFrame())
        assert result.empty
