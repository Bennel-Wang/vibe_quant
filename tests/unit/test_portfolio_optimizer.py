"""
投资组合优化器单元测试

PortfolioOptimizer 通过 _fetch_returns() 从数据源获取收益率，
测试时 mock 该方法以使用预置数据，避免依赖真实数据文件。
"""
import pytest
import numpy as np
import pandas as pd
import sys
import os
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


@pytest.fixture
def returns_df():
    """3 只股票 × 252 天的模拟日收益率矩阵"""
    np.random.seed(7)
    n = 252
    df = pd.DataFrame({
        'A': np.random.normal(0.0005, 0.015, n),
        'B': np.random.normal(0.0003, 0.012, n),
        'C': np.random.normal(0.0008, 0.020, n),
    })
    return df


@pytest.fixture
def optimizer():
    from quant_system.portfolio_optimizer import PortfolioOptimizer
    return PortfolioOptimizer(n_simulations=500)  # 小模拟次数，加快测试


def run_optimize(optimizer, returns_df, method):
    """通过 mock _fetch_returns 来调用 optimize"""
    with patch.object(optimizer, '_fetch_returns', return_value=returns_df):
        return optimizer.optimize(
            codes=list(returns_df.columns),
            method=method,
        )


class TestEqualWeight:
    def test_weights_sum_to_one(self, optimizer, returns_df):
        result = run_optimize(optimizer, returns_df, 'equal_weight')
        assert abs(sum(result.weights.values()) - 1.0) < 1e-6

    def test_all_equal(self, optimizer, returns_df):
        result = run_optimize(optimizer, returns_df, 'equal_weight')
        weights = list(result.weights.values())
        assert max(weights) - min(weights) < 1e-9


class TestMinVolatility:
    def test_weights_sum_to_one(self, optimizer, returns_df):
        result = run_optimize(optimizer, returns_df, 'min_volatility')
        assert abs(sum(result.weights.values()) - 1.0) < 1e-6

    def test_all_non_negative(self, optimizer, returns_df):
        result = run_optimize(optimizer, returns_df, 'min_volatility')
        for w in result.weights.values():
            assert w >= -1e-6


class TestMaxSharpe:
    def test_weights_sum_to_one(self, optimizer, returns_df):
        result = run_optimize(optimizer, returns_df, 'max_sharpe')
        assert abs(sum(result.weights.values()) - 1.0) < 1e-6

    def test_sharpe_in_metrics(self, optimizer, returns_df):
        result = run_optimize(optimizer, returns_df, 'max_sharpe')
        assert 'sharpe_ratio' in result.metrics

    def test_annual_return_in_metrics(self, optimizer, returns_df):
        result = run_optimize(optimizer, returns_df, 'max_sharpe')
        assert 'annual_return' in result.metrics
        assert isinstance(result.metrics['annual_return'], float)


class TestMonteCarloOptimize:
    def test_internal_monte_carlo(self, optimizer, returns_df):
        """_monte_carlo_optimize 应返回和为 1 的权重数组"""
        weights = optimizer._monte_carlo_optimize(returns_df, 'max_sharpe', {})
        assert abs(weights.sum() - 1.0) < 1e-6
        assert len(weights) == len(returns_df.columns)

    def test_metrics_calculation(self, optimizer, returns_df):
        """_calc_metrics 应返回包含 sharpe_ratio 的字典"""
        w = np.array([1/3, 1/3, 1/3])
        metrics = optimizer._calc_metrics(returns_df, w)
        assert 'sharpe_ratio' in metrics
        assert 'annual_return' in metrics
        assert 'annual_volatility' in metrics


class TestEdgeCases:
    def test_single_asset_equal_weight(self, optimizer):
        """单资产组合权重应为 1.0"""
        single = pd.DataFrame({'A': np.random.normal(0.001, 0.01, 100)})
        with patch.object(optimizer, '_fetch_returns', return_value=single):
            result = optimizer.optimize(codes=['A'], method='equal_weight')
        assert abs(result.weights['A'] - 1.0) < 1e-6

    def test_invalid_method_raises(self, optimizer, returns_df):
        """未知方法应抛出 ValueError"""
        with pytest.raises(ValueError):
            with patch.object(optimizer, '_fetch_returns', return_value=returns_df):
                optimizer.optimize(codes=['A', 'B', 'C'], method='unknown_method')
