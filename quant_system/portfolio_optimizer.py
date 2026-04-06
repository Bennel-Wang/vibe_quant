"""
投资组合优化模块

支持三种优化目标：
  - max_sharpe       : 最大化夏普比率（马科维兹均值-方差框架）
  - min_volatility   : 最小化组合波动率
  - equal_weight     : 等权分配（基准方案）

依赖：numpy / pandas（无需 scipy，使用梯度下降近似）
"""

import logging
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .config_manager import config
from .data_source import unified_data

logger = logging.getLogger(__name__)

RISK_FREE_RATE = 0.02   # 年化无风险利率（约等于国债收益率）
TRADING_DAYS = 252


class PortfolioOptimizer:
    """
    均值-方差组合优化器

    用法示例:
        optimizer = PortfolioOptimizer()
        result = optimizer.optimize(
            codes=["600519.SH", "000858.SZ", "601318.SH"],
            method="max_sharpe",
            start_date="20230101",
        )
        print(result.weights)    # {'600519.SH': 0.45, ...}
        print(result.metrics)    # {'annual_return': 0.18, 'volatility': 0.12, 'sharpe': 1.5}
    """

    def __init__(self, risk_free_rate: float = RISK_FREE_RATE, n_simulations: int = 5000):
        """
        Args:
            risk_free_rate: 年化无风险利率（默认 2%）
            n_simulations : 蒙特卡洛模拟次数（用于 max_sharpe 近似）
        """
        self.risk_free_rate = risk_free_rate
        self.n_simulations = n_simulations

    # ─── 数据获取 ─────────────────────────────────────────────────────────

    def _fetch_returns(self, codes: List[str],
                       start_date: str, end_date: str = None) -> pd.DataFrame:
        """
        获取多只股票的日收益率矩阵。

        Returns:
            DataFrame，列=股票代码，行=交易日，值=当日涨跌幅（小数）
        """
        returns_dict = {}
        for code in codes:
            try:
                df = unified_data.get_historical_data(code, start_date, end_date)
                if df is None or df.empty or 'close' not in df.columns:
                    logger.warning(f"无法获取 {code} 历史数据，跳过")
                    continue
                df = df.sort_values('date').reset_index(drop=True)
                ret = df['close'].astype(float).pct_change().dropna()
                returns_dict[code] = ret.values
            except Exception as e:
                logger.warning(f"获取 {code} 收益率失败: {e}")

        if not returns_dict:
            return pd.DataFrame()

        min_len = min(len(v) for v in returns_dict.values())
        aligned = {k: v[-min_len:] for k, v in returns_dict.items()}
        return pd.DataFrame(aligned)

    # ─── 核心优化 ─────────────────────────────────────────────────────────

    def optimize(
        self,
        codes: List[str],
        method: str = "max_sharpe",
        start_date: str = "20230101",
        end_date: str = None,
        constraints: Optional[Dict] = None,
    ) -> "OptimizationResult":
        """
        执行组合优化。

        Args:
            codes      : 股票代码列表
            method     : 优化目标 ('max_sharpe' | 'min_volatility' | 'equal_weight')
            start_date : 历史数据起始日期
            end_date   : 历史数据截止日期（默认今天）
            constraints: 约束条件，如 {'min_weight': 0.05, 'max_weight': 0.4}

        Returns:
            OptimizationResult
        """
        returns_df = self._fetch_returns(codes, start_date, end_date)
        valid_codes = list(returns_df.columns)

        if len(valid_codes) < 2:
            logger.warning(f"有效股票不足2只（{valid_codes}），返回等权方案")
            method = "equal_weight"

        if method == "equal_weight":
            weights = {c: 1.0 / len(valid_codes) for c in valid_codes}
        elif method in ("max_sharpe", "min_volatility"):
            weights_arr = self._monte_carlo_optimize(
                returns_df, method, constraints or {}
            )
            weights = dict(zip(valid_codes, weights_arr))
        else:
            raise ValueError(f"未知优化方法: {method}，支持 max_sharpe / min_volatility / equal_weight")

        metrics = self._calc_metrics(returns_df, np.array(list(weights.values())))
        return OptimizationResult(weights=weights, metrics=metrics,
                                  method=method, codes=valid_codes)

    def _monte_carlo_optimize(
        self,
        returns_df: pd.DataFrame,
        method: str,
        constraints: Dict,
    ) -> np.ndarray:
        """
        蒙特卡洛随机权重搜索（无需 scipy，简单有效）

        返回最优权重数组。
        """
        n = len(returns_df.columns)
        mu = returns_df.mean().values * TRADING_DAYS           # 年化期望收益
        cov = returns_df.cov().values * TRADING_DAYS           # 年化协方差矩阵

        min_w = constraints.get('min_weight', 0.01)
        max_w = constraints.get('max_weight', 1.0)

        best_score = -np.inf if method == 'max_sharpe' else np.inf
        best_weights = np.ones(n) / n

        rng = np.random.default_rng(42)
        for _ in range(self.n_simulations):
            raw = rng.random(n)
            # 裁剪到 [min_w, max_w] 后归一化
            w = np.clip(raw, min_w, max_w)
            w /= w.sum()

            port_ret = np.dot(w, mu)
            port_vol = np.sqrt(w @ cov @ w)

            if port_vol < 1e-10:
                continue

            if method == 'max_sharpe':
                score = (port_ret - self.risk_free_rate) / port_vol
                if score > best_score:
                    best_score = score
                    best_weights = w
            else:  # min_volatility
                if port_vol < best_score or best_score == np.inf:
                    best_score = port_vol
                    best_weights = w

        return best_weights

    def _calc_metrics(self, returns_df: pd.DataFrame, weights: np.ndarray) -> Dict:
        """计算组合指标"""
        mu = returns_df.mean().values * TRADING_DAYS
        cov = returns_df.cov().values * TRADING_DAYS

        port_ret = float(np.dot(weights, mu))
        port_vol = float(np.sqrt(weights @ cov @ weights))
        sharpe = (port_ret - self.risk_free_rate) / port_vol if port_vol > 0 else 0.0

        # 最大回撤（基于加权日收益序列）
        daily_port_ret = returns_df.values @ weights
        cumulative = (1 + daily_port_ret).cumprod()
        running_max = np.maximum.accumulate(cumulative)
        drawdowns = (cumulative - running_max) / running_max
        max_drawdown = float(drawdowns.min())

        return {
            'annual_return': round(port_ret * 100, 4),
            'annual_volatility': round(port_vol * 100, 4),
            'sharpe_ratio': round(sharpe, 4),
            'max_drawdown': round(max_drawdown * 100, 4),
        }

    def efficient_frontier(
        self,
        codes: List[str],
        start_date: str = "20230101",
        n_points: int = 50,
    ) -> List[Dict]:
        """
        生成有效前沿上的点（用于可视化）

        Returns:
            List of {'return': x, 'volatility': y, 'sharpe': z, 'weights': {...}}
        """
        returns_df = self._fetch_returns(codes, start_date)
        if returns_df.empty:
            return []

        mu = returns_df.mean().values * TRADING_DAYS
        cov = returns_df.cov().values * TRADING_DAYS
        n = len(codes)
        rng = np.random.default_rng(0)
        points = []
        for _ in range(max(n_points * 100, 5000)):
            w = rng.random(n)
            w /= w.sum()
            r = float(np.dot(w, mu))
            v = float(np.sqrt(w @ cov @ w))
            s = (r - self.risk_free_rate) / v if v > 0 else 0
            points.append({'return': round(r * 100, 4), 'volatility': round(v * 100, 4),
                           'sharpe': round(s, 4)})

        # 取帕累托前沿（给定波动率下最高收益）
        points.sort(key=lambda x: x['volatility'])
        frontier = []
        max_ret = -np.inf
        for pt in points:
            if pt['return'] > max_ret:
                max_ret = pt['return']
                frontier.append(pt)
        return frontier[:n_points]


class OptimizationResult:
    """优化结果"""

    def __init__(self, weights: Dict[str, float], metrics: Dict,
                 method: str, codes: List[str]):
        self.weights = weights     # {code: weight}
        self.metrics = metrics     # {annual_return, annual_volatility, sharpe_ratio, max_drawdown}
        self.method = method
        self.codes = codes

    def to_dict(self) -> Dict:
        return {
            'method': self.method,
            'weights': {k: round(v * 100, 2) for k, v in self.weights.items()},  # 转为百分比
            'metrics': self.metrics,
        }

    def __repr__(self):
        lines = [f"OptimizationResult ({self.method}):"]
        for code, w in sorted(self.weights.items(), key=lambda x: -x[1]):
            lines.append(f"  {code}: {w * 100:.1f}%")
        lines.append(f"  夏普比率: {self.metrics.get('sharpe_ratio', 0):.2f}")
        lines.append(f"  年化收益: {self.metrics.get('annual_return', 0):.2f}%")
        lines.append(f"  年化波动: {self.metrics.get('annual_volatility', 0):.2f}%")
        return "\n".join(lines)


# 全局实例
portfolio_optimizer = PortfolioOptimizer()
