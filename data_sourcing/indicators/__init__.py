"""
技术指标模块 - 优先使用Cython编译版本，回退到NumPy实现
"""
try:
    from indicators.indicators import compute_all_indicators
    BACKEND = "cython"
except ImportError:
    from indicators.indicators_numpy import compute_all_indicators
    BACKEND = "numpy"

from indicators.indicators_numpy import compute_rolling_percentile_rank
