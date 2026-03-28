"""
技术指标计算 - 纯NumPy实现（Cython不可用时的回退方案）
性能: NumPy向量化操作已经是C级别速度
"""
import numpy as np


def compute_change(close: np.ndarray) -> np.ndarray:
    result = np.empty_like(close)
    result[0] = np.nan
    result[1:] = np.diff(close)
    return result


def compute_pct_chg(close: np.ndarray) -> np.ndarray:
    result = np.empty_like(close)
    result[0] = np.nan
    with np.errstate(divide='ignore', invalid='ignore'):
        result[1:] = np.diff(close) / close[:-1] * 100.0
    return result


def _ema(data: np.ndarray, period: int) -> np.ndarray:
    alpha = 2.0 / (period + 1)
    result = np.empty_like(data)
    result[0] = data[0]
    for i in range(1, len(data)):
        result[i] = alpha * data[i] + (1 - alpha) * result[i - 1]
    return result


def compute_ma(data: np.ndarray, period: int) -> np.ndarray:
    n = len(data)
    result = np.full(n, np.nan)
    if n < period:
        return result
    cumsum = np.nancumsum(data)
    result[period - 1] = cumsum[period - 1] / period
    result[period:] = (cumsum[period:] - cumsum[:-period]) / period
    return result


def compute_rsi(close: np.ndarray, period: int) -> np.ndarray:
    n = len(close)
    result = np.full(n, np.nan)
    if n <= period:
        return result

    delta = np.diff(close)
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)

    avg_gain = np.mean(gain[:period])
    avg_loss = np.mean(loss[:period])

    if avg_loss == 0:
        result[period] = 100.0
    else:
        result[period] = 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)

    for i in range(period, n - 1):
        avg_gain = (avg_gain * (period - 1) + gain[i]) / period
        avg_loss = (avg_loss * (period - 1) + loss[i]) / period
        if avg_loss == 0:
            result[i + 1] = 100.0
        else:
            result[i + 1] = 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)

    return result


def compute_macd(close: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9):
    ema_fast = _ema(close, fast)
    ema_slow = _ema(close, slow)
    macd_line = ema_fast - ema_slow
    signal_line = _ema(macd_line, signal)
    histogram = 2.0 * (macd_line - signal_line)
    # 前slow-1个值设为NaN
    macd_line[:slow - 1] = np.nan
    signal_line[:slow - 1] = np.nan
    histogram[:slow - 1] = np.nan
    return macd_line, signal_line, histogram


def compute_bollinger(close: np.ndarray, period: int = 20, num_std: float = 2.0):
    n = len(close)
    middle = compute_ma(close, period)
    upper = np.full(n, np.nan)
    lower = np.full(n, np.nan)

    for i in range(period - 1, n):
        window = close[i - period + 1: i + 1]
        std_val = np.std(window, ddof=0)
        upper[i] = middle[i] + num_std * std_val
        lower[i] = middle[i] - num_std * std_val

    return middle, upper, lower


def compute_kdj(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                period: int = 9, k_smooth: int = 3, d_smooth: int = 3):
    n = len(close)
    k_values = np.full(n, np.nan)
    d_values = np.full(n, np.nan)
    j_values = np.full(n, np.nan)

    for i in range(period - 1, n):
        highest = np.max(high[i - period + 1: i + 1])
        lowest = np.min(low[i - period + 1: i + 1])

        if highest == lowest:
            rsv = 50.0
        else:
            rsv = (close[i] - lowest) / (highest - lowest) * 100.0

        if i == period - 1:
            k_values[i] = rsv
            d_values[i] = rsv
        else:
            k_values[i] = 2.0 / 3.0 * k_values[i - 1] + 1.0 / 3.0 * rsv
            d_values[i] = 2.0 / 3.0 * d_values[i - 1] + 1.0 / 3.0 * k_values[i]

        j_values[i] = 3.0 * k_values[i] - 2.0 * d_values[i]

    return k_values, d_values, j_values


def compute_volatility(close: np.ndarray, period: int = 20) -> np.ndarray:
    n = len(close)
    result = np.full(n, np.nan)
    with np.errstate(divide='ignore', invalid='ignore'):
        returns = np.diff(close) / close[:-1]
    returns = np.insert(returns, 0, np.nan)

    for i in range(period, n):
        window = returns[i - period + 1: i + 1]
        if np.any(np.isnan(window)):
            continue
        result[i] = np.std(window, ddof=0) * np.sqrt(252.0)

    return result


def compute_williams_r(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                       period: int = 14) -> np.ndarray:
    n = len(close)
    result = np.full(n, np.nan)

    for i in range(period - 1, n):
        highest = np.max(high[i - period + 1: i + 1])
        lowest = np.min(low[i - period + 1: i + 1])

        if highest == lowest:
            result[i] = -50.0
        else:
            result[i] = (highest - close[i]) / (highest - lowest) * -100.0

    return result


def compute_rolling_percentile_rank(data: np.ndarray, window: int,
                                    min_periods: int = None) -> np.ndarray:
    """计算滚动百分位排名 (0-100)
    result[i] = 当前值在过去 window 个值（含当前）中的百分位

    Args:
        data: 输入数组
        window: 滚动窗口大小
        min_periods: 最小有效期数（默认为 max(window//4, 30)）
                     当可用数据不足 window 但 >= min_periods 时仍计算百分位
    """
    if min_periods is None:
        min_periods = max(window // 4, 30)
    n = len(data)
    result = np.full(n, np.nan)
    for i in range(n):
        start = max(0, i - window + 1)
        window_data = data[start: i + 1]
        valid = window_data[~np.isnan(window_data)]
        if len(valid) < min_periods:
            continue
        current = data[i]
        if np.isnan(current):
            continue
        result[i] = np.sum(valid <= current) / len(valid) * 100.0
    return result


def compute_all_indicators(df_dict: dict) -> dict:
    """计算所有技术指标"""
    close = np.asarray(df_dict["close"], dtype=np.float64)
    high = np.asarray(df_dict["high"], dtype=np.float64)
    low = np.asarray(df_dict["low"], dtype=np.float64)
    vol = np.asarray(df_dict["vol"], dtype=np.float64)
    open_arr = np.asarray(df_dict["open"], dtype=np.float64)

    result = {}
    n = len(close)

    result["change"] = compute_change(close)
    result["pct_chg"] = compute_pct_chg(close)
    result["amount"] = vol * (open_arr + close) / 2.0

    if n > 6:
        result["rsi_6"] = compute_rsi(close, 6)
    if n > 12:
        result["rsi_12"] = compute_rsi(close, 12)
    if n > 24:
        result["rsi_24"] = compute_rsi(close, 24)

    if n > 5:
        result["ma_5"] = compute_ma(close, 5)
    if n > 20:
        result["ma_20"] = compute_ma(close, 20)
    if n > 60:
        result["ma_60"] = compute_ma(close, 60)

    if n > 26:
        macd, signal, hist = compute_macd(close)
        result["macd"] = macd
        result["macd_signal"] = signal
        result["macd_histogram"] = hist

    if n > 20:
        boll_m, boll_u, boll_l = compute_bollinger(close)
        result["boll_middle"] = boll_m
        result["boll_upper"] = boll_u
        result["boll_lower"] = boll_l

    if n > 9:
        k, d, j = compute_kdj(high, low, close)
        result["kdj_k"] = k
        result["kdj_d"] = d
        result["kdj_j"] = j

    if n > 20:
        result["volatility"] = compute_volatility(close)

    if n > 5:
        result["volume_ma_5"] = compute_ma(vol, 5)
    if n > 20:
        result["volume_ma_20"] = compute_ma(vol, 20)

    if n > 5:
        vma5 = compute_ma(vol, 5)
        with np.errstate(divide='ignore', invalid='ignore'):
            vr = np.where(vma5 == 0, np.nan, vol / vma5)
        vr[np.isinf(vr)] = np.nan
        result["volume_ratio"] = vr

    if n > 14:
        result["wr_14"] = compute_williams_r(high, low, close)

    return result
