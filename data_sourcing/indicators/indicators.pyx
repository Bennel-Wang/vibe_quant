# cython: boundscheck=False, wraparound=False, cdivision=True
"""
Cython加速的技术指标计算模块
编译: python indicators/setup.py build_ext --inplace
"""
import numpy as np
cimport numpy as np
from libc.math cimport sqrt, fabs, NAN, isnan

ctypedef np.float64_t DTYPE_t


def compute_change(np.ndarray[DTYPE_t, ndim=1] close):
    """计算涨跌额"""
    cdef int n = close.shape[0]
    cdef np.ndarray[DTYPE_t, ndim=1] result = np.empty(n, dtype=np.float64)
    result[0] = NAN
    cdef int i
    for i in range(1, n):
        if isnan(close[i]) or isnan(close[i-1]):
            result[i] = NAN
        else:
            result[i] = close[i] - close[i-1]
    return result


def compute_pct_chg(np.ndarray[DTYPE_t, ndim=1] close):
    """计算涨跌幅(%)"""
    cdef int n = close.shape[0]
    cdef np.ndarray[DTYPE_t, ndim=1] result = np.empty(n, dtype=np.float64)
    result[0] = NAN
    cdef int i
    for i in range(1, n):
        if isnan(close[i]) or isnan(close[i-1]) or close[i-1] == 0:
            result[i] = NAN
        else:
            result[i] = (close[i] - close[i-1]) / close[i-1] * 100.0
    return result


def compute_ma(np.ndarray[DTYPE_t, ndim=1] data, int period):
    """计算移动平均线"""
    cdef int n = data.shape[0]
    cdef np.ndarray[DTYPE_t, ndim=1] result = np.empty(n, dtype=np.float64)
    cdef double total = 0.0
    cdef int i, count = 0
    for i in range(n):
        if not isnan(data[i]):
            total += data[i]
            count += 1
        if i >= period:
            if not isnan(data[i - period]):
                total -= data[i - period]
                count -= 1
        if count >= period:
            result[i] = total / period
        else:
            result[i] = NAN
    return result


def compute_rsi(np.ndarray[DTYPE_t, ndim=1] close, int period):
    """计算RSI (Relative Strength Index)"""
    cdef int n = close.shape[0]
    cdef np.ndarray[DTYPE_t, ndim=1] result = np.empty(n, dtype=np.float64)
    cdef double gain, loss, avg_gain, avg_loss, delta
    cdef int i

    for i in range(period + 1):
        result[i] = NAN

    # 初始平均
    avg_gain = 0.0
    avg_loss = 0.0
    for i in range(1, period + 1):
        delta = close[i] - close[i-1]
        if delta > 0:
            avg_gain += delta
        else:
            avg_loss += fabs(delta)
    avg_gain /= period
    avg_loss /= period

    if avg_loss == 0:
        result[period] = 100.0
    else:
        result[period] = 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)

    # 平滑计算
    for i in range(period + 1, n):
        delta = close[i] - close[i-1]
        if isnan(delta):
            result[i] = NAN
            continue
        if delta > 0:
            gain = delta
            loss = 0.0
        else:
            gain = 0.0
            loss = fabs(delta)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        if avg_loss == 0:
            result[i] = 100.0
        else:
            result[i] = 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)

    return result


def compute_macd(np.ndarray[DTYPE_t, ndim=1] close, int fast=12, int slow=26, int signal=9):
    """计算MACD (macd_line, signal_line, histogram)"""
    cdef int n = close.shape[0]
    cdef np.ndarray[DTYPE_t, ndim=1] ema_fast = np.empty(n, dtype=np.float64)
    cdef np.ndarray[DTYPE_t, ndim=1] ema_slow = np.empty(n, dtype=np.float64)
    cdef np.ndarray[DTYPE_t, ndim=1] macd_line = np.empty(n, dtype=np.float64)
    cdef np.ndarray[DTYPE_t, ndim=1] signal_line = np.empty(n, dtype=np.float64)
    cdef np.ndarray[DTYPE_t, ndim=1] histogram = np.empty(n, dtype=np.float64)
    cdef double alpha_fast = 2.0 / (fast + 1)
    cdef double alpha_slow = 2.0 / (slow + 1)
    cdef double alpha_signal = 2.0 / (signal + 1)
    cdef int i

    ema_fast[0] = close[0]
    ema_slow[0] = close[0]
    for i in range(1, n):
        if isnan(close[i]):
            ema_fast[i] = ema_fast[i-1]
            ema_slow[i] = ema_slow[i-1]
        else:
            ema_fast[i] = alpha_fast * close[i] + (1 - alpha_fast) * ema_fast[i-1]
            ema_slow[i] = alpha_slow * close[i] + (1 - alpha_slow) * ema_slow[i-1]

    for i in range(n):
        macd_line[i] = ema_fast[i] - ema_slow[i]

    signal_line[0] = macd_line[0]
    for i in range(1, n):
        signal_line[i] = alpha_signal * macd_line[i] + (1 - alpha_signal) * signal_line[i-1]

    for i in range(n):
        histogram[i] = 2.0 * (macd_line[i] - signal_line[i])

    # 前slow-1个值设为NAN
    for i in range(slow - 1):
        macd_line[i] = NAN
        signal_line[i] = NAN
        histogram[i] = NAN

    return macd_line, signal_line, histogram


def compute_bollinger(np.ndarray[DTYPE_t, ndim=1] close, int period=20, double num_std=2.0):
    """计算布林带 (middle, upper, lower)"""
    cdef int n = close.shape[0]
    cdef np.ndarray[DTYPE_t, ndim=1] middle = compute_ma(close, period)
    cdef np.ndarray[DTYPE_t, ndim=1] upper = np.empty(n, dtype=np.float64)
    cdef np.ndarray[DTYPE_t, ndim=1] lower = np.empty(n, dtype=np.float64)
    cdef double std_val, total, mean_val
    cdef int i, j

    for i in range(n):
        if isnan(middle[i]) or i < period - 1:
            upper[i] = NAN
            lower[i] = NAN
        else:
            total = 0.0
            mean_val = middle[i]
            for j in range(i - period + 1, i + 1):
                total += (close[j] - mean_val) * (close[j] - mean_val)
            std_val = sqrt(total / period)
            upper[i] = mean_val + num_std * std_val
            lower[i] = mean_val - num_std * std_val

    return middle, upper, lower


def compute_kdj(np.ndarray[DTYPE_t, ndim=1] high,
                np.ndarray[DTYPE_t, ndim=1] low,
                np.ndarray[DTYPE_t, ndim=1] close,
                int period=9, int k_smooth=3, int d_smooth=3):
    """计算KDJ指标"""
    cdef int n = close.shape[0]
    cdef np.ndarray[DTYPE_t, ndim=1] k_values = np.empty(n, dtype=np.float64)
    cdef np.ndarray[DTYPE_t, ndim=1] d_values = np.empty(n, dtype=np.float64)
    cdef np.ndarray[DTYPE_t, ndim=1] j_values = np.empty(n, dtype=np.float64)
    cdef double highest, lowest, rsv
    cdef int i, j

    for i in range(n):
        if i < period - 1:
            k_values[i] = NAN
            d_values[i] = NAN
            j_values[i] = NAN
            continue

        highest = high[i]
        lowest = low[i]
        for j in range(i - period + 1, i + 1):
            if high[j] > highest:
                highest = high[j]
            if low[j] < lowest:
                lowest = low[j]

        if highest == lowest:
            rsv = 50.0
        else:
            rsv = (close[i] - lowest) / (highest - lowest) * 100.0

        if i == period - 1:
            k_values[i] = rsv
            d_values[i] = rsv
        else:
            k_values[i] = (2.0 / 3.0) * k_values[i-1] + (1.0 / 3.0) * rsv
            d_values[i] = (2.0 / 3.0) * d_values[i-1] + (1.0 / 3.0) * k_values[i]

        j_values[i] = 3.0 * k_values[i] - 2.0 * d_values[i]

    return k_values, d_values, j_values


def compute_volatility(np.ndarray[DTYPE_t, ndim=1] close, int period=20):
    """计算波动率 (日收益率标准差 * sqrt(252))"""
    cdef int n = close.shape[0]
    cdef np.ndarray[DTYPE_t, ndim=1] result = np.empty(n, dtype=np.float64)
    cdef np.ndarray[DTYPE_t, ndim=1] returns = np.empty(n, dtype=np.float64)
    cdef double total, mean_val, var_sum
    cdef int i, j

    returns[0] = NAN
    for i in range(1, n):
        if isnan(close[i]) or isnan(close[i-1]) or close[i-1] == 0:
            returns[i] = NAN
        else:
            returns[i] = (close[i] - close[i-1]) / close[i-1]

    for i in range(n):
        if i < period:
            result[i] = NAN
        else:
            total = 0.0
            for j in range(i - period + 1, i + 1):
                if isnan(returns[j]):
                    result[i] = NAN
                    break
                total += returns[j]
            else:
                mean_val = total / period
                var_sum = 0.0
                for j in range(i - period + 1, i + 1):
                    var_sum += (returns[j] - mean_val) * (returns[j] - mean_val)
                result[i] = sqrt(var_sum / period) * sqrt(252.0)

    return result


def compute_williams_r(np.ndarray[DTYPE_t, ndim=1] high,
                       np.ndarray[DTYPE_t, ndim=1] low,
                       np.ndarray[DTYPE_t, ndim=1] close,
                       int period=14):
    """计算威廉指标 %R"""
    cdef int n = close.shape[0]
    cdef np.ndarray[DTYPE_t, ndim=1] result = np.empty(n, dtype=np.float64)
    cdef double highest, lowest
    cdef int i, j

    for i in range(n):
        if i < period - 1:
            result[i] = NAN
            continue

        highest = high[i]
        lowest = low[i]
        for j in range(i - period + 1, i + 1):
            if high[j] > highest:
                highest = high[j]
            if low[j] < lowest:
                lowest = low[j]

        if highest == lowest:
            result[i] = -50.0
        else:
            result[i] = (highest - close[i]) / (highest - lowest) * -100.0

    return result


def compute_all_indicators(df_dict):
    """计算所有技术指标，输入为包含OHLCV的dict，返回指标dict"""
    close = np.asarray(df_dict["close"], dtype=np.float64)
    high = np.asarray(df_dict["high"], dtype=np.float64)
    low = np.asarray(df_dict["low"], dtype=np.float64)
    vol = np.asarray(df_dict["vol"], dtype=np.float64)

    result = {}

    # 涨跌额/幅
    result["change"] = compute_change(close)
    result["pct_chg"] = compute_pct_chg(close)

    # 成交额估算 (vol * (open + close) / 2 / 100)
    open_arr = np.asarray(df_dict["open"], dtype=np.float64)
    result["amount"] = vol * (open_arr + close) / 2.0

    # RSI
    if len(close) > 6:
        result["rsi_6"] = compute_rsi(close, 6)
    if len(close) > 12:
        result["rsi_12"] = compute_rsi(close, 12)
    if len(close) > 24:
        result["rsi_24"] = compute_rsi(close, 24)

    # MA
    if len(close) > 5:
        result["ma_5"] = compute_ma(close, 5)
    if len(close) > 20:
        result["ma_20"] = compute_ma(close, 20)
    if len(close) > 60:
        result["ma_60"] = compute_ma(close, 60)

    # MACD
    if len(close) > 26:
        macd, signal, hist = compute_macd(close)
        result["macd"] = macd
        result["macd_signal"] = signal
        result["macd_histogram"] = hist

    # Bollinger
    if len(close) > 20:
        boll_m, boll_u, boll_l = compute_bollinger(close)
        result["boll_middle"] = boll_m
        result["boll_upper"] = boll_u
        result["boll_lower"] = boll_l

    # KDJ
    if len(close) > 9:
        k, d, j = compute_kdj(high, low, close)
        result["kdj_k"] = k
        result["kdj_d"] = d
        result["kdj_j"] = j

    # 波动率
    if len(close) > 20:
        result["volatility"] = compute_volatility(close)

    # Volume MA
    if len(vol) > 5:
        result["volume_ma_5"] = compute_ma(vol, 5)
    if len(vol) > 20:
        result["volume_ma_20"] = compute_ma(vol, 20)

    # 量比
    if len(vol) > 5:
        vma5 = compute_ma(vol, 5)
        n = len(vol)
        vr = np.empty(n, dtype=np.float64)
        for i in range(n):
            if isnan(vma5[i]) or vma5[i] == 0:
                vr[i] = NAN
            else:
                vr[i] = vol[i] / vma5[i]
        result["volume_ratio"] = vr

    # Williams %R
    if len(close) > 14:
        result["wr_14"] = compute_williams_r(high, low, close)

    return result
