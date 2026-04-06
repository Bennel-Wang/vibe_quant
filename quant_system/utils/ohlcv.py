"""
OHLCV 数据工具函数
统一处理 K 线重采样，消除 web_app.py / backtest.py 中的重复代码
"""

import pandas as pd


def resample_ohlcv(daily_df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """
    将日线 OHLCV 重采样为指定频率。

    Args:
        daily_df: 日线 DataFrame，必须包含 date 列（datetime 类型）及 open/high/low/close/volume
        rule    : pandas resample 规则字符串
                  - 'W-FRI'  → 周线（以周五收盘为周末，对齐 A 股交易周）
                  - 'ME'     → 月线（pandas 2.2+）；自动回退到 'M'
                  - 'QE'     → 季线
                  - 'YE'     → 年线

    Returns:
        重采样后的 DataFrame，index 重置、date 列为 datetime
    """
    if daily_df is None or daily_df.empty:
        return pd.DataFrame()

    df = daily_df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date').sort_index()

    agg: dict = {}
    for col, func in [('open', 'first'), ('high', 'max'), ('low', 'min'),
                      ('close', 'last'), ('volume', 'sum'),
                      ('amount', 'sum'), ('pct_chg', 'sum')]:
        if col in df.columns:
            agg[col] = func

    # pandas 2.2+ 用 'ME'/'QE'/'YE'，旧版用 'M'/'Q'/'A'
    fallback_map = {'ME': 'M', 'QE': 'Q', 'YE': 'A'}
    try:
        resampled = df.resample(rule, label='right', closed='right').agg(agg)
    except ValueError:
        resampled = df.resample(fallback_map.get(rule, rule),
                                label='right', closed='right').agg(agg)

    resampled = resampled.dropna(how='all').reset_index()
    resampled.rename(columns={'index': 'date'}, inplace=True)
    return resampled


def resample_to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """日线 → 周线（W-FRI）"""
    return resample_ohlcv(df, 'W-FRI')


def resample_to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """日线 → 月线（月末）"""
    return resample_ohlcv(df, 'ME')
