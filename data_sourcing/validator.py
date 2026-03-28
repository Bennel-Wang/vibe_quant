"""
数据校验模块 - 检查数据有效性
"""
import pandas as pd
import numpy as np


def is_valid_value(val) -> bool:
    """检查单个值是否有效 (非空、非NaN、非0)"""
    if val is None:
        return False
    if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
        return False
    if val == 0 or val == "":
        return False
    return True


def validate_ohlcv_row(row: pd.Series) -> bool:
    """校验单行OHLCV数据是否有效"""
    required = ["open", "high", "low", "close", "vol"]
    for col in required:
        if col not in row.index:
            return False
        if not is_valid_value(row[col]):
            return False
    # 逻辑校验: high >= low, high >= open, high >= close
    if row["high"] < row["low"]:
        return False
    return True


def validate_dataframe(df: pd.DataFrame) -> tuple:
    """校验DataFrame，返回 (有效行mask, 无效行mask)
    Returns:
        (valid_mask, invalid_mask): 布尔Series
    """
    if df is None or df.empty:
        return pd.Series(dtype=bool), pd.Series(dtype=bool)

    required = ["open", "high", "low", "close", "vol"]
    valid_mask = pd.Series(True, index=df.index)

    for col in required:
        if col not in df.columns:
            valid_mask[:] = False
            break
        col_data = pd.to_numeric(df[col], errors="coerce")
        valid_mask &= col_data.notna()
        valid_mask &= col_data != 0
        valid_mask &= ~np.isinf(col_data)

    # high >= low 逻辑校验
    if "high" in df.columns and "low" in df.columns:
        h = pd.to_numeric(df["high"], errors="coerce")
        l = pd.to_numeric(df["low"], errors="coerce")
        valid_mask &= h >= l

    return valid_mask, ~valid_mask


def has_valid_data(df: pd.DataFrame) -> bool:
    """检查DataFrame是否包含有效数据"""
    if df is None or df.empty:
        return False
    valid_mask, _ = validate_dataframe(df)
    return valid_mask.any()


def get_invalid_dates(df: pd.DataFrame) -> list:
    """获取无效数据的日期列表"""
    if df is None or df.empty:
        return []
    _, invalid_mask = validate_dataframe(df)
    if "trade_date" in df.columns:
        return df.loc[invalid_mask, "trade_date"].tolist()
    return df.loc[invalid_mask].index.tolist()


def count_trading_days(start_date: str, end_date: str, market: str = "A") -> int:
    """计算两个日期之间的交易日数量（含两端）

    对于有明确节假日数据的年份精确计算，
    对于无节假日数据的年份按每年约15天假期估算。

    Args:
        start_date: 起始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD
        market: 市场 "A" / "HK"

    Returns:
        交易日数量
    """
    import datetime as _dt
    from trading_calendar import is_trading_day, _A_HOLIDAYS, _HK_HOLIDAYS

    d = _dt.datetime.strptime(start_date, "%Y%m%d").date()
    end = _dt.datetime.strptime(end_date, "%Y%m%d").date()
    holidays_db = _HK_HOLIDAYS if market.startswith("HK") else _A_HOLIDAYS
    # 每年估计的非周末假期天数（无数据年份使用）
    est_holidays_per_year = 13 if market.startswith("HK") else 15

    count = 0
    current_year = None
    year_has_calendar = False
    year_weekdays = 0
    year_est_deduct = 0

    while d <= end:
        if d.year != current_year:
            # 对前一个无日历年份扣除估计假期
            if current_year is not None and not year_has_calendar and year_weekdays > 0:
                count -= min(year_est_deduct, year_weekdays)

            current_year = d.year
            year_has_calendar = current_year in holidays_db
            year_weekdays = 0
            # 按该年在范围内的比例估算扣减天数
            year_start = max(d, _dt.date(current_year, 1, 1))
            year_end = min(end, _dt.date(current_year, 12, 31))
            fraction = (year_end - year_start).days / 365.0
            year_est_deduct = round(est_holidays_per_year * fraction) if not year_has_calendar else 0

        if d.weekday() < 5:  # 工作日
            if year_has_calendar:
                if is_trading_day(d, market):
                    count += 1
            else:
                count += 1
                year_weekdays += 1
        d += _dt.timedelta(days=1)

    # 扣除最后一个无日历年份的估计假期
    if current_year is not None and not year_has_calendar and year_weekdays > 0:
        count -= min(year_est_deduct, year_weekdays)

    return max(count, 0)


def validate_data_completeness(
    code: str, df: pd.DataFrame, start_date: str, market: str = "A",
    tolerance: float = 0.05, logger=None, list_date: str = None,
) -> dict:
    """校验数据行数与预期交易日数是否匹配

    Args:
        code: 股票代码（仅用于日志）
        df: 带 trade_date 列的 DataFrame
        start_date: 配置中的历史起始日期 YYYYMMDD
        market: "A" / "HK"
        tolerance: 允许的缺失比例 (0.05 = 5%)
        logger: 可选 logger
        list_date: 股票上市日期 YYYYMMDD（优先于数据最早日期，更准确）

    Returns:
        dict with keys: expected, actual, missing, missing_pct, ok, message
    """
    result = {"code": code, "expected": 0, "actual": 0,
              "missing": 0, "missing_pct": 0.0, "ok": True, "message": ""}

    if df is None or df.empty or "trade_date" not in df.columns:
        result["ok"] = False
        result["message"] = f"{code}: 无数据"
        if logger:
            logger.error(result["message"])
        return result

    actual = len(df)
    end_date = str(df["trade_date"].max())
    earliest = str(df["trade_date"].min())

    # 用上市日期（如有）、数据实际最早日期、配置起始日期三者取最晚
    # 上市日期比数据最早日期更准确（数据可能从上市后某天才开始记录）
    if list_date and len(list_date) == 8:
        effective_start = max(start_date, list_date, earliest)
    else:
        effective_start = max(start_date, earliest)

    expected = count_trading_days(effective_start, end_date, market)

    missing = expected - actual
    missing_pct = missing / expected if expected > 0 else 0.0

    result.update({
        "expected": expected,
        "actual": actual,
        "missing": missing,
        "missing_pct": round(missing_pct * 100, 2),
    })

    if missing_pct > tolerance:
        result["ok"] = False
        result["message"] = (
            f"{code}: 数据缺失过多 — 预期 {expected} 行, 实际 {actual} 行, "
            f"缺失 {missing} ({result['missing_pct']}%), 日期范围 {effective_start}-{end_date}"
        )
        if logger:
            logger.error(result["message"])
    elif missing > 0:
        result["message"] = (
            f"{code}: 数据略有缺失 — 预期 {expected}, 实际 {actual}, "
            f"缺失 {missing} ({result['missing_pct']}%)"
        )
        if logger:
            logger.info(result["message"])
    else:
        result["message"] = f"{code}: 数据完整 ({actual} 行)"
        if logger:
            logger.info(result["message"])

    return result
