"""
交易日历模块 - 支持A股和港股交易日判断

增加功能：生成并保存这几年（由内置节假日控制）A股和港股的完整交易日历到 data/trading_calendars.json
同时保持向后兼容：写入 data/trading_dates.json（只包含最新交易日 summary）
"""
import datetime
from functools import lru_cache
import os
import json


# A股固定节假日(月-日) - 元旦、春节、清明、劳动、端午、中秋、国庆
# 注意: 实际节假日每年不同，这里只做基础判断，详细日历需要从数据源获取
A_SHARE_HOLIDAYS_2024 = {
    (1, 1), (2, 9), (2, 10), (2, 11), (2, 12), (2, 13), (2, 14), (2, 15), (2, 16), (2, 17),
    (4, 4), (4, 5), (4, 6), (5, 1), (5, 2), (5, 3), (5, 4), (5, 5),
    (6, 8), (6, 9), (6, 10), (9, 15), (9, 16), (9, 17),
    (10, 1), (10, 2), (10, 3), (10, 4), (10, 5), (10, 6), (10, 7),
}

A_SHARE_HOLIDAYS_2025 = {
    (1, 1), (1, 28), (1, 29), (1, 30), (1, 31), (2, 1), (2, 2), (2, 3), (2, 4),
    (4, 4), (4, 5), (4, 6), (5, 1), (5, 2), (5, 3), (5, 4), (5, 5),
    (5, 31), (6, 1), (6, 2), (10, 1), (10, 2), (10, 3), (10, 4), (10, 5), (10, 6), (10, 7),
    (10, 6), (10, 7), (10, 8),
}

A_SHARE_HOLIDAYS_2026 = {
    (1, 1), (1, 2), (2, 16), (2, 17), (2, 18), (2, 19), (2, 20), (2, 21), (2, 22),
    (4, 5), (4, 6), (4, 7), (5, 1), (5, 2), (5, 3),
    (6, 19), (6, 20), (6, 21),
    (10, 1), (10, 2), (10, 3), (10, 4), (10, 5), (10, 6), (10, 7), (10, 8),
}

# 港股节假日较复杂，含圣诞、复活节、佛诞等
HK_HOLIDAYS_2024 = {
    (1, 1), (2, 10), (2, 11), (2, 12), (2, 13),
    (3, 29), (3, 30), (4, 1), (4, 4), (5, 1), (5, 15),
    (6, 10), (7, 1), (9, 18), (10, 1), (10, 11),
    (12, 25), (12, 26),
}

HK_HOLIDAYS_2025 = {
    (1, 1), (1, 29), (1, 30), (1, 31),
    (4, 4), (4, 18), (4, 19), (4, 21), (5, 1), (5, 5),
    (7, 1), (10, 1), (10, 7), (10, 29),
    (12, 25), (12, 26),
}

HK_HOLIDAYS_2026 = {
    (1, 1), (2, 17), (2, 18), (2, 19),
    (4, 3), (4, 5), (4, 6), (4, 7), (5, 1), (5, 24),
    (6, 19), (7, 1), (10, 1), (10, 19),
    (12, 25), (12, 26),
}

_A_HOLIDAYS = {2024: A_SHARE_HOLIDAYS_2024, 2025: A_SHARE_HOLIDAYS_2025, 2026: A_SHARE_HOLIDAYS_2026}
_HK_HOLIDAYS = {2024: HK_HOLIDAYS_2024, 2025: HK_HOLIDAYS_2025, 2026: HK_HOLIDAYS_2026}


def is_a_share_trading_day(date: datetime.date) -> bool:
    """判断是否为A股交易日"""
    if date.weekday() >= 5:  # 周六日
        return False
    year_holidays = _A_HOLIDAYS.get(date.year, set())
    return (date.month, date.day) not in year_holidays


def is_hk_trading_day(date: datetime.date) -> bool:
    """判断是否为港股交易日"""
    if date.weekday() >= 5:
        return False
    year_holidays = _HK_HOLIDAYS.get(date.year, set())
    return (date.month, date.day) not in year_holidays


def is_trading_day(date: datetime.date, market: str) -> bool:
    """通用交易日判断"""
    if market.startswith("HK"):
        return is_hk_trading_day(date)
    return is_a_share_trading_day(date)


def get_last_trading_day(date: datetime.date, market: str) -> datetime.date:
    """获取指定日期之前的最近交易日（包含当天）"""
    while not is_trading_day(date, market):
        date -= datetime.timedelta(days=1)
    return date


def get_prev_trading_day(date: datetime.date, market: str) -> datetime.date:
    """获取上一个交易日（不含当天）"""
    date -= datetime.timedelta(days=1)
    return get_last_trading_day(date, market)


def is_trading_hours(market: str) -> bool:
    """判断当前是否在交易时段内"""
    now = datetime.datetime.now()
    if market.startswith("HK"):
        # 港股: 9:30-12:00, 13:00-16:00
        morning = datetime.time(9, 30) <= now.time() <= datetime.time(12, 0)
        afternoon = datetime.time(13, 0) <= now.time() <= datetime.time(16, 0)
        return morning or afternoon
    else:
        # A股: 9:30-11:30, 13:00-15:00
        morning = datetime.time(9, 30) <= now.time() <= datetime.time(11, 30)
        afternoon = datetime.time(13, 0) <= now.time() <= datetime.time(15, 0)
        return morning or afternoon


def today_is_trading_day(market: str) -> bool:
    """判断今天是否为交易日"""
    return is_trading_day(datetime.date.today(), market)


def update_trading_dates(output_dir: str = None) -> dict:
    """生成并保存指定年份范围内的交易日历到 data/trading_calendars.json，并写入最新日期 summary 到 data/trading_dates.json

    返回: {'cn': 'YYYY-MM-DD', 'hk': 'YYYY-MM-DD'}
    """
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    data_dir = output_dir or os.path.join(repo_root, 'data')
    os.makedirs(data_dir, exist_ok=True)

    years = sorted(set(list(_A_HOLIDAYS.keys()) + list(_HK_HOLIDAYS.keys())))

    calendars = {'cn': {}, 'hk': {}}
    from datetime import date, timedelta

    for y in years:
        start = date(y, 1, 1)
        end = date(y, 12, 31)
        d = start
        cn_days = []
        hk_days = []
        while d <= end:
            if is_a_share_trading_day(d):
                cn_days.append(d.strftime('%Y-%m-%d'))
            if is_hk_trading_day(d):
                hk_days.append(d.strftime('%Y-%m-%d'))
            d = d + timedelta(days=1)
        calendars['cn'][str(y)] = cn_days
        calendars['hk'][str(y)] = hk_days

    # 保存完整日历
    cal_path = os.path.join(data_dir, 'trading_calendars.json')
    try:
        with open(cal_path, 'w', encoding='utf-8') as f:
            json.dump(calendars, f, ensure_ascii=False, indent=2)
    except Exception as e:
        # 不要抛出异常给调用者，记录日志
        try:
            import logging
            logging.getLogger(__name__).warning(f"保存交易日历失败: {e}")
        except Exception:
            pass

    # 生成 summary（最新交易日）
    last_cn = ''
    last_hk = ''
    try:
        all_cn = [d for year_list in calendars['cn'].values() for d in year_list]
        if all_cn:
            last_cn = max(all_cn)
        all_hk = [d for year_list in calendars['hk'].values() for d in year_list]
        if all_hk:
            last_hk = max(all_hk)
    except Exception:
        pass

    summary = {'cn': last_cn, 'hk': last_hk}
    summary_path = os.path.join(data_dir, 'trading_dates.json')
    try:
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    return summary


def get_trading_dates() -> dict:
    """读取已生成的交易日历（优先返回完整日历），如果不存在则返回空结构或 summary"""
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    cal_path = os.path.join(repo_root, 'data', 'trading_calendars.json')
    if os.path.exists(cal_path):
        try:
            with open(cal_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    # fallback summary
    summary_path = os.path.join(repo_root, 'data', 'trading_dates.json')
    if os.path.exists(summary_path):
        try:
            with open(summary_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {'cn': {}, 'hk': {}}
