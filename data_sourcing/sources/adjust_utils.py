"""
A股前复权工具 - 基于新浪财经复权因子
适用于 pytdx、mootdx 等不内置前复权的数据源

新浪复权因子数据格式 (qfq.js):
  - 每条记录对应一个除权除息日，代表该日起新的复权因子生效
  - 最新一条因子 = 1.0 (当前价格为基准，不做调整)
  - 向历史方向因子逐渐增大 (> 1.0)
  - 前复权公式: qfq_price = raw_price / f_factor
  - 仅适用于 A股个股 (沪深 .SH/.SZ)，A股指数/港股不需要复权
"""
import io
import json
import logging
import pandas as pd
import requests
from typing import Optional

logger = logging.getLogger(__name__)

_SINA_QFQ_URL = 'https://finance.sina.com.cn/realstock/company/{}/qfq.js'


def _to_sina_code(unified_code: str) -> Optional[str]:
    """统一代码 → 新浪代码 (000001.SZ → sz000001)"""
    if '.' not in unified_code:
        return None
    symbol, suffix = unified_code.rsplit('.', 1)
    if suffix == 'SH':
        return f'sh{symbol}'
    if suffix == 'SZ':
        return f'sz{symbol}'
    return None


def get_sina_qfq_factor(unified_code: str) -> pd.DataFrame:
    """
    从新浪财经获取前复权因子表

    Returns: DataFrame[date, f_factor]，按日期升序排列
      date: datetime，除权除息日期
      f_factor: float，该日期之后到下一除权日之前的复权因子
                最新期间 = 1.0，越旧越大
    """
    sina_code = _to_sina_code(unified_code)
    if not sina_code:
        return pd.DataFrame()

    url = _SINA_QFQ_URL.format(sina_code)
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        resp = requests.get(url, timeout=10, verify=False)
        resp.raise_for_status()

        raw_text = resp.text.split('=', 1)
        if len(raw_text) < 2:
            return pd.DataFrame()

        json_text = raw_text[1].strip()
        # 去掉末尾 JS 注释（/* ... */）
        for marker in ['\n/*', '\n//']:
            if marker in json_text:
                json_text = json_text[:json_text.index(marker)]
        json_text = json_text.rstrip(';').strip()

        data = json.loads(json_text).get('data', [])
        if not data:
            return pd.DataFrame()

        # Sina data format: [{"d": "2025-10-15", "f": "1.0000..."}, ...]
        df = pd.DataFrame(data)
        df = df.rename(columns={'d': 'date', 'f': 'f_factor'})
        df['date'] = pd.to_datetime(df['date'])
        df['f_factor'] = pd.to_numeric(df['f_factor'], errors='coerce')
        return df.dropna().sort_values('date').reset_index(drop=True)

    except Exception as e:
        logger.warning(f"新浪前复权因子获取失败 ({unified_code}): {e}")
        return pd.DataFrame()


def apply_qfq(df: pd.DataFrame, unified_code: str) -> pd.DataFrame:
    """
    对A股个股价格数据应用前复权

    Args:
        df:            含 trade_date(YYYYMMDD), open/high/low/close 列的 DataFrame
        unified_code:  统一代码 (如 000001.SZ)

    Returns:
        前复权后的 DataFrame；若获取复权因子失败则返回原始数据并打印警告

    算法:
        对每个交易日，用 merge_asof(direction='backward') 找到最近的因子记录，
        然后 qfq_price = raw_price / f_factor
        - 早于最早记录的历史日期：使用最大因子（最旧的因子值）
        - A股指数 (.SH/.SZ 的指数代码)：通常无复权记录，sina 返回空数据，直接返回原始数据
    """
    if '.' not in unified_code:
        return df
    suffix = unified_code.rsplit('.', 1)[-1]
    if suffix not in ('SH', 'SZ'):
        return df  # 只处理沪深A股，港股/其他不适用

    factor_df = get_sina_qfq_factor(unified_code)
    if factor_df.empty:
        # 指数/ETF 等无复权记录时静默返回原始数据
        return df

    df = df.copy()
    df['_dt'] = pd.to_datetime(df['trade_date'].astype(str), format='%Y%m%d', errors='coerce')

    # merge_asof direction='backward': 每个交易日取最近一个 ≤ 该日的因子记录
    # 这样因子从其生效日期开始一直沿用到下一个除权日
    merged = pd.merge_asof(
        df.sort_values('_dt'),
        factor_df.rename(columns={'date': '_fdate', 'f_factor': '_factor'}),
        left_on='_dt',
        right_on='_fdate',
        direction='backward'
    )

    # 早于最早因子记录的日期 → 使用最旧的因子（因子最大值）
    oldest_factor = float(factor_df['f_factor'].max())
    merged['_factor'] = merged['_factor'].fillna(oldest_factor)

    # 前复权: qfq = raw / f_factor (f_factor ≥ 1, 越旧越大)
    for col in ['open', 'high', 'low', 'close']:
        if col in merged.columns:
            merged[col] = (merged[col] / merged['_factor']).round(3)

    merged = merged.drop(columns=['_dt', '_fdate', '_factor'], errors='ignore')
    return merged.sort_values('trade_date').reset_index(drop=True)
