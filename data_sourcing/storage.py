"""
CSV存储与增量更新模块
"""
import os
import pandas as pd
import numpy as np
from config import DATA_DIR, BASE_COLUMNS, FULL_COLUMNS, setup_logger
from validator import validate_dataframe

logger = setup_logger("storage", "storage.log")


def get_csv_path(code: str, freq: str = "day") -> str:
    """获取CSV文件路径"""
    safe_code = code.replace(".", "_")
    return str(DATA_DIR / f"{safe_code}_{freq}.csv")


def load_existing_data(code: str, freq: str = "day") -> pd.DataFrame:
    """加载本地已有数据"""
    path = get_csv_path(code, freq)
    if os.path.exists(path):
        try:
            df = pd.read_csv(path, dtype={"trade_date": str})
            if not df.empty:
                df["trade_date"] = df["trade_date"].astype(str).str.replace("-", "")
                logger.info(f"加载本地数据 {code}: {len(df)} 行, 日期范围 {df['trade_date'].min()}-{df['trade_date'].max()}")
                return df
        except Exception as e:
            logger.warning(f"加载本地数据失败 {code}: {e}")
    return pd.DataFrame(columns=BASE_COLUMNS)


def get_latest_valid_date(code: str, freq: str = "day") -> str:
    """获取本地数据的最新有效日期"""
    df = load_existing_data(code, freq)
    if df.empty:
        return None
    valid_mask, _ = validate_dataframe(df)
    valid_df = df[valid_mask]
    if valid_df.empty:
        return None
    return str(valid_df["trade_date"].max())


def merge_and_save(code: str, new_data: pd.DataFrame, freq: str = "day") -> pd.DataFrame:
    """增量合并并保存数据
    - 本地已有有效数据保留
    - 新数据覆盖无效数据或追加新日期数据
    """
    if new_data is None or new_data.empty:
        logger.warning(f"无新数据可合并 {code}")
        return load_existing_data(code, freq)

    new_data = new_data.copy()
    new_data["trade_date"] = new_data["trade_date"].astype(str).str.replace("-", "")

    existing = load_existing_data(code, freq)

    if existing.empty:
        result = new_data
    else:
        # 以trade_date为key进行合并
        existing_valid_mask, _ = validate_dataframe(existing)
        valid_existing = existing[existing_valid_mask]
        invalid_existing = existing[~existing_valid_mask]

        # 新数据覆盖无效数据 + 追加新日期
        valid_dates = set(valid_existing["trade_date"].values)
        # 只保留新数据中不在有效集中的日期或覆盖无效日期
        new_for_merge = new_data[~new_data["trade_date"].isin(valid_dates)]

        result = pd.concat([valid_existing, new_for_merge], ignore_index=True)

    # 去重并排序
    result = result.drop_duplicates(subset=["trade_date"], keep="last")
    result = result.sort_values("trade_date").reset_index(drop=True)

    # 确保列顺序
    for col in BASE_COLUMNS:
        if col not in result.columns:
            result[col] = np.nan

    # 保存
    path = get_csv_path(code, freq)
    result.to_csv(path, index=False)
    logger.info(f"保存数据 {code}: {len(result)} 行 -> {path}")

    return result


def save_with_indicators(code: str, df: pd.DataFrame, freq: str = "day"):
    """保存含技术指标的完整数据"""
    if df is None or df.empty:
        return

    path = get_csv_path(code, freq)

    # 确保所有列存在
    for col in FULL_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    # 按FULL_COLUMNS排序列，额外列放最后
    ordered_cols = [c for c in FULL_COLUMNS if c in df.columns]
    extra_cols = [c for c in df.columns if c not in FULL_COLUMNS]
    df = df[ordered_cols + extra_cols]

    df = df.sort_values("trade_date").reset_index(drop=True)
    df.to_csv(path, index=False)
    logger.info(f"保存完整数据(含指标) {code}: {len(df)} 行 -> {path}")
