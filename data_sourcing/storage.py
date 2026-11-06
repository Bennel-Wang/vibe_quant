"""
CSV存储与增量更新模块
"""
import os
import threading
from typing import Optional

try:
    import msvcrt  # Windows 跨进程文件锁
except ImportError:  # pragma: no cover - POSIX
    msvcrt = None
    import fcntl

import pandas as pd
import numpy as np
from config import DATA_DIR, BASE_COLUMNS, FULL_COLUMNS, setup_logger
from validator import validate_dataframe

logger = setup_logger("storage", "storage.log")

# ── 每文件读-改-写锁（线程 + 跨进程）+ 原子写 ─────────────────────────────────
# to_csv 非原子，并发读-改-写（实时轮询 vs 评分刷新 vs 外部脚本）会读到半截
# 文件 → 读失败返回空 → 被 1 行今日数据覆盖（历史截断事故根因，2026-11-06）。
_file_locks: dict = {}
_file_locks_guard = threading.Lock()


class _FileGuard:
    """同进程 RLock + 跨进程文件区域锁（每次调用独立句柄，可嵌套）"""

    def __init__(self, path: str, thread_lock: threading.RLock):
        self._thread_lock = thread_lock
        self._path = path
        self._fh = None

    def __enter__(self):
        self._thread_lock.acquire()
        self._fh = open(self._path + ".lock", "a+")
        try:
            if msvcrt is not None:
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_LOCK, 1)
            else:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        except OSError:
            pass  # 锁失败不阻塞写入（可用性优先，原子写仍防撕裂）
        return self

    def __exit__(self, *exc):
        try:
            if msvcrt is not None:
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        self._fh.close()
        self._thread_lock.release()
        return False


def _lock_for(path: str) -> _FileGuard:
    with _file_locks_guard:
        thread_lock = _file_locks.get(path)
        if thread_lock is None:
            thread_lock = threading.RLock()
            _file_locks[path] = thread_lock
    return _FileGuard(path, thread_lock)


def _atomic_to_csv(df: pd.DataFrame, path: str) -> None:
    """先写临时文件再原子替换，杜绝读到半截 CSV"""
    tmp = path + ".tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)


def _disk_row_count(path: str) -> int:
    try:
        with open(path, encoding="utf-8") as f:
            return max(sum(1 for _ in f) - 1, 0)
    except OSError:
        return 0


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
                # 过滤 trade_date 不是合法 YYYYMMDD 的损坏行
                before = len(df)
                valid_date_mask = df["trade_date"].str.match(r'^\d{8}$')
                df = df[valid_date_mask]
                removed = before - len(df)
                if removed > 0:
                    logger.warning(f"加载 {code} 时过滤掉 {removed} 行损坏数据（trade_date 非法）")
                if not df.empty:
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
    with _lock_for(path):
        _atomic_to_csv(result, path)
    logger.info(f"保存数据 {code}: {len(result)} 行 -> {path}")

    return result


def update_today_row(code: str, freq: str, new_row: dict) -> bool:
    """把今日行情行合并进 CSV（读-改-写全程持锁 + 原子写 + 防缩水守卫）。

    防缩水守卫：读到 0 行但磁盘文件明显有历史数据时，判定为并发读写瞬时失败，
    拒绝用 1 行覆盖（2026-11-06 全量截断事故的直接防线）。
    """
    path = get_csv_path(code, freq)
    today = str(new_row.get("trade_date", ""))
    with _lock_for(path):
        df = load_existing_data(code, freq)
        disk_rows = _disk_row_count(path) if os.path.exists(path) else 0
        if df.empty and disk_rows > 1:
            logger.warning(
                f"跳过实时合并 {code}: 读到 0 行但磁盘有 {disk_rows} 行"
                f"（疑似并发读写冲突，拒绝覆盖）"
            )
            return False

        if df.empty:
            df = pd.DataFrame([new_row])
        else:
            df = df.copy()
            mask = df["trade_date"].astype(str) == today
            if mask.any():
                for col, val in new_row.items():
                    if col in df.columns:
                        df.loc[mask, col] = val
                    else:
                        df[col] = np.nan
                        df.loc[mask, col] = val
            else:
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        _atomic_to_csv(df, path)
    return True


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
    with _lock_for(path):
        _atomic_to_csv(df, path)
    logger.info(f"保存完整数据(含指标) {code}: {len(df)} 行 -> {path}")
