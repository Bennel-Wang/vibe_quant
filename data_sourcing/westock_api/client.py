"""westock CLI 客户端：子进程调用 + 输出解析（带 TTL 结果缓存 + 分级限速）。

实测限流条件（2026-11-06 受控探测）：
- 轻查询（search/短区间 kline/finance/profile 等）几乎不限流（1s 间隔持续跑全过）；
- 重查询（全历史 kline，返回数千行）：滑动窗口 ~60s 内只允许 2 次，超了返回
  "请求过于频繁"，窗口滑过后自动恢复；触发因素是**返回数据量**而非 --limit 参数；
- 两类配额独立（重查询被限期间轻查询正常）。
据此：重查询走跨进程滑动窗口限速（2 次/62s，阻塞排队），轻查询仅保留最小间隔。
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
from datetime import datetime, timedelta

from .parser import parse_output, strip_ansi

DEFAULT_TIMEOUT = 30

# 同参数查询的 TTL 缓存：k 线类数据短周期内不变，避免重复子进程+网络开销
CACHE_TTL = float(os.environ.get("WESTOCK_CACHE_TTL", "120"))
_cache: dict = {}  # tuple(args) -> (expires_at, envelope)
_cache_lock = threading.Lock()

# ── 分级限速 ────────────────────────────────────────────────────────────────
# 重查询（返回数千行的 kline）：滑动窗口 _HEAVY_WINDOW 内最多 _HEAVY_CAPACITY 次。
# 后端 ~2 次/62s 为全 IP 总配额（含其它进程），默认取 1 次/62s 留余量保证零限流。
_HEAVY_CAPACITY = int(os.environ.get("WESTOCK_HEAVY_CAPACITY", "1"))
_HEAVY_WINDOW = float(os.environ.get("WESTOCK_HEAVY_WINDOW", "62"))  # 实测恢复窗 ~60s
_STATE_PATH = os.environ.get("WESTOCK_RATE_STATE") or os.path.join(
    tempfile.gettempdir(), "westock_rate_state.json")

_CALL_LOCK = threading.Lock()
MIN_CALL_INTERVAL = 0.2  # 轻查询最小间隔（实测几乎不限流，仅防止子进程堆积）
_last_call_ts = 0.0


def _is_heavy(args: list) -> bool:
    """返回数据量大的 kline 查询 → 重查询。

    实测限流按**返回行数**计（短区间+大 --limit 不触发），故按日期跨度判定；
    无日期的 limit 模式按 --limit 大小保守判定。
    """
    if not args or args[0] != "kline":
        return False
    start = end = None
    limit = 0
    it = iter(args[1:])
    for a in it:
        if a == "--start":
            try:
                start = datetime.strptime(next(it), "%Y-%m-%d")
            except (StopIteration, ValueError):
                pass
        elif a == "--end":
            try:
                end = datetime.strptime(next(it), "%Y-%m-%d")
            except (StopIteration, ValueError):
                pass
        elif a == "--limit":
            try:
                limit = int(next(it))
            except (StopIteration, ValueError):
                pass
    if start is not None and end is not None:
        return (end - start).days > 120
    return limit > 300


def _read_stamps() -> list:
    try:
        with open(_STATE_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return [float(t) for t in data.get("heavy_stamps", [])]
    except Exception:
        return []


def _write_stamps(stamps: list) -> None:
    try:
        tmp = _STATE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"heavy_stamps": stamps[-_HEAVY_CAPACITY:]}, f)
        os.replace(tmp, _STATE_PATH)
    except Exception:
        pass


def _heavy_acquire() -> None:
    """阻塞到重查询配额可用（跨进程共享的滑动窗口）。"""
    while True:
        now = time.time()
        stamps = [t for t in _read_stamps() if now - t < _HEAVY_WINDOW]
        if len(stamps) < _HEAVY_CAPACITY:
            stamps.append(now)
            _write_stamps(stamps)
            return
        wait = _HEAVY_WINDOW - (now - stamps[0])
        time.sleep(min(max(wait, 0.5), _HEAVY_WINDOW))


def _heavy_penalize() -> None:
    """被后端限流时罚时：把窗口填满，让后续重查询等满一个恢复窗。"""
    now = time.time()
    stamps = [t for t in _read_stamps() if now - t < _HEAVY_WINDOW]
    stamps.extend([now] * _HEAVY_CAPACITY)
    _write_stamps(stamps)


def cache_clear():
    with _cache_lock:
        _cache.clear()


def _cache_get(key):
    if CACHE_TTL <= 0:
        return None
    with _cache_lock:
        entry = _cache.get(key)
    if entry is None:
        return None
    expires_at, envelope = entry
    if time.time() > expires_at:
        return None
    return envelope


def _cache_set(key, envelope):
    if CACHE_TTL <= 0:
        return
    with _cache_lock:
        _cache[key] = (time.time() + CACHE_TTL, envelope)


def find_westock() -> str | None:
    env = os.environ.get("WESTOCK_BIN")
    if env and os.path.isfile(env):
        return env
    exe = shutil.which("westock")
    if exe:
        return exe
    for p in (
        os.path.expanduser("~/.local/bin/westock"),
        os.path.expanduser("~/.local/bin/westock.exe"),
    ):
        if os.path.isfile(p):
            return p
    return None


def run_westock(args: list[str], timeout: float = DEFAULT_TIMEOUT) -> dict:
    """执行 `westock <args...>`，返回统一 envelope。

    {"ok", "args", "exit_code", "elapsed_ms", "data": {"tables", "text"}, "error"}
    成功结果按参数缓存 CACHE_TTL 秒（失败不缓存）。
    """
    if not args or not all(isinstance(a, str) and a.strip() for a in args):
        raise ValueError("args 必须是非空的字符串列表")

    key = tuple(args)
    cached = _cache_get(key)
    if cached is not None:
        return {**cached, "cached": True, "elapsed_ms": 0}

    exe = find_westock()
    if not exe:
        raise FileNotFoundError(
            "westock CLI 未安装（设置 WESTOCK_BIN 环境变量或运行 skill 的 scripts/setup.cjs）"
        )

    t0 = time.time()
    if _is_heavy(args):
        # 重查询先取跨进程配额（阻塞排队，窗口 2 次/62s）
        _heavy_acquire()
    try:
        global _last_call_ts
        with _CALL_LOCK:
            wait = MIN_CALL_INTERVAL - (time.time() - _last_call_ts)
            if wait > 0:
                time.sleep(wait)
            proc = subprocess.run(
                [exe, *args],
                capture_output=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout,
                shell=False,
            )
            _last_call_ts = time.time()
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "args": args,
            "exit_code": None,
            "elapsed_ms": int((time.time() - t0) * 1000),
            "data": {"tables": [], "text": None},
            "error": f"westock 执行超时（{timeout}s）",
        }

    out = strip_ansi(proc.stdout or "")
    err = strip_ansi(proc.stderr or "")
    data = parse_output(out if out.strip() else err)

    error = None
    all_text = (data["text"] or "") + "\n" + err
    if "请求过于频繁" in all_text or "稍后再试" in all_text:
        # 后端频控：非业务空数据，标记为限流错误（不缓存）并罚时一个恢复窗
        error = "westock 限流（请求过于频繁）"
        if _is_heavy(args):
            _heavy_penalize()
    elif proc.returncode != 0:
        first = next((l.strip() for l in err.splitlines() if l.strip()), "")
        error = first or f"westock 退出码 {proc.returncode}"
    elif not data["tables"] and not data["text"]:
        error = "westock 无输出"
    elif not data["tables"] and any(k in (data["text"] or "") for k in ("失败", "错误", "error")):
        # 业务失败时 westock 退出码仍为 0，仅以文本提示
        error = data["text"].splitlines()[0]

    envelope = {
        "ok": proc.returncode == 0 and error is None,
        "args": args,
        "exit_code": proc.returncode,
        "elapsed_ms": int((time.time() - t0) * 1000),
        "data": data,
        "error": error,
        "cached": False,
    }
    if envelope["ok"]:
        _cache_set(key, envelope)
    return envelope
