"""westock CLI 客户端：子进程调用 + 输出解析（带 TTL 结果缓存）。"""
from __future__ import annotations

import os
import shutil
import subprocess
import threading
import time

from .parser import parse_output, strip_ansi

DEFAULT_TIMEOUT = 30

# 同参数查询的 TTL 缓存：k 线类数据短周期内不变，避免重复子进程+网络开销
CACHE_TTL = float(os.environ.get("WESTOCK_CACHE_TTL", "120"))
_cache: dict = {}  # tuple(args) -> (expires_at, envelope)
_cache_lock = threading.Lock()

# 全局调用节流：westock 后端有突发频控（"请求过于频繁"），并发打会自己触发限流，
# 因此 CLI 调用串行化并保证最小间隔
_CALL_LOCK = threading.Lock()
MIN_CALL_INTERVAL = 0.5  # seconds between CLI invocations
_last_call_ts = 0.0


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
        # 后端频控：非业务空数据，标记为限流错误（不缓存），由调用方退避重试/降级
        error = "westock 限流（请求过于频繁）"
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
