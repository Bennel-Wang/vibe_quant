"""westock 自用数据 API：腾讯自选股 CLI 的 JSON 化封装。

直接调用（进程内）:
    from data_sourcing.westock_api import run_westock
    run_westock(["kline", "sh600519", "--period", "day", "--limit", "20"])

HTTP 服务见 server.py 模块说明。
"""
from .client import find_westock, run_westock
from .parser import parse_output

__all__ = ["find_westock", "run_westock", "parse_output"]
