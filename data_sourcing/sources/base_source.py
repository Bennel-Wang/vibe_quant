"""
数据源抽象基类
"""
from abc import ABC, abstractmethod
import time
from typing import Optional
import pandas as pd
from config import setup_logger


class BaseSource(ABC):
    """数据源抽象基类"""

    def __init__(self, name: str):
        self.name = name
        self.logger = setup_logger(f"source.{name}", f"source_{name}.log")
        self._initialized = False
        self._unavailable_until = 0.0  # 熔断截止时间戳

    def mark_unavailable(self, seconds: float) -> None:
        """熔断：连续失败（登录不通/限流等）后一段时间内跳过该源，避免重复付出失败代价"""
        self._unavailable_until = time.time() + seconds
        self.logger.warning(f"{self.name} 熔断 {seconds:.0f}s（失败冷却，期间跳过该源）")

    def is_unavailable(self) -> bool:
        return time.time() < self._unavailable_until

    @abstractmethod
    def init(self) -> bool:
        """初始化数据源连接，返回是否成功"""
        pass

    @abstractmethod
    def fetch_daily(self, code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取日线数据
        Args:
            code: 统一股票代码 (如 000001.SZ)
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
        Returns:
            DataFrame with columns: uniformed_stock_code, trade_date, open, high, low, close, vol
            或 None 表示获取失败
        """
        pass

    def fetch_weekly(self, code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取周线数据（默认从日线聚合）"""
        return None

    def fetch_monthly(self, code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取月线数据（默认从日线聚合）"""
        return None

    def prefetch(self, requests: dict) -> None:
        """批量预取（可选优化）：requests = {code: (start_date, end_date)}

        默认不实现。支持批量的数据源可一次调用拉取多只，
        供后续 fetch_daily 命中，避免逐只请求。
        """
        return None

    @abstractmethod
    def supports_market(self, market: str) -> bool:
        """判断是否支持该市场类型
        market: A_STOCK, A_INDEX, HK_STOCK, HK_INDEX
        """
        pass

    @abstractmethod
    def is_realtime(self) -> bool:
        """是否为实时数据源"""
        pass

    def close(self):
        """关闭数据源连接"""
        pass

    def __repr__(self):
        return f"<{self.__class__.__name__}({self.name})>"
