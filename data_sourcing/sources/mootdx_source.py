"""
Mootdx 数据源 - 实时/历史数据
支持: A股个股、A股指数
注意: mootdx的market='hk'实际使用标准TDX协议，返回的不是真实港股数据
      mootdx的ext扩展市场接口已官方标注失效
      港股数据请使用pytdx ext或easyquotation
API: https://github.com/mootdx/mootdx
"""
import pandas as pd
import datetime
from typing import Optional
from sources.base_source import BaseSource
from sources.adjust_utils import apply_qfq
import code_mapper


class MootdxSource(BaseSource):
    def __init__(self):
        super().__init__("mootdx")
        self._std_client = None
        self._ext_client = None

    def init(self) -> bool:
        try:
            from mootdx.quotes import Quotes
            self._Quotes = Quotes
            self._initialized = True
            self.logger.info("Mootdx 初始化成功")
            return True
        except Exception as e:
            self.logger.error(f"Mootdx 初始化失败: {e}")
            return False

    def _get_std_client(self):
        """获取标准市场客户端"""
        if self._std_client is None:
            self._std_client = self._Quotes.factory(market="std", multithread=True, heartbeat=True)
        return self._std_client

    def _get_ext_client(self):
        """获取扩展市场客户端（港股）"""
        if self._ext_client is None:
            self._ext_client = self._Quotes.factory(market="ext")
        return self._ext_client

    def fetch_daily(self, code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        if not self._initialized and not self.init():
            return None

        market = code_mapper.get_market(code)
        symbol, market_type = code_mapper.to_mootdx(code)

        if symbol is None:
            self.logger.warning(f"Mootdx 无法转换代码: {code}")
            return None

        try:
            if market_type == "std":
                return self._fetch_std(code, symbol, market, start_date, end_date)
            else:
                return self._fetch_ext(code, symbol, start_date, end_date)
        except Exception as e:
            self.logger.error(f"Mootdx 获取失败 {code}: {e}")
            return None

    def _fetch_std(self, code, symbol, market_type, start_date, end_date):
        """获取A股数据"""
        try:
            client = self._get_std_client()

            # mootdx的offset参数是返回记录数(从最新往回), 一次取够
            record_count = 1600  # 约6年日线数据

            if market_type in ("A_INDEX",):
                df = client.index(symbol=symbol, frequency=9, offset=record_count)
            else:
                df = client.bars(symbol=symbol, frequency=9, offset=record_count)

            if df is None or df.empty:
                self.logger.warning(f"Mootdx 无数据: {code}")
                return None

            # 处理日期 - mootdx的datetime可能在index或列中
            if "datetime" in df.columns:
                df["trade_date"] = df["datetime"].astype(str).str[:10].str.replace("-", "")
            elif hasattr(df.index, 'strftime'):
                df["trade_date"] = df.index.strftime("%Y%m%d")
            else:
                df = df.reset_index()
                if "datetime" in df.columns:
                    df["trade_date"] = df["datetime"].astype(str).str[:10].str.replace("-", "")
                elif "date" in df.columns:
                    df["trade_date"] = df["date"].astype(str).str[:10].str.replace("-", "")
                else:
                    df["trade_date"] = df.index.astype(str)

            df = df[(df["trade_date"] >= start_date) & (df["trade_date"] <= end_date)]

            if df.empty:
                self.logger.warning(f"Mootdx 日期范围内无数据: {code}")
                return None

            # 选择正确的列名 - mootdx可能用不同的列名
            vol_col = "vol" if "vol" in df.columns else "volume" if "volume" in df.columns else None

            result = pd.DataFrame()
            result["uniformed_stock_code"] = [code] * len(df)
            result["trade_date"] = df["trade_date"].values
            result["open"] = pd.to_numeric(df["open"], errors="coerce").values
            result["high"] = pd.to_numeric(df["high"], errors="coerce").values
            result["low"] = pd.to_numeric(df["low"], errors="coerce").values
            result["close"] = pd.to_numeric(df["close"], errors="coerce").values
            if vol_col:
                result["vol"] = (pd.to_numeric(df[vol_col], errors="coerce") / 100.0).values
            else:
                result["vol"] = 0.0

            result = result.drop_duplicates(subset=["trade_date"]).sort_values("trade_date").reset_index(drop=True)

            # 只对A股个股应用前复权（指数不需要复权）
            if market_type not in ("A_INDEX",):
                result = apply_qfq(result, code)

            self.logger.info(f"Mootdx A股获取成功: {code}, {len(result)} 条")
            return result

        except Exception as e:
            self.logger.error(f"Mootdx A股获取异常 {code}: {e}")
            return None

    def _fetch_ext(self, code, symbol, start_date, end_date):
        """获取港股数据 - mootdx扩展市场目前已失效"""
        self.logger.warning(f"Mootdx 扩展市场(港股)接口目前已失效: {code}")
        return None

    def supports_market(self, market: str) -> bool:
        # mootdx仅可靠支持A股，港股market='hk'返回数据不准确
        return market in ("A_STOCK", "A_INDEX")

    def is_realtime(self) -> bool:
        return True

    def close(self):
        pass
