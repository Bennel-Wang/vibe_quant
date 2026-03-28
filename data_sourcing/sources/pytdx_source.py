"""
Pytdx 数据源 - 实时/历史数据
支持: A股个股、A股指数、港股个股、港股指数
API: https://rainx.gitbooks.io/pytdx/content/pytdx_hq.html
港股扩展行情: https://blog.csdn.net/chang1976272446/article/details/83618539
  - 港股个股 market=31, 港股指数 market=27
  - 扩展服务器: 180.153.18.176:7721
"""
import pandas as pd
import datetime
from typing import Optional
from sources.base_source import BaseSource
from sources.adjust_utils import apply_qfq
import code_mapper
from config import PYTDX_HQ_HOSTS, PYTDX_EX_HOSTS


class PytdxSource(BaseSource):
    def __init__(self):
        super().__init__("pytdx")
        self._api = None
        self._ex_api = None

    def init(self) -> bool:
        try:
            from pytdx.hq import TdxHq_API
            self._api = TdxHq_API(heartbeat=True, auto_retry=True)
            self._TdxHq_API = TdxHq_API
            self._initialized = True
            self.logger.info("Pytdx 初始化成功")
            return True
        except Exception as e:
            self.logger.error(f"Pytdx 初始化失败: {e}")
            return False

    def _connect_hq(self):
        """连接A股行情服务器"""
        for host, port in PYTDX_HQ_HOSTS:
            try:
                if self._api.connect(host, port):
                    self.logger.debug(f"Pytdx HQ 已连接: {host}:{port}")
                    return True
            except Exception:
                continue
        self.logger.error("Pytdx HQ 所有服务器连接失败")
        return False

    def _connect_ex(self):
        """连接扩展行情服务器（港股）"""
        try:
            from pytdx.exhq import TdxExHq_API
            if self._ex_api is None:
                self._ex_api = TdxExHq_API(auto_retry=True, raise_exception=False)
            for host, port in PYTDX_EX_HOSTS:
                try:
                    if self._ex_api.connect(host, port, time_out=10):
                        self.logger.debug(f"Pytdx ExHQ 已连接: {host}:{port}")
                        return True
                except Exception:
                    continue
        except ImportError:
            self.logger.error("pytdx 扩展行情模块不可用")
        self.logger.error("Pytdx ExHQ 所有服务器连接失败")
        return False

    def fetch_daily(self, code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        if not self._initialized and not self.init():
            return None

        market = code_mapper.get_market(code)
        pytdx_market, symbol, is_hk = code_mapper.to_pytdx(code)

        if pytdx_market is None:
            self.logger.warning(f"Pytdx 无法转换代码: {code}")
            return None

        try:
            if is_hk:
                return self._fetch_hk(code, symbol, start_date, end_date)
            else:
                return self._fetch_a_share(code, pytdx_market, symbol, market, start_date, end_date)
        except Exception as e:
            self.logger.error(f"Pytdx 获取失败 {code}: {e}")
            return None

    def _fetch_a_share(self, code, market, symbol, market_type, start_date, end_date):
        """获取A股数据"""
        if not self._connect_hq():
            return None

        try:
            all_bars = []
            offset = 0
            batch_size = 800

            while True:
                if market_type == "A_INDEX":
                    bars = self._api.get_index_bars(9, market, symbol, offset, batch_size)
                else:
                    bars = self._api.get_security_bars(9, market, symbol, offset, batch_size)

                if not bars:
                    break

                df_batch = self._api.to_df(bars)
                all_bars.append(df_batch)

                # 检查是否已经获取到start_date之前的数据
                if len(df_batch) > 0:
                    earliest = df_batch["datetime"].min()[:10].replace("-", "").replace("/", "")
                    if earliest <= start_date:
                        break

                offset += batch_size
                if len(bars) < batch_size:
                    break

            self._api.disconnect()

            if not all_bars:
                self.logger.warning(f"Pytdx 无数据: {code}")
                return None

            df = pd.concat(all_bars, ignore_index=True)

            # 处理日期
            df["trade_date"] = df["datetime"].str[:10].str.replace("-", "").str.replace("/", "")
            df = df[(df["trade_date"] >= start_date) & (df["trade_date"] <= end_date)]

            if df.empty:
                self.logger.warning(f"Pytdx 日期范围内无数据: {code}")
                return None

            result = pd.DataFrame()
            result["uniformed_stock_code"] = [code] * len(df)
            result["trade_date"] = df["trade_date"].values
            result["open"] = pd.to_numeric(df["open"], errors="coerce").values
            result["high"] = pd.to_numeric(df["high"], errors="coerce").values
            result["low"] = pd.to_numeric(df["low"], errors="coerce").values
            result["close"] = pd.to_numeric(df["close"], errors="coerce").values
            # pytdx volume 单位是股，转手
            result["vol"] = (pd.to_numeric(df["vol"], errors="coerce") / 100.0).values

            result = result.drop_duplicates(subset=["trade_date"]).sort_values("trade_date").reset_index(drop=True)

            # 只对A股个股应用前复权（指数本身不需要复权）
            if market_type == "A_STOCK":
                result = apply_qfq(result, code)

            self.logger.info(f"Pytdx A股获取成功: {code}, {len(result)} 条")
            return result

        except Exception as e:
            self.logger.error(f"Pytdx A股获取异常 {code}: {e}")
            try:
                self._api.disconnect()
            except Exception:
                pass
            return None

    def _fetch_hk(self, code, symbol, start_date, end_date):
        """获取港股数据（通过扩展行情接口）
        港股个股: market=31 (港股主板)
        港股指数: market=27 (港股指数, 如HSI)
        """
        if not self._connect_ex():
            return None

        market_type = code_mapper.get_market(code)
        # 港股指数用market=27, 港股个股用market=31
        hk_market = 27 if market_type == "HK_INDEX" else 31

        try:
            all_bars = []
            offset = 0
            batch_size = 800

            while True:
                bars = self._ex_api.get_instrument_bars(
                    4, hk_market, symbol, offset, batch_size
                )

                if not bars:
                    break

                df_batch = self._ex_api.to_df(bars)
                all_bars.append(df_batch)

                if len(df_batch) > 0 and "datetime" in df_batch.columns:
                    earliest = df_batch["datetime"].min()[:10].replace("-", "").replace("/", "")
                    if earliest <= start_date:
                        break

                offset += batch_size
                if len(bars) < batch_size:
                    break

            self._ex_api.disconnect()

            if not all_bars:
                self.logger.warning(f"Pytdx 港股无数据: {code}")
                return None

            df = pd.concat(all_bars, ignore_index=True)

            df["trade_date"] = df["datetime"].str[:10].str.replace("-", "").str.replace("/", "")
            df = df[(df["trade_date"] >= start_date) & (df["trade_date"] <= end_date)]

            if df.empty:
                return None

            result = pd.DataFrame()
            result["uniformed_stock_code"] = [code] * len(df)
            result["trade_date"] = df["trade_date"].values
            result["open"] = pd.to_numeric(df["open"], errors="coerce").values
            result["high"] = pd.to_numeric(df["high"], errors="coerce").values
            result["low"] = pd.to_numeric(df["low"], errors="coerce").values
            result["close"] = pd.to_numeric(df["close"], errors="coerce").values
            # 扩展行情的成交量字段为 trade (股), 转为手(/100)
            vol_col = "trade" if "trade" in df.columns else "vol" if "vol" in df.columns else None
            if vol_col:
                result["vol"] = (pd.to_numeric(df[vol_col], errors="coerce") / 100.0).values
            else:
                result["vol"] = 0.0

            result = result.drop_duplicates(subset=["trade_date"]).sort_values("trade_date").reset_index(drop=True)
            self.logger.info(f"Pytdx 港股获取成功: {code}, {len(result)} 条")
            return result

        except Exception as e:
            self.logger.error(f"Pytdx 港股获取异常 {code}: {e}")
            try:
                self._ex_api.disconnect()
            except Exception:
                pass
            return None

    def supports_market(self, market: str) -> bool:
        return market in ("A_STOCK", "A_INDEX", "HK_STOCK", "HK_INDEX")

    def is_realtime(self) -> bool:
        return True

    def close(self):
        try:
            if self._api:
                self._api.disconnect()
            if self._ex_api:
                self._ex_api.disconnect()
        except Exception:
            pass
