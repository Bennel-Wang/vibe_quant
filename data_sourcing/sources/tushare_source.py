"""
Tushare Pro 数据源 - 历史数据
支持: A股个股(前复权)、A股指数、A股板块(行业指数)、港股大盘(国际指数)
API文档:
  A股前复权行情: https://tushare.pro/document/2?doc_id=109  (pro_bar, adj='qfq')
  A股日线(未复权): https://tushare.pro/document/2?doc_id=27  (daily)
  A股每日指标(PE/PB): https://tushare.pro/document/2?doc_id=32  (daily_basic, 需2000积分)
  指数日线: https://tushare.pro/document/2?doc_id=95
  国际指数: https://tushare.pro/document/2?doc_id=211
  港股日线: https://tushare.pro/document/2?doc_id=31
"""
import io
import sys
import pandas as pd
from typing import Optional
from sources.base_source import BaseSource
import code_mapper
from config import TUSHARE_TOKEN
from contextlib import contextmanager


@contextmanager
def _suppress_stderr(buf=None):
    """Context manager: redirect both stdout and stderr to silence noisy tushare SDK output.
    tushare data_pro.py uses `print(e)` (stdout) for exception messages."""
    old_out, old_err = sys.stdout, sys.stderr
    sink = buf if buf is not None else io.StringIO()
    sys.stdout = sys.stderr = sink
    try:
        yield
    finally:
        sys.stdout, sys.stderr = old_out, old_err


class TushareSource(BaseSource):
    def __init__(self):
        super().__init__("tushare")
        self.pro = None
        self._ts = None

    def init(self) -> bool:
        try:
            import tushare as ts
            self._ts = ts
            if not TUSHARE_TOKEN:
                self.logger.warning("TUSHARE_TOKEN 未设置, 尝试使用默认token")
                ts.set_token("")
                self.pro = ts.pro_api()
            else:
                ts.set_token(TUSHARE_TOKEN)
                self.pro = ts.pro_api(TUSHARE_TOKEN)
            self._initialized = True
            self.logger.info("Tushare Pro 初始化成功")
            return True
        except Exception as e:
            self.logger.error(f"Tushare Pro 初始化失败: {e}")
            return False

    def fetch_daily(self, code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        if not self._initialized and not self.init():
            return None

        market = code_mapper.get_market(code)
        ts_code = code_mapper.to_tushare(code)

        try:
            if market == "A_STOCK":
                return self._fetch_a_stock(code, ts_code, start_date, end_date)
            elif market == "A_INDEX":
                return self._fetch_a_index(code, ts_code, start_date, end_date)
            elif market == "HK_INDEX":
                return self._fetch_hk_index(code, start_date, end_date)
            elif market == "HK_STOCK":
                return self._fetch_hk_stock(code, ts_code, start_date, end_date)
            else:
                self.logger.warning(f"Tushare Pro 不支持的市场类型: {market} for {code}")
                return None

        except Exception as e:
            self.logger.error(f"Tushare Pro 获取失败 {code}: {e}")
            return None

    def _fetch_a_stock(self, code: str, ts_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取A股个股前复权日线行情 + PE/PB基本面指标
        优先使用 pro_bar(adj='qfq')，若受 pandas 版本限制则手动使用 adj_factor 复权
        """
        df = None

        # 1. 优先尝试 pro_bar 前复权（SDK封装）
        # 用 redirect_stderr 屏蔽 tushare SDK 内部的 pandas 2.x 兼容性警告
        try:
            _buf = io.StringIO()
            with _suppress_stderr(_buf):
                df = self._ts.pro_bar(
                    ts_code=ts_code,
                    adj='qfq',
                    start_date=start_date,
                    end_date=end_date,
                    asset='E',
                    api=self.pro
                )
            if df is not None and not df.empty:
                self.logger.info(f"Tushare pro_bar(qfq) 成功: {code}")
        except Exception as e:
            self.logger.warning(f"Tushare pro_bar 不可用, 改用 daily+adj_factor 手动复权")
            df = None

        # 2. 如果 pro_bar 失败，获取未复权日线 + 复权因子，手动计算前复权
        if df is None or df.empty:
            try:
                df_raw = self.pro.daily(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date
                )
                if df_raw is None or df_raw.empty:
                    self.logger.warning(f"Tushare daily 也无数据: {code}")
                    return None

                # 获取复权因子（含最新一天的因子用于前复权基准）
                try:
                    df_adj = self.pro.adj_factor(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                    if df_adj is not None and not df_adj.empty:
                        df_adj["trade_date"] = df_adj["trade_date"].astype(str)
                        df_adj["adj_factor"] = pd.to_numeric(df_adj["adj_factor"], errors="coerce")
                        # 前复权基准：最新日期的 adj_factor
                        latest_factor = df_adj.loc[df_adj["trade_date"].idxmax(), "adj_factor"]
                        if pd.isna(latest_factor) or latest_factor == 0:
                            latest_factor = 1.0
                        df_raw["trade_date"] = df_raw["trade_date"].astype(str)
                        df_raw = df_raw.merge(
                            df_adj[["trade_date", "adj_factor"]],
                            on="trade_date", how="left"
                        )
                        df_raw["adj_factor"] = df_raw["adj_factor"].fillna(1.0)
                        # 应用前复权: price_qfq = price_raw * (adj_factor / latest_factor)
                        ratio = df_raw["adj_factor"] / latest_factor
                        for col in ["open", "high", "low", "close"]:
                            df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce") * ratio
                        self.logger.info(f"Tushare daily+adj_factor 手动前复权成功: {code}")
                    else:
                        self.logger.warning(f"Tushare adj_factor 无数据，使用未复权价格: {code}")
                except Exception as adj_e:
                    self.logger.warning(f"Tushare adj_factor 不可用(需2000积分)，使用未复权价格: {code}, {adj_e}")

                df = df_raw

            except Exception as e:
                self.logger.error(f"Tushare daily 获取失败: {code}, {e}")
                return None

        if df is None or df.empty:
            self.logger.warning(f"Tushare Pro A股无数据: {code} ({start_date}-{end_date})")
            return None

        result = pd.DataFrame()
        result["uniformed_stock_code"] = [code] * len(df)
        result["trade_date"] = df["trade_date"].astype(str)
        result["open"] = pd.to_numeric(df["open"], errors="coerce")
        result["high"] = pd.to_numeric(df["high"], errors="coerce")
        result["low"] = pd.to_numeric(df["low"], errors="coerce")
        result["close"] = pd.to_numeric(df["close"], errors="coerce")
        result["vol"] = pd.to_numeric(df["vol"], errors="coerce")

        # 获取 PE_TTM/PB (daily_basic 接口，需要2000积分)
        try:
            df_basic = self.pro.daily_basic(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields="ts_code,trade_date,pe_ttm,pb"
            )
            if df_basic is not None and not df_basic.empty:
                df_basic["trade_date"] = df_basic["trade_date"].astype(str)
                df_basic["pe_ttm"] = pd.to_numeric(df_basic["pe_ttm"], errors="coerce")
                df_basic["pb"] = pd.to_numeric(df_basic["pb"], errors="coerce")
                result = result.merge(
                    df_basic[["trade_date", "pe_ttm", "pb"]],
                    on="trade_date",
                    how="left"
                )
                self.logger.info(f"Tushare Pro PE_TTM/PB 获取成功: {code}, {len(df_basic)} 条")
            else:
                result["pe_ttm"] = float("nan")
                result["pb"] = float("nan")
        except Exception as e:
            self.logger.warning(f"Tushare Pro daily_basic (PE_TTM/PB) 不可用(需2000积分): {code}, {e}")
            result["pe_ttm"] = float("nan")
            result["pb"] = float("nan")

        result = result.sort_values("trade_date").reset_index(drop=True)
        self.logger.info(f"Tushare Pro A股(前复权) 获取成功: {code}, {len(result)} 条")
        return result

    def _fetch_a_index(self, code: str, ts_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取A股指数日线行情（指数不做复权）"""
        df = self.pro.index_daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date
        )

        if df is None or df.empty:
            self.logger.warning(f"Tushare Pro A股指数无数据: {code} ({start_date}-{end_date})")
            return None

        result = pd.DataFrame()
        result["uniformed_stock_code"] = [code] * len(df)
        result["trade_date"] = df["trade_date"].astype(str)
        result["open"] = pd.to_numeric(df["open"], errors="coerce")
        result["high"] = pd.to_numeric(df["high"], errors="coerce")
        result["low"] = pd.to_numeric(df["low"], errors="coerce")
        result["close"] = pd.to_numeric(df["close"], errors="coerce")
        result["vol"] = pd.to_numeric(df["vol"], errors="coerce")
        result["pe_ttm"] = float("nan")
        result["pb"] = float("nan")

        result = result.sort_values("trade_date").reset_index(drop=True)
        self.logger.info(f"Tushare Pro A股指数 获取成功: {code}, {len(result)} 条")
        return result

    def _fetch_hk_index(self, code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取港股大盘指数（HSI/HSCEI等）- 使用 index_global 接口，需6000积分"""
        symbol, _ = code_mapper._split_code(code)
        try:
            df = self.pro.index_global(
                ts_code=symbol,
                start_date=start_date,
                end_date=end_date
            )
        except Exception as e:
            self.logger.warning(f"Tushare Pro index_global 不可用(需6000积分): {code}, {e}")
            return None

        if df is None or df.empty:
            self.logger.warning(f"Tushare Pro 港股指数无数据: {code} ({start_date}-{end_date})")
            return None

        result = pd.DataFrame()
        result["uniformed_stock_code"] = [code] * len(df)
        result["trade_date"] = df["trade_date"].astype(str)
        result["open"] = pd.to_numeric(df["open"], errors="coerce")
        result["high"] = pd.to_numeric(df["high"], errors="coerce")
        result["low"] = pd.to_numeric(df["low"], errors="coerce")
        result["close"] = pd.to_numeric(df["close"], errors="coerce")
        result["vol"] = pd.to_numeric(df["vol"], errors="coerce")
        result["pe_ttm"] = float("nan")
        result["pb"] = float("nan")

        result = result.sort_values("trade_date").reset_index(drop=True)
        self.logger.info(f"Tushare Pro 港股指数 获取成功: {code}, {len(result)} 条")
        return result

    def _fetch_hk_stock(self, code: str, ts_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取港股个股日线行情 - 使用 hk_daily 接口，需更高积分"""
        try:
            df = self.pro.hk_daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )
        except Exception as e:
            self.logger.warning(f"Tushare Pro hk_daily 不可用(需更高积分): {code}, {e}")
            return None

        if df is None or df.empty:
            self.logger.warning(f"Tushare Pro 港股无数据: {code} ({start_date}-{end_date})")
            return None

        result = pd.DataFrame()
        result["uniformed_stock_code"] = [code] * len(df)
        result["trade_date"] = df["trade_date"].astype(str)
        result["open"] = pd.to_numeric(df["open"], errors="coerce")
        result["high"] = pd.to_numeric(df["high"], errors="coerce")
        result["low"] = pd.to_numeric(df["low"], errors="coerce")
        result["close"] = pd.to_numeric(df["close"], errors="coerce")
        result["vol"] = pd.to_numeric(df["vol"], errors="coerce")
        result["pe_ttm"] = float("nan")
        result["pb"] = float("nan")

        result = result.sort_values("trade_date").reset_index(drop=True)
        self.logger.info(f"Tushare Pro 港股 获取成功: {code}, {len(result)} 条")
        return result

    def supports_market(self, market: str) -> bool:
        return market in ("A_STOCK", "A_INDEX", "HK_STOCK", "HK_INDEX")

    def is_realtime(self) -> bool:
        return False

    def close(self):
        pass
