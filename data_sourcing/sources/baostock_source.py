"""
Baostock 数据源 - 历史数据
支持: A股个股、A股指数
API: bs.query_history_k_data_plus
"""
import pandas as pd
from typing import Optional
from sources.base_source import BaseSource
import code_mapper


class BaostockSource(BaseSource):
    def __init__(self):
        super().__init__("baostock")
        self._bs = None

    def init(self) -> bool:
        try:
            import baostock as bs
            self._bs = bs
            lg = bs.login()
            if lg.error_code != '0':
                self.logger.error(f"Baostock 登录失败: {lg.error_msg}")
                return False
            self._initialized = True
            self.logger.info("Baostock 初始化成功")
            return True
        except Exception as e:
            self.logger.error(f"Baostock 初始化失败: {e}")
            return False

    def fetch_daily(self, code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        if not self._initialized and not self.init():
            return None

        market = code_mapper.get_market(code)
        if not self.supports_market(market):
            self.logger.warning(f"Baostock 不支持: {market} for {code}")
            return None

        bs_code = code_mapper.to_baostock(code)
        if bs_code is None:
            self.logger.warning(f"Baostock 无法转换代码: {code}")
            return None

        # 日期格式转换 YYYYMMDD -> YYYY-MM-DD
        sd = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
        ed = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"

        try:
            fields = "date,code,open,high,low,close,volume"
            rs = self._bs.query_history_k_data_plus(
                bs_code,
                fields,
                start_date=sd,
                end_date=ed,
                frequency="d",
                adjustflag="2"  # 前复权
            )

            if rs.error_code != '0':
                self.logger.error(f"Baostock 查询错误 {code}: {rs.error_msg}")
                return None

            data_list = []
            while rs.error_code == '0' and rs.next():
                data_list.append(rs.get_row_data())

            if not data_list:
                self.logger.warning(f"Baostock 无数据: {code} ({start_date}-{end_date})")
                return None

            df = pd.DataFrame(data_list, columns=rs.fields)

            # 统一格式
            result = pd.DataFrame()
            result["uniformed_stock_code"] = [code] * len(df)
            result["trade_date"] = df["date"].str.replace("-", "")
            result["open"] = pd.to_numeric(df["open"], errors="coerce")
            result["high"] = pd.to_numeric(df["high"], errors="coerce")
            result["low"] = pd.to_numeric(df["low"], errors="coerce")
            result["close"] = pd.to_numeric(df["close"], errors="coerce")
            # baostock volume 单位是股，转换为手（/100）
            result["vol"] = pd.to_numeric(df["volume"], errors="coerce") / 100.0

            result = result.sort_values("trade_date").reset_index(drop=True)
            self.logger.info(f"Baostock 获取成功: {code}, {len(result)} 条")
            return result

        except Exception as e:
            self.logger.error(f"Baostock 获取失败 {code}: {e}")
            return None

    def supports_market(self, market: str) -> bool:
        return market in ("A_STOCK", "A_INDEX")

    def is_realtime(self) -> bool:
        return False

    def close(self):
        try:
            if self._bs and self._initialized:
                self._bs.logout()
                self.logger.info("Baostock 已登出")
        except Exception:
            pass
