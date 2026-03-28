"""
Easyquotation 数据源 - 实时数据
支持: A股个股、A股指数、港股个股、港股指数(日K/实时)
API: https://github.com/shidenggui/easyquotation
港股指数bug修复: https://blog.csdn.net/zhangkexin_z/article/details/140048461
"""
import pandas as pd
import datetime
from typing import Optional
from sources.base_source import BaseSource
from sources.fixed_hkquote import FixedHKQuote
import code_mapper


class EasyquotationSource(BaseSource):
    def __init__(self):
        super().__init__("easyquotation")
        self._eq = None

    def init(self) -> bool:
        try:
            import easyquotation
            self._eq = easyquotation
            self._initialized = True
            self.logger.info("Easyquotation 初始化成功")
            return True
        except Exception as e:
            self.logger.error(f"Easyquotation 初始化失败: {e}")
            return False

    def fetch_daily(self, code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """easyquotation主要用于实时数据，只返回当天行情"""
        if not self._initialized and not self.init():
            return None

        market = code_mapper.get_market(code)

        try:
            if market in ("HK_STOCK", "HK_INDEX"):
                return self._fetch_hk(code, start_date, end_date)
            elif market in ("A_STOCK", "A_INDEX"):
                return self._fetch_a_share(code)
            else:
                self.logger.warning(f"Easyquotation 不支持: {market}")
                return None
        except Exception as e:
            self.logger.error(f"Easyquotation 获取失败 {code}: {e}")
            return None

    def _fetch_a_share(self, code: str) -> Optional[pd.DataFrame]:
        """获取A股实时行情"""
        eq_code, source = code_mapper.to_easyquotation(code)
        if eq_code is None:
            return None

        quotation = self._eq.use("sina")
        data = quotation.real(eq_code)

        if not data:
            self.logger.warning(f"Easyquotation A股无数据: {code}")
            return None

        # 获取第一个key的数据
        key = list(data.keys())[0]
        info = data[key]

        today = datetime.date.today().strftime("%Y%m%d")

        result = pd.DataFrame([{
            "uniformed_stock_code": code,
            "trade_date": today,
            "open": float(info.get("open", 0)),
            "high": float(info.get("high", 0)),
            "low": float(info.get("low", 0)),
            "close": float(info.get("now", 0)),
            "vol": float(info.get("turnover", 0)) / 100.0,  # 股数转手
        }])

        self.logger.info(f"Easyquotation A股实时: {code}, close={info.get('now')}")
        return result

    def _fetch_hk(self, code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取港股数据 - 支持日K线历史和实时(含指数)"""
        symbol, _ = code_mapper._split_code(code)
        market = code_mapper.get_market(code)

        # 港股指数(如HSI)不支持日K线，直接走实时
        if market != "HK_INDEX":
            # 尝试港股日K线 (腾讯数据源) - 仅个股
            try:
                quotation = self._eq.use("daykline")
                data = quotation.real([symbol])

                if data and symbol in data and data[symbol]:
                    rows = []
                    for item in data[symbol]:
                        # [日期, 今开, 今收, 最高, 最低, 成交量]
                        date_str = item[0].replace("-", "")
                        if start_date <= date_str <= end_date:
                            rows.append({
                                "uniformed_stock_code": code,
                                "trade_date": date_str,
                                "open": float(item[1]),
                                "high": float(item[3]),
                                "low": float(item[4]),
                                "close": float(item[2]),
                                "vol": float(item[5]) / 100.0,
                            })

                    if rows:
                        df = pd.DataFrame(rows)
                        df = df.sort_values("trade_date").reset_index(drop=True)
                        self.logger.info(f"Easyquotation 港股日K: {code}, {len(df)} 条")
                        return df
            except Exception as e:
                self.logger.debug(f"Easyquotation 港股日K失败: {e}")

        # 使用修复版HKQuote获取实时行情(支持HSI等字母代码)
        try:
            fixed_hk = FixedHKQuote()
            data = fixed_hk.stocks([symbol])
            if data and symbol in data:
                info = data[symbol]
                today = datetime.date.today().strftime("%Y%m%d")
                result = pd.DataFrame([{
                    "uniformed_stock_code": code,
                    "trade_date": today,
                    "open": float(info.get("openPrice", 0)),
                    "high": float(info.get("high", 0)),
                    "low": float(info.get("low", 0)),
                    "close": float(info.get("price", 0)),
                    "vol": float(info.get("amount", 0)) / 100.0,
                }])
                self.logger.info(f"Easyquotation 港股实时(FixedHKQuote): {code}, close={info.get('price')}")
                return result
        except Exception as e:
            self.logger.warning(f"Easyquotation FixedHKQuote 失败 {code}: {e}")

        # 最后回退到原版hkquote(仅数字代码)
        try:
            quotation = self._eq.use("hkquote")
            data = quotation.real([symbol])
            if data and symbol in data:
                info = data[symbol]
                today = datetime.date.today().strftime("%Y%m%d")
                result = pd.DataFrame([{
                    "uniformed_stock_code": code,
                    "trade_date": today,
                    "open": float(info.get("openPrice", 0)),
                    "high": float(info.get("high", 0)),
                    "low": float(info.get("low", 0)),
                    "close": float(info.get("price", 0)),
                    "vol": float(info.get("amount", 0)) / 100.0,
                }])
                self.logger.info(f"Easyquotation 港股实时: {code}")
                return result
        except Exception as e:
            self.logger.debug(f"Easyquotation 港股实时失败: {e}")

        return None

    def supports_market(self, market: str) -> bool:
        return market in ("A_STOCK", "A_INDEX", "HK_STOCK", "HK_INDEX")

    def is_realtime(self) -> bool:
        return True

    def close(self):
        pass
