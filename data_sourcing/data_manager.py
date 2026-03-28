"""
数据管理器 - 核心调度模块
- 自动降级切换数据源
- 增量更新逻辑
- 技术指标计算
"""
import datetime
import pandas as pd
import numpy as np
from typing import Optional, List

from config import HISTORICAL_SOURCES, REALTIME_SOURCES, setup_logger
import code_mapper
import trading_calendar
from validator import validate_dataframe, has_valid_data, validate_data_completeness
from storage import load_existing_data, merge_and_save, save_with_indicators, get_latest_valid_date
from indicators import compute_all_indicators, compute_rolling_percentile_rank

from sources.tushare_source import TushareSource
from sources.baostock_source import BaostockSource
from sources.easyquotation_source import EasyquotationSource
from sources.pytdx_source import PytdxSource
from sources.mootdx_source import MootdxSource

logger = setup_logger("data_manager", "data_manager.log")


class DataManager:
    """统一数据管理器"""

    def __init__(self):
        self.historical_sources = []
        self.realtime_sources = []
        self._source_map = {}
        self._init_sources()

    def _init_sources(self):
        """初始化所有数据源"""
        source_classes = {
            "tushare": TushareSource,
            "baostock": BaostockSource,
            "easyquotation": EasyquotationSource,
            "pytdx": PytdxSource,
            "mootdx": MootdxSource,
        }

        for name in HISTORICAL_SOURCES:
            if name in source_classes:
                src = source_classes[name]()
                self.historical_sources.append(src)
                self._source_map[name] = src

        for name in REALTIME_SOURCES:
            if name in source_classes:
                src = source_classes[name]()
                self.realtime_sources.append(src)
                self._source_map[name] = src

    def get_source(self, name: str):
        return self._source_map.get(name)

    def fetch(self, code: str, start_date: str, end_date: str = None,
              freq: str = "day") -> Optional[pd.DataFrame]:
        """获取股票数据（含自动降级和增量更新）

        Args:
            code: 统一股票代码
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD，默认今天
            freq: 频率 day/week/month

        Returns:
            包含OHLCV和技术指标的DataFrame
        """
        if end_date is None:
            end_date = datetime.date.today().strftime("%Y%m%d")

        market = code_mapper.get_market(code)
        logger.info(f"开始获取数据: {code} ({market}), {start_date}-{end_date}, {freq}")

        # 1. 检查本地数据，确定增量起始日期
        actual_start = start_date
        latest_valid = get_latest_valid_date(code, freq)
        if latest_valid:
            # 从最新有效日期的下一天开始
            d = datetime.datetime.strptime(latest_valid, "%Y%m%d")
            next_day = (d + datetime.timedelta(days=1)).strftime("%Y%m%d")
            if next_day > start_date:
                actual_start = next_day
                logger.info(f"增量更新: 从 {actual_start} 开始 (本地数据到 {latest_valid})")

        # 1b. 检查本地数据是否覆盖了请求的起始日期
        # 避免只有短窗口数据（如实时7天）被误认为完整数据
        if actual_start > end_date:
            existing_check = load_existing_data(code, freq)
            if not existing_check.empty:
                earliest_local = str(existing_check['trade_date'].min())
                try:
                    gap = (datetime.datetime.strptime(earliest_local, "%Y%m%d")
                           - datetime.datetime.strptime(start_date, "%Y%m%d")).days
                except ValueError:
                    gap = 0
                if gap > 30:
                    logger.warning(
                        f"本地数据不完整 ({code}): 最早 {earliest_local}, "
                        f"请求起始 {start_date}, 差距 {gap} 天，需重新获取"
                    )
                    actual_start = start_date

        if actual_start > end_date:
            logger.info(f"本地数据已是最新: {code}")
            df = load_existing_data(code, freq)
            return self._add_indicators(df, freq)

        # 2. 尝试历史数据源
        new_data = None
        for source in self.historical_sources:
            if not source.supports_market(market):
                logger.debug(f"{source.name} 不支持 {market}, 跳过")
                continue

            logger.info(f"尝试历史数据源: {source.name}")
            try:
                data = source.fetch_daily(code, actual_start, end_date)
                if data is not None and has_valid_data(data):
                    # 数据完整性检查（仅对长日期范围有意义，短范围跳过）
                    # 超过20%的交易日缺失则尝试下一个数据源
                    try:
                        days_requested = (
                            datetime.datetime.strptime(end_date, "%Y%m%d") -
                            datetime.datetime.strptime(actual_start, "%Y%m%d")
                        ).days
                    except ValueError:
                        days_requested = 0

                    if days_requested >= 10:
                        market_code = "HK" if market.startswith("HK") else "A"
                        completeness = validate_data_completeness(
                            code, data, actual_start, market=market_code, tolerance=0.20
                        )
                        if not completeness["ok"] and completeness.get("missing_pct", 0) > 20:
                            logger.warning(
                                f"{source.name} 数据缺失过多 "
                                f"({completeness.get('missing_pct', '?')}%), 尝试下一个数据源"
                            )
                            continue

                    # 检查是否需要实时数据
                    if self._needs_realtime(data, market):
                        logger.info(f"{source.name} 数据非最新，尝试补充实时数据")
                        new_data = data
                        break  # 保留历史数据，后续补充实时
                    else:
                        new_data = data
                        break
                else:
                    logger.warning(f"{source.name} 数据无效或为空, 尝试下一个")
            except Exception as e:
                logger.error(f"{source.name} 异常: {e}")
                continue

        # 3. 如果需要实时数据或历史数据源全部失败
        today = datetime.date.today().strftime("%Y%m%d")
        need_realtime = (
            new_data is None or
            self._needs_realtime(new_data, market) and end_date >= today
        )

        if need_realtime:
            logger.info("尝试实时数据源补充今日数据")
            for source in self.realtime_sources:
                if not source.supports_market(market):
                    continue

                logger.info(f"尝试实时数据源: {source.name}")
                try:
                    rt_data = source.fetch_daily(code, actual_start, end_date)
                    if rt_data is not None and has_valid_data(rt_data):
                        if new_data is not None:
                            # 合并历史 + 实时
                            new_data = pd.concat([new_data, rt_data], ignore_index=True)
                            new_data = new_data.drop_duplicates(subset=["trade_date"], keep="last")
                        else:
                            new_data = rt_data
                        logger.info(f"{source.name} 实时数据获取成功")
                        break
                    else:
                        logger.warning(f"{source.name} 实时数据无效")
                except Exception as e:
                    logger.error(f"{source.name} 实时异常: {e}")
                    continue

        # 4. 增量合并并保存
        if new_data is not None and not new_data.empty:
            merged = merge_and_save(code, new_data, freq)
        else:
            logger.warning(f"所有数据源均未获取到有效数据: {code}")
            merged = load_existing_data(code, freq)

        if merged.empty:
            return None

        # 5. 计算技术指标
        result = self._add_indicators(merged, freq)
        save_with_indicators(code, result, freq)
        return result

    def _needs_realtime(self, data: pd.DataFrame, market: str) -> bool:
        """判断是否需要补充实时数据
        如果最新数据日期 < 今天 且 今天是交易日，则需要实时数据
        """
        if data is None or data.empty:
            return True

        today = datetime.date.today()
        if not trading_calendar.is_trading_day(today, market):
            return False

        latest_date_str = str(data["trade_date"].max())
        today_str = today.strftime("%Y%m%d")

        return latest_date_str < today_str

    def _add_indicators(self, df: pd.DataFrame, freq: str = "day") -> pd.DataFrame:
        """添加技术指标（pe/pb等基本面列保持不变）"""
        if df is None or df.empty or len(df) < 2:
            return df

        df = df.copy()

        # 确保数值类型
        for col in ["open", "high", "low", "close", "vol"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 补充date和volume列
        df["date"] = df["trade_date"]
        df["volume"] = df["vol"]

        try:
            indicators = compute_all_indicators({
                "open": df["open"].values,
                "high": df["high"].values,
                "low": df["low"].values,
                "close": df["close"].values,
                "vol": df["vol"].values,
            })

            for key, values in indicators.items():
                df[key] = values

        except Exception as e:
            logger.error(f"计算技术指标失败: {e}")

        # 计算 RSI_6 百分位（前100个周期）和 PE_TTM 百分位（前10年）
        # 注：week/month CSV 存储的是日线原始数据，窗口按交易日换算
        # day: 100日; week: ~100周=500日; month: ~100月=2100日
        rsi6_windows = {"day": 100, "week": 500, "month": 2100}
        # PE_TTM 10年: day=2520日, week/month同样以日线行数近似
        pettm_windows = {"day": 2520, "week": 2520, "month": 2520}
        rsi6_window = rsi6_windows.get(freq, 100)
        pettm_window = pettm_windows.get(freq, 2520)

        if "rsi_6" in df.columns:
            try:
                rsi6_arr = pd.to_numeric(df["rsi_6"], errors="coerce").values
                df["rsi6_pct100"] = compute_rolling_percentile_rank(rsi6_arr, rsi6_window)
            except Exception as e:
                logger.error(f"计算 rsi6_pct100 失败: {e}")

        if "pe_ttm" in df.columns:
            try:
                pe_arr = pd.to_numeric(df["pe_ttm"], errors="coerce").values
                # 仅对正值有意义（亏损时PE为负，排除）
                pe_arr_filtered = np.where(pe_arr > 0, pe_arr, np.nan)
                df["pettm_pct10y"] = compute_rolling_percentile_rank(pe_arr_filtered, pettm_window)
            except Exception as e:
                logger.error(f"计算 pettm_pct10y 失败: {e}")

        return df

    def close_all(self):
        """关闭所有数据源"""
        for src in list(self._source_map.values()):
            try:
                src.close()
            except Exception:
                pass
