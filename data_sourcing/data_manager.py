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

from config import (
    HISTORICAL_SOURCES, REALTIME_SOURCES, MISMATCH_TOLERANCE,
    DEFAULT_START_DATE, setup_logger,
)
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
from sources.westock_source import WestockSource

logger = setup_logger("data_manager", "data_manager.log")


class DataManager:
    """统一数据管理器"""

    def __init__(self):
        self.historical_sources = []
        self.realtime_sources = []
        self._source_map = {}
        self._fail_counts = {}  # source.name -> 连续失败次数（连续 3 次熔断 120s）
        self._init_sources()

    def _note_source_fail(self, source) -> None:
        """连续失败熔断：单只股票"无数据"可能是正常的，连续失败则是源故障"""
        n = self._fail_counts.get(source.name, 0) + 1
        self._fail_counts[source.name] = n
        if n >= 3 and not source.is_unavailable():
            source.mark_unavailable(120)
            self._fail_counts[source.name] = 0

    @staticmethod
    def _note_source_ok(source, counts: dict) -> None:
        counts[source.name] = 0

    def _init_sources(self):
        """初始化所有数据源"""
        source_classes = {
            "tushare": TushareSource,
            "baostock": BaostockSource,
            "easyquotation": EasyquotationSource,
            "pytdx": PytdxSource,
            "mootdx": MootdxSource,
            "westock": WestockSource,
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
              freq: str = "day", list_date: str = None) -> Optional[pd.DataFrame]:
        """获取股票数据（含自动降级和增量更新）

        Args:
            code: 统一股票代码
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD，默认今天
            freq: 频率 day/week/month
            list_date: 上市日期 YYYYMMDD（可选）——用于按真实上市日推算预期行数，
                使「数据被截断/行数差异过大」也能触发降级（否则截断数据会因
                effective_start 被缩到数据最早日而漏判）

        Returns:
            包含OHLCV和技术指标的DataFrame
        """
        if end_date is None:
            end_date = datetime.date.today().strftime("%Y%m%d")

        market = code_mapper.get_market(code)
        logger.info(f"开始获取数据: {code} ({market}), {start_date}-{end_date}, {freq}")

        # 1. 检查本地数据，确定增量起始日期（整个 fetch 只读盘一次）
        actual_start = start_date
        existing = load_existing_data(code, freq)

        latest_valid = None
        if not existing.empty:
            valid_mask, _ = validate_dataframe(existing)
            valid_rows = existing[valid_mask]
            if not valid_rows.empty:
                latest_valid = str(valid_rows["trade_date"].max())
        if latest_valid:
            # 从最新有效日期的下一天开始
            d = datetime.datetime.strptime(latest_valid, "%Y%m%d")
            next_day = (d + datetime.timedelta(days=1)).strftime("%Y%m%d")
            if next_day > start_date:
                actual_start = next_day
                logger.info(f"增量更新: 从 {actual_start} 开始 (本地数据到 {latest_valid})")

        # 1b. 本地历史完整性检查（截断/短窗口数据被误认为完整 → 全量重取）
        # 用 validate_data_completeness 按预期交易日核对行数（带上市日期），
        # 与源级降级判定同一口径（MISMATCH_TOLERANCE），避免残缺历史一直算评分
        if not existing.empty:
            market_code = "HK" if market.startswith("HK") else "A"
            check = validate_data_completeness(
                code, existing, start_date, market=market_code,
                tolerance=MISMATCH_TOLERANCE, list_date=list_date,
            )
            if not check["ok"]:
                logger.warning(f"本地数据不完整 ({code}): {check['message']}，需重新获取")
                actual_start = start_date

        if actual_start > end_date:
            logger.info(f"本地数据已是最新: {code}")
            df = existing
            had_val = "pe_ttm" in df.columns and df["pe_ttm"].notna().any()
            df = self._enrich_valuation(code, df)
            has_val = "pe_ttm" in df.columns and df["pe_ttm"].notna().any()
            # 指标已随 CSV 持久化且估值无新增时跳过重算（重算结果确定性相同）
            if {"rsi_6", "macd", "volume_ratio", "pettm_pct10y"} <= set(df.columns) and had_val == has_val:
                return df
            result = self._add_indicators(df, freq)
            save_with_indicators(code, result, freq)
            return result

        # 2. 尝试历史数据源
        new_data = None
        for source in self.historical_sources:
            if not source.supports_market(market):
                logger.debug(f"{source.name} 不支持 {market}, 跳过")
                continue
            if source.is_unavailable():
                logger.debug(f"{source.name} 熔断冷却中, 跳过")
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
                            code, data, actual_start, market=market_code,
                            tolerance=MISMATCH_TOLERANCE, list_date=list_date,
                        )
                        if not completeness["ok"] and \
                                completeness.get("missing_pct", 0) > MISMATCH_TOLERANCE * 100:
                            logger.warning(
                                f"{source.name} 数据缺失过多 "
                                f"({completeness.get('missing_pct', '?')}%), 降级下一个数据源"
                            )
                            self._note_source_fail(source)
                            continue

                    self._note_source_ok(source, self._fail_counts)
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
                # 仅异常计熔断（"无数据"可能是正常的，如当日K线未发布）
                self._note_source_fail(source)
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
                if source.is_unavailable():
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
                        self._note_source_ok(source, self._fail_counts)
                        break
                    else:
                        logger.warning(f"{source.name} 实时数据无效")
                        # 实时补充是可牺牲路径，空返回也计熔断（快速跳过挂掉的源）
                        self._note_source_fail(source)
                except Exception as e:
                    logger.error(f"{source.name} 实时异常: {e}")
                    self._note_source_fail(source)
                    continue

        # 4. 增量合并并保存
        if new_data is not None and not new_data.empty:
            merged = merge_and_save(code, new_data, freq)
        else:
            logger.warning(f"所有数据源均未获取到有效数据: {code}")
            merged = load_existing_data(code, freq)

        if merged.empty:
            return None

        merged = self._enrich_valuation(code, merged)

        # 5. 计算技术指标
        result = self._add_indicators(merged, freq)
        save_with_indicators(code, result, freq)
        return result

    def _enrich_valuation(self, code: str, df: pd.DataFrame) -> pd.DataFrame:
        """用 westock 估值基础数据（EPSTTM/NAPS）补 pe_ttm/pb，供 V1/T1 估值评分使用"""
        src = self._source_map.get("westock")
        if src is None or df is None or df.empty:
            return df
        try:
            return src.enrich_valuation(code, df)
        except Exception as e:
            logger.warning(f"估值补全失败（忽略）: {code}: {e}")
            return df

    def prefetch_history(self, codes: list, start_date: str = None,
                         end_date: str = None) -> None:
        """批量预取历史日K：让支持批量的数据源（westock）一次调用拉多只。

        各 code 起始日期按增量口径计算（本地已有数据则从最新日期次日起），
        之后 fetch() 的 fetch_daily 将命中预取缓冲。仅对 freq=day 的后续取数生效。
        """
        if not codes:
            return
        end = end_date or datetime.date.today().strftime("%Y%m%d")
        requests = {}
        for code in codes:
            actual_start = start_date or DEFAULT_START_DATE
            latest = get_latest_valid_date(code, "day")
            if latest:
                d = datetime.datetime.strptime(latest, "%Y%m%d")
                next_day = (d + datetime.timedelta(days=1)).strftime("%Y%m%d")
                if next_day > actual_start:
                    actual_start = next_day
            requests[code] = (actual_start, end)

        for source in self.historical_sources:
            try:
                source.prefetch(requests)
            except Exception as e:
                logger.warning(f"{source.name} prefetch 失败（忽略）: {e}")

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

    def fetch_live_quotes(self, codes: list) -> list:
        """直接从实时源获取最新行情，完全绕过本地 CSV 缓存。

        适用于页面高频轮询场景。A 股批量调用新浪 API（单次请求），
        港股调用 HKQuote。返回字段包含 Sina 报告的数据时间戳。

        Args:
            codes: 统一格式代码列表，如 ["000001.SZ", "600519.SH", "00700.HK"]

        Returns:
            list of dicts with keys:
                code, name, now, open, prev_close, high, low,
                volume (手), amount (元), pct_chg, change,
                data_date (YYYY-MM-DD), data_time (HH:MM:SS)
        """
        import code_mapper as _cm

        results = []
        a_pairs = []   # [(unified_code, eq_code_with_prefix), ...]
        hk_pairs = []  # [(unified_code, symbol), ...]

        for code in codes:
            market = _cm.get_market(code)
            if market in ("A_STOCK", "A_INDEX"):
                eq_code, _ = _cm.to_easyquotation(code)
                if eq_code:
                    a_pairs.append((code, eq_code))
            elif market in ("HK_STOCK", "HK_INDEX"):
                symbol, _ = _cm._split_code(code)
                hk_pairs.append((code, symbol))

        # ── A 股：批量新浪 API（单次 HTTP 请求拿所有代码）──────────────
        if a_pairs:
            eq_src = self._source_map.get("easyquotation")
            if eq_src is None:
                logger.warning("EasyquotationSource 未注册，跳过A股实时行情")
            elif not eq_src._initialized and not eq_src.init():
                logger.warning("EasyquotationSource 初始化失败，跳过A股实时行情")
            else:
                try:
                    import easyquotation as _eq_lib
                    quotation = _eq_lib.use("sina")
                    eq_codes = [eq for _, eq in a_pairs]
                    # prefix=True → 返回 key 为 "sh600519" 格式，方便匹配
                    raw = quotation.real(eq_codes, prefix=True)
                    for unified, eq_code in a_pairs:
                        info = raw.get(eq_code)
                        if not info:
                            continue
                        now_price = float(info.get("now", 0) or 0)
                        if now_price <= 0:
                            continue
                        # Sina "close" = 昨收，"now" = 当前价
                        prev_close = float(info.get("close", 0) or 0)
                        change = round(now_price - prev_close, 4) if prev_close > 0 else None
                        pct_chg = round((now_price - prev_close) / prev_close * 100, 4) if prev_close > 0 else None
                        results.append({
                            "code": unified,
                            "name": info.get("name", ""),
                            "now": now_price,
                            "open": float(info.get("open", 0) or 0),
                            "prev_close": prev_close,
                            "high": float(info.get("high", 0) or 0),
                            "low": float(info.get("low", 0) or 0),
                            "volume": float(info.get("turnover", 0) or 0) / 100.0,
                            "amount": float(info.get("volume", 0) or 0),
                            "change": change,
                            "pct_chg": pct_chg,
                            "data_date": info.get("date", ""),   # e.g. "2024-01-10"
                            "data_time": info.get("time", ""),   # e.g. "10:30:00"
                        })
                except Exception as e:
                    logger.error(f"Sina批量实时行情失败: {e}", exc_info=True)

        # ── 港股：FixedHKQuote ────────────────────────────────────────
        if hk_pairs:
            eq_src = self._source_map.get("easyquotation")
            if eq_src and (eq_src._initialized or eq_src.init()):
                try:
                    from sources.fixed_hkquote import FixedHKQuote
                    fixed_hk = FixedHKQuote()
                    symbols = [sym for _, sym in hk_pairs]
                    raw = fixed_hk.stocks(symbols)
                    for unified, symbol in hk_pairs:
                        info = (raw or {}).get(symbol)
                        if not info:
                            continue
                        price = float(info.get("price", 0) or 0)
                        if price <= 0:
                            continue
                        last_price = float(info.get("lastPrice", 0) or 0)
                        pct_chg = float(info.get("dtd", 0) or 0)
                        results.append({
                            "code": unified,
                            "name": info.get("name", ""),
                            "now": price,
                            "open": float(info.get("openPrice", 0) or 0),
                            "prev_close": last_price,
                            "high": float(info.get("high", 0) or 0),
                            "low": float(info.get("low", 0) or 0),
                            "volume": float(info.get("amount", 0) or 0) / 100.0,
                            "amount": 0.0,
                            "change": round(price - last_price, 4) if last_price > 0 else None,
                            "pct_chg": pct_chg,
                            "data_date": "",
                            "data_time": info.get("time", ""),
                        })
                except Exception as e:
                    logger.error(f"港股实时行情失败: {e}", exc_info=True)

        logger.info(f"fetch_live_quotes: {len(results)}/{len(codes)} 成功")
        return results

    def close_all(self):
        """关闭所有数据源"""
        for src in list(self._source_map.values()):
            try:
                src.close()
            except Exception:
                pass
