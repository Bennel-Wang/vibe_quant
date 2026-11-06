"""
Westock 数据源 - 历史数据（腾讯自选股 westock CLI，经 westock_api 调用）

支持: A股个股/指数、港股个股/指数
K线为延迟数据（非盘中实时），A股个股默认前复权(qfq)。
后端有突发频控，限流时指数退避重试，仍失败则短熔断降级。
"""
import time
from typing import Optional

import pandas as pd

from sources.base_source import BaseSource
import code_mapper
from westock_api.client import find_westock, run_westock


def _to_yyyymmdd(d: str) -> str:
    return d.replace("-", "").replace("/", "")[:8]


def _to_iso(d: str) -> str:
    d = _to_yyyymmdd(d)
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}"


# 指数 → 跟踪ETF 种子映射（已验证 trackIndexCode；未收录的运行时发现）
_INDEX_ETF_SEED = {
    "000300": "sh510300",   # 华泰柏瑞沪深300ETF
}


class WestockSource(BaseSource):
    def __init__(self):
        super().__init__("westock")
        self._prefetch: dict = {}  # code -> (start, end, df)
        self._valuation_cache: dict = {}  # westock_code -> {yyyymmdd: (epsttm, naps)}
        self._index_etf_cache: dict = {}  # 指数 symbol -> ETF westock_code | None

    def init(self) -> bool:
        if find_westock() is None:
            self.logger.error("Westock CLI 未安装（运行 skill 的 scripts/setup.cjs 或设置 WESTOCK_BIN）")
            return False
        self._initialized = True
        self.logger.info("Westock 数据源初始化成功")
        return True

    def supports_market(self, market: str) -> bool:
        return market in ("A_STOCK", "A_INDEX", "HK_STOCK", "HK_INDEX")

    def is_realtime(self) -> bool:
        return False

    # ── 批量预取：一次 westock 调用拉多只股票，供后续 fetch_daily 命中 ──────

    def prefetch(self, requests: dict) -> None:
        """requests: {code: (start_date, end_date)}，内部合并为批量 kline 调用。

        A股个股与其它市场分组调用，保证 A股走前复权（qfq）与单查口径一致。
        """
        if not self._initialized and not self.init():
            return
        if self.is_unavailable():
            return
        pending = {
            code: (s, e) for code, (s, e) in requests.items()
            if code not in self._prefetch and code_mapper.to_westock(code)
        }
        if not pending:
            return

        groups = {True: [], False: []}  # qfq_needed -> [code]
        for code in pending:
            qfq = code_mapper.get_market(code) == "A_STOCK"
            groups[qfq].append(code)

        for qfq, codes in groups.items():
            if not codes:
                continue
            start = min(_to_yyyymmdd(pending[c][0]) for c in codes)
            end = max(_to_yyyymmdd(pending[c][1]) for c in codes)
            # 一次调用最多 30 只，避免请求过大
            for i in range(0, len(codes), 30):
                chunk = codes[i:i + 30]
                wcodes = [code_mapper.to_westock(c) for c in chunk]
                rows = self._fetch_kline(",".join(wcodes), start, end, qfq=qfq)
                if rows is not None:
                    for code in chunk:
                        wc = code_mapper.to_westock(code)
                        sub = rows[rows["westock_code"] == wc] if "westock_code" in rows.columns else rows
                        if not sub.empty:
                            s, e = pending[code]
                            self._prefetch[code] = (s, e, self._to_unified(code, sub))
                # 估值基础数据（EPSTTM/NAPS）批量预取，供 enrich_valuation 使用
                try:
                    self._fetch_valuation_batch(wcodes)
                except Exception as e:
                    self.logger.warning(f"Westock finance 预取失败（忽略）: {e}")

    def _to_unified(self, code: str, rows: pd.DataFrame) -> pd.DataFrame:
        """westock 行 → 统一列格式（与 tushare_source 输出对齐）"""
        result = pd.DataFrame()
        result["uniformed_stock_code"] = [code] * len(rows)
        result["trade_date"] = rows["date"].astype(str).str.replace("-", "", regex=False)
        for src, dst in [("open", "open"), ("high", "high"), ("low", "low"),
                         ("last", "close"), ("volume", "vol")]:
            result[dst] = pd.to_numeric(rows[src], errors="coerce")
        result["pe_ttm"] = float("nan")
        result["pb"] = float("nan")
        return result.sort_values("trade_date").reset_index(drop=True)

    def _fetch_kline(self, westock_codes: str, start: str, end: str,
                     qfq: bool = False) -> Optional[pd.DataFrame]:
        """调用 westock kline，返回原始行 DataFrame（含 westock_code 列）。

        限流时指数退避重试（3s/8s），仍被限流则短熔断 30s 并返回 None（降级下一源）。
        """
        args = ["kline", westock_codes, "--period", "day",
                "--start", _to_iso(start), "--end", _to_iso(end),
                # 范围模式默认只回 2000 条，放宽上限以覆盖全历史
                "--limit", "10000"]
        # 仅 A股个股做前复权（指数/港股 westock 强制不复权）
        if qfq:
            args += ["--fq", "qfq"]

        env = None
        for attempt, backoff in enumerate((0, 3, 8)):
            if backoff:
                self.logger.warning(f"Westock 限流，{backoff}s 后重试 ({attempt + 1}/3): {westock_codes}")
                time.sleep(backoff)
            env = run_westock(args)
            if env["ok"] or "限流" not in (env.get("error") or ""):
                break

        if env is None or not env["ok"]:
            err = (env or {}).get("error") or "未知错误"
            if "限流" in err:
                self.logger.warning(f"Westock 限流重试耗尽: {westock_codes}")
                self.mark_unavailable(30)
            else:
                self.logger.warning(f"Westock 查询失败({err}): {westock_codes} ({start}-{end})")
            return None

        tables = env.get("data", {}).get("tables") or []
        frames = []
        for t in tables:
            if t["rows"]:
                frames.append(pd.DataFrame(t["rows"]))
        if not frames:
            self.logger.warning(f"Westock 无数据: {westock_codes} ({start}-{end})")
            return None

        df = pd.concat(frames, ignore_index=True)
        if "code" in df.columns:
            df = df.rename(columns={"code": "westock_code"})
        else:
            df["westock_code"] = westock_codes
        # 统一列名：单代码输出无 code 列且 close 列叫 last
        if "last" not in df.columns and "close" in df.columns:
            df = df.rename(columns={"close": "last"})
        return df

    # ── 估值补全：westock finance（EPSTTM/NAPS）→ 日线 pe_ttm/pb ──────────

    @staticmethod
    def _parse_finance_tables(tables: list, default_code: str = None) -> dict:
        """解析 finance 输出：{westock_code: {yyyymmdd: (epsttm, naps)}}"""
        eps_map: dict = {}
        naps_map: dict = {}
        for t in tables:
            title = t.get("title") or ""
            for row in t.get("rows") or []:
                end = row.get("EndDate")
                if not end:
                    continue
                d = str(end).replace("-", "")[:8]
                wc = str(row.get("code") or default_code or "")
                if not wc:
                    continue
                eps_map.setdefault(wc, {})
                naps_map.setdefault(wc, {})
                if "利润" in title:
                    v = row.get("EPSTTM")
                    if v is not None:
                        eps_map[wc][d] = float(v)
                elif "资产负债" in title:
                    v = row.get("NAPS")
                    if v is not None:
                        naps_map[wc][d] = float(v)
        result: dict = {}
        for wc in set(eps_map) | set(naps_map):
            result[wc] = {
                d: (eps_map.get(wc, {}).get(d), naps_map.get(wc, {}).get(d))
                for d in set(eps_map.get(wc, {})) | set(naps_map.get(wc, {}))
            }
        return result

    def _fetch_valuation_batch(self, westock_codes: list) -> None:
        """批量拉取 finance 估值基础数据（40 个报告期 ≈ 10 年），写入 _valuation_cache"""
        env = run_westock(["finance", ",".join(westock_codes), "--limit", "40"])
        if not env["ok"]:
            return
        tables = env.get("data", {}).get("tables") or []
        # 单代码输出无 code 列，需回填归属
        default_code = westock_codes[0] if len(westock_codes) == 1 else None
        for wc, series in self._parse_finance_tables(tables, default_code=default_code).items():
            if series:
                self._valuation_cache[wc] = series

    # ── 指数估值：跟踪ETF 的 etf overview（peTtm/pb + 历史百分位）──────────

    def find_index_etf(self, code: str) -> Optional[str]:
        """找到跟踪该指数的 ETF（westock 码），带缓存。找不到返回 None。"""
        symbol, _ = code_mapper._split_code(code)
        if symbol in self._index_etf_cache:
            return self._index_etf_cache[symbol]

        etf = _INDEX_ETF_SEED.get(symbol)
        if etf is None and self._initialized:
            # 运行时发现：指数名 → 搜 ETF（关键词按 ETF 命名习惯降级）→ 校验 trackIndexCode
            wc = code_mapper.to_westock(code)
            env = run_westock(["profile", wc]) if wc else None
            name = None
            for t in (env or {}).get("data", {}).get("tables") or []:
                for row in t.get("rows") or []:
                    name = row.get("name") or name
            if name:
                # ETF 名一般不含“指/指数”尾缀（如 创业板指 → 创业板ETF）
                keywords = [name]
                for kw in (name[:-2] if name.endswith("指数") else None,
                           name[:-1] if name.endswith("指") else None,
                           name[: len(name) - 1], name[: len(name) - 2]):
                    if kw and len(kw) >= 2 and kw not in keywords:
                        keywords.append(kw)
                cands = []
                for kw in keywords[:4]:
                    if len(cands) >= 5:
                        break
                    senv = run_westock(["search", kw, "--type", "etf"])
                    for t in (senv or {}).get("data", {}).get("tables") or []:
                        for row in t.get("rows") or []:
                            if row.get("code") and row.get("code") not in cands:
                                # 仅 etfDetail=支持 的可用 etf profile 校验
                                if row.get("etfDetail") in (None, "支持"):
                                    cands.append(row["code"])
                for cand in cands[:5]:
                    penv = run_westock(["etf", "profile", cand])
                    for t in (penv or {}).get("data", {}).get("tables") or []:
                        for row in t.get("rows") or []:
                            if str(row.get("trackIndexCode") or "") == symbol:
                                etf = cand
                                break
                        if etf:
                            break
                    if etf:
                        break
                if etf:
                    self.logger.info(f"指数 {code} 跟踪ETF发现: {etf}")

        self._index_etf_cache[symbol] = etf
        return etf

    def _enrich_index_valuation(self, code: str, df: pd.DataFrame) -> pd.DataFrame:
        """指数估值自算（最节约路径）：

        1 次 etf overview 拿跟踪指数的 peTtm/pb 及其历史百分位（peTtmPct/pbPct）；
        历史 PE/PB 序列按价格比例法还原（盈利视作常数：pe_t = PE_now × P_t / P_now），
        并把来源自带的百分位直写 pe_hist_direct/pb_hist_direct 供评分引擎优先使用。
        """
        if not self._initialized and not self.init():
            return df
        need_pe = "pe_ttm" not in df.columns or df["pe_ttm"].isna().any()
        need_pb = "pb" not in df.columns or df["pb"].isna().any()
        need_hist = ("pe_hist_direct" not in df.columns
                     or df["pe_hist_direct"].isna().all())
        if not (need_pe or need_pb or need_hist):
            return df

        etf = self.find_index_etf(code)
        if etf is None:
            return df
        env = run_westock(["etf", "overview", etf])
        if not env["ok"]:
            return df
        row = None
        for t in env.get("data", {}).get("tables") or []:
            if t.get("rows"):
                row = t["rows"][0]
                break
        if not row:
            return df

        pe_now = row.get("peTtm")
        pb_now = row.get("pb")
        pe_pct = row.get("peTtmPct")
        pb_pct = row.get("pbPct")

        closes = pd.to_numeric(df["close"], errors="coerce")
        last_close = closes.iloc[-1] if len(closes) else None

        if need_pe and pe_now and last_close == last_close and last_close > 0:
            pe_series = float(pe_now) * closes / last_close
            df["pe_ttm"] = pe_series if "pe_ttm" not in df.columns else df["pe_ttm"].fillna(pe_series)
        if need_pb and pb_now and last_close == last_close and last_close > 0:
            pb_series = float(pb_now) * closes / last_close
            df["pb"] = pb_series if "pb" not in df.columns else df["pb"].fillna(pb_series)
        if need_hist and isinstance(pe_pct, (int, float)):
            df["pe_hist_direct"] = float(pe_pct) / 100.0
        if isinstance(pb_pct, (int, float)):
            df["pb_hist_direct"] = float(pb_pct) / 100.0

        self.logger.info(
            f"指数估值自算: {code} via {etf} pe={pe_now} pb={pb_now} "
            f"pe分位={pe_pct}% pb分位={pb_pct}%"
        )
        return df

    def enrich_valuation(self, code: str, df: pd.DataFrame) -> pd.DataFrame:
        """用 EPSTTM/NAPS 按报告期映射填充 df 的 pe_ttm/pb（仅补 NaN 行）。

        pe_ttm = close / EPSTTM（亏损时为负，语义与 tushare 一致）；
        pb = close / NAPS。指数等无三大表的标的原样返回。
        """
        if df is None or df.empty or "close" not in df.columns:
            return df
        market = code_mapper.get_market(code)
        if market in ("A_INDEX", "HK_INDEX"):
            return self._enrich_index_valuation(code, df)
        if market not in ("A_STOCK", "HK_STOCK"):
            return df

        need_pe = "pe_ttm" not in df.columns or df["pe_ttm"].isna().any()
        need_pb = "pb" not in df.columns or df["pb"].isna().any()
        if not need_pe and not need_pb:
            return df

        wc = code_mapper.to_westock(code)
        if wc is None:
            return df
        series = self._valuation_cache.get(wc)
        if series is None:
            if not self._initialized and not self.init():
                return df
            self._fetch_valuation_batch([wc])
            series = self._valuation_cache.get(wc) or {}
            self._valuation_cache[wc] = series
        if not series:
            return df

        periods = sorted(series.items())  # [(yyyymmdd, (epsttm, naps)), ...]
        dates = df["trade_date"].astype(str).str.replace("-", "", regex=False)
        closes = pd.to_numeric(df["close"], errors="coerce")

        eps_vals, naps_vals = [], []
        pi, eps, naps = 0, float("nan"), float("nan")
        for d in dates:
            while pi < len(periods) and periods[pi][0] <= d:
                eps, naps = periods[pi][1]
                pi += 1
            eps_vals.append(eps)
            naps_vals.append(naps)

        if need_pe:
            pe = [
                (c / e) if (c == c and e is not None and e == e and e != 0) else float("nan")
                for c, e in zip(closes, eps_vals)
            ]
            pe_series = pd.Series(pe, index=df.index)
            df["pe_ttm"] = pe_series if "pe_ttm" not in df.columns else df["pe_ttm"].fillna(pe_series)
        if need_pb:
            pb = [
                (c / na) if (c == c and na is not None and na == na and na > 0) else float("nan")
                for c, na in zip(closes, naps_vals)
            ]
            pb_series = pd.Series(pb, index=df.index)
            df["pb"] = pb_series if "pb" not in df.columns else df["pb"].fillna(pb_series)
        return df

    def fetch_listing_date(self, code: str) -> Optional[str]:
        """westock profile → 上市日期 YYYYMMDD（完整性校验/截断判定用）"""
        wc = code_mapper.to_westock(code)
        if wc is None:
            return None
        env = run_westock(["profile", wc])
        if not env["ok"]:
            return None
        for t in env.get("data", {}).get("tables") or []:
            for row in t.get("rows") or []:
                ld = row.get("listedDate")
                if ld:
                    s = str(ld).replace("-", "")[:8]
                    return s if len(s) == 8 else None
        return None

    # ── 单代码取数 ─────────────────────────────────────────────────────────

    def fetch_daily(self, code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        if not self._initialized and not self.init():
            return None
        if self.is_unavailable():
            return None
        if not self.supports_market(code_mapper.get_market(code)):
            return None

        # 命中预取缓冲（单次消费）
        cached = self._prefetch.pop(code, None)
        if cached is not None:
            p_start, p_end, df = cached
            if _to_yyyymmdd(p_start) <= _to_yyyymmdd(start_date) and \
               _to_yyyymmdd(p_end) >= _to_yyyymmdd(end_date):
                return df

        wc = code_mapper.to_westock(code)
        if wc is None:
            return None

        rows = self._fetch_kline(
            wc, start_date, end_date,
            qfq=code_mapper.get_market(code) == "A_STOCK",
        )
        if rows is None or rows.empty:
            return None

        result = self._to_unified(code, rows)
        s, e = _to_yyyymmdd(start_date), _to_yyyymmdd(end_date)
        result = result[(result["trade_date"] >= s) & (result["trade_date"] <= e)]
        self.logger.info(f"Westock 日K获取成功: {code}, {len(result)} 条")
        return result.reset_index(drop=True) if not result.empty else None
