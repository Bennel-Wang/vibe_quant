"""
数据源模块 - 基于 data_sourcing 统一API
所有数据获取委托给 data_sourcing.DataManager，本模块仅做代码/列名适配
"""

import os
import sys
import math
import logging
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path

import pandas as pd

from .config_manager import config
from .stock_manager import stock_manager, StockInfo
from .code_converter import to_unified_code, is_sector_code, is_index_code

# 将 data_sourcing 加入搜索路径
_data_sourcing_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data_sourcing")
if _data_sourcing_dir not in sys.path:
    sys.path.insert(0, _data_sourcing_dir)

import json

from data_manager import DataManager as _DataManager
from trading_calendar import is_trading_day as _is_trading_day
from validator import validate_data_completeness as _validate_completeness

logger = logging.getLogger(__name__)


class UnifiedDataSource:
    """统一数据源接口 — 委托 data_sourcing.DataManager"""

    def __init__(self):
        self.data_dirs = config.get_data_dirs()
        self._dm: Optional[_DataManager] = None
        self._preferred_source = config.get('data_storage.history_source', 'easyquotation')
        # 惰性初始化标记
        self._init_attempted = False
        # 兼容旧代码对 tushare_available 的检查
        self.tushare_available = True
        # 上市日期缓存：{unified_code: "YYYYMMDD"}
        self._listing_dates: Dict[str, str] = {}
        self._listing_dates_path = os.path.join(
            config.get('data_storage.data_dir', './data'), 'listing_dates.json')
        self._load_listing_dates()

    def _ensure_dm(self) -> _DataManager:
        """惰性初始化 DataManager（首次调用时才连接数据源）"""
        if self._dm is None and not self._init_attempted:
            self._init_attempted = True
            try:
                self._dm = _DataManager()
                logger.info("data_sourcing.DataManager 初始化成功")
            except Exception as e:
                logger.error(f"DataManager 初始化失败: {e}")
        return self._dm

    # ------ 上市日期缓存 ------

    def _load_listing_dates(self):
        """从缓存文件加载上市日期"""
        try:
            if os.path.exists(self._listing_dates_path):
                with open(self._listing_dates_path, 'r', encoding='utf-8') as f:
                    self._listing_dates = json.load(f)
                logger.debug(f"已加载 {len(self._listing_dates)} 条上市日期缓存")
        except Exception as e:
            logger.warning(f"加载上市日期缓存失败: {e}")
            self._listing_dates = {}

    def _save_listing_dates(self):
        """保存上市日期到缓存文件"""
        try:
            os.makedirs(os.path.dirname(self._listing_dates_path), exist_ok=True)
            with open(self._listing_dates_path, 'w', encoding='utf-8') as f:
                json.dump(self._listing_dates, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"保存上市日期缓存失败: {e}")

    def get_listing_date(self, unified_code: str) -> Optional[str]:
        """获取单只股票上市日期（优先从 StockInfo，其次从缓存）"""
        # 优先从 StockInfo 中读取（用户手动维护的最准确）
        stock = stock_manager.get_stock_by_code(unified_code)
        if stock and stock.list_date:
            return stock.list_date
        # 从缓存读取
        return self._listing_dates.get(unified_code)

    def fetch_listing_dates(self, force: bool = False) -> Dict[str, str]:
        """
        从 Tushare 批量获取 A 股上市日期并更新缓存。
        返回 {unified_code: "YYYYMMDD"} 字典。

        Args:
            force: True 时忽略已有缓存强制重新拉取
        """
        if self._listing_dates and not force:
            logger.info(f"上市日期缓存已有 {len(self._listing_dates)} 条，跳过拉取（force=False）")
            return self._listing_dates

        try:
            from config import TUSHARE_TOKEN
            import tushare as ts
            if not TUSHARE_TOKEN:
                logger.warning("TUSHARE_TOKEN 未配置，无法获取上市日期")
                return self._listing_dates

            ts.set_token(TUSHARE_TOKEN)
            pro = ts.pro_api(TUSHARE_TOKEN)
            frames = []
            for status in ('L', 'D', 'P'):
                try:
                    df = pro.stock_basic(
                        exchange='', list_status=status,
                        fields='ts_code,list_date'
                    )
                    if df is not None and not df.empty:
                        frames.append(df)
                except Exception as e:
                    logger.warning(f"获取 list_status={status} 上市日期失败: {e}")

            if not frames:
                return self._listing_dates

            import pandas as pd
            all_df = pd.concat(frames, ignore_index=True)
            updated = 0
            for _, row in all_df.iterrows():
                ts_code = str(row.get('ts_code', ''))
                list_date = str(row.get('list_date', '')) if pd.notna(row.get('list_date')) else ''
                if ts_code and list_date and len(list_date) == 8:
                    self._listing_dates[ts_code] = list_date
                    updated += 1

            self._save_listing_dates()
            logger.info(f"上市日期更新完成，共 {updated} 条，已保存到缓存")
        except Exception as e:
            logger.error(f"fetch_listing_dates 异常: {e}")

        return self._listing_dates

    @staticmethod
    def _resolve_unified_code(code: str) -> str:
        """
        将任意格式的代码转为 data_sourcing 统一格式
        如果代码已包含市场信息（前缀sh/sz/hk或后缀.SH/.SZ/.HK），直接转换
        否则从 stock_manager 查找
        """
        # 代码已包含明确市场信息时直接转换（避免 stock_manager 歧义匹配）
        has_market_info = (
            code.startswith(('sh', 'sz', 'hk', 'SH', 'SZ', 'HK')) or
            '.' in code
        )
        if has_market_info:
            return to_unified_code(code)

        # 纯代码时查 stock_manager
        stock = stock_manager.get_stock_by_code(code)
        if stock:
            return stock.unified_code
        return to_unified_code(code)

    # ------ 列名适配 ------

    @staticmethod
    def _adapt_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        将 data_sourcing 返回的 DataFrame 适配为旧系统消费者期望的格式
        新API: uniformed_stock_code, trade_date, open, high, low, close, vol, ...
        旧消费者期望: code, date, open, high, low, close, volume, ...
        """
        if df is None or df.empty:
            return df

        df = df.copy()

        # 添加兼容列
        if 'trade_date' in df.columns:
            df['date'] = df['trade_date']
        if 'vol' in df.columns:
            df['volume'] = df['vol']
        if 'uniformed_stock_code' in df.columns:
            df['code'] = df['uniformed_stock_code']

        return df

    # ------ 交易日过滤 ------

    @staticmethod
    def _filter_trading_days(df: pd.DataFrame, unified_code: str) -> pd.DataFrame:
        """移除非交易日（周末/假日）的数据行"""
        if df is None or df.empty or 'trade_date' not in df.columns:
            return df
        market = "HK" if unified_code.endswith(".HK") else "A"
        dates = pd.to_datetime(df['trade_date'], format='%Y%m%d', errors='coerce')
        mask = dates.apply(lambda d: _is_trading_day(d.date(), market) if pd.notna(d) else False)
        removed = (~mask).sum()
        if removed > 0:
            logger.info(f"过滤 {unified_code} 非交易日数据 {removed} 行")
        return df[mask].reset_index(drop=True)

    # ------ 主接口 ------

    @property
    def preferred_source(self):
        return self._preferred_source

    @preferred_source.setter
    def preferred_source(self, value):
        if value in ('easyquotation', 'tushare'):
            self._preferred_source = value
            logger.info(f"数据源已切换为: {value}")

    def get_historical_data(self, code: str, start_date: Optional[str] = None,
                            end_date: Optional[str] = None,
                            freq: str = "day",
                            adjust: bool = True) -> pd.DataFrame:
        """
        获取历史数据（统一接口）

        Args:
            code: 股票代码（支持 sh600519 / 600519.SH / 600519 等多种格式）
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD（默认今天）
            freq: 频率 day/week/month
            adjust: 保留参数兼容（data_sourcing 内部处理）

        Returns:
            标准化后的 DataFrame，包含 date/volume 等兼容列
        """
        dm = self._ensure_dm()
        if dm is None:
            logger.error("DataManager 不可用，无法获取数据")
            return pd.DataFrame()

        unified = self._resolve_unified_code(code)
        if not start_date:
            start_date = config.get('data_collection.history.start_date', '20030101')
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')

        # 清洗日期格式
        start_date = str(start_date).replace('-', '').replace('/', '')[:8]
        end_date = str(end_date).replace('-', '').replace('/', '')[:8]

        try:
            df = dm.fetch(unified, start_date, end_date, freq=freq)
        except Exception as e:
            logger.error(f"DataManager.fetch 失败 ({unified}): {e}")
            return pd.DataFrame()

        if df is None or df.empty:
            logger.warning(f"未获取到数据: {code} ({unified})")
            return pd.DataFrame()

        # 过滤非交易日数据（周末/假日可能被实时源错误写入）
        df = self._filter_trading_days(df, unified)

        # 数据完整性校验
        market = "HK" if unified.endswith(".HK") else "A"
        cfg_start = start_date or config.get('data_collection.history.start_date', '20030101')
        _validate_completeness(unified, df, cfg_start, market=market, logger=logger)

        return self._adapt_columns(df)

    def get_realtime_data(self, codes: Optional[list] = None, adjust: bool = True) -> pd.DataFrame:
        """获取实时/最新数据（通过 data_sourcing 获取近期数据取最后一条）"""
        dm = self._ensure_dm()
        if dm is None:
            return pd.DataFrame()

        if codes is None:
            all_stocks = stock_manager.get_all_stocks()
            codes = [s.unified_code for s in all_stocks if s.type != 'sector']
        else:
            codes = [self._resolve_unified_code(c) for c in codes]

        # 跳过板块代码（无数据源支持）
        codes = [c for c in codes if not is_sector_code(c.split('.')[0])]

        # 取最近7天以覆盖周末/假期
        from datetime import timedelta
        end_d = datetime.now()
        start_d = end_d - timedelta(days=7)
        start_date = start_d.strftime('%Y%m%d')
        end_date = end_d.strftime('%Y%m%d')

        rows = []
        for idx, uc in enumerate(codes):
            try:
                df = dm.fetch(uc, start_date, end_date, freq="day")
                if df is not None and not df.empty:
                    # 过滤非交易日
                    df = self._filter_trading_days(df, uc)
                    if df.empty:
                        continue

                    last = df.iloc[-1].to_dict()
                    # 使用统一后缀格式（如 000001.SH）作为代码标识
                    last['code'] = uc
                    # 映射字段以兼容 web_app 消费者（期望 now/price/volume）
                    last['now'] = last.get('close')
                    last['price'] = last.get('close')
                    if 'vol' in last:
                        last['volume'] = last['vol']

                    # 用前一交易日收盘价作为 prev_close，使涨跌额/涨跌幅正确计算
                    if len(df) >= 2:
                        prev_close = df.iloc[-2]['close']
                        last['lastPrice'] = prev_close
                        cur = last.get('close')
                        if cur is not None and prev_close is not None:
                            try:
                                c, p = float(cur), float(prev_close)
                                if p > 0:
                                    last['change'] = round(c - p, 4)
                                    last['pct_chg'] = round((c - p) / p * 100, 4)
                            except (ValueError, TypeError):
                                pass

                    # 清理 NaN 值为 None（避免 JSON 序列化 NaN 导致前端显示异常）
                    last = {k: (None if isinstance(v, float) and math.isnan(v) else v)
                            for k, v in last.items()}
                    rows.append(last)
            except Exception as e:
                logger.debug(f"实时数据获取失败 {uc}: {e}")
            # 反爬虫：每次请求间短暂延迟
            if idx < len(codes) - 1:
                import time as _time
                _time.sleep(0.5)

        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def merge_realtime_data(self, live_data):
        """兼容旧接口 — data_sourcing 已在 fetch 中自动合并实时数据"""
        logger.debug("merge_realtime_data: data_sourcing 已自动处理实时数据合并")
        return True

    def update_all_data(self, refresh: bool = False):
        """更新所有股票的历史数据（从 start_date 到最新，含完整性校验）"""
        dm = self._ensure_dm()
        if dm is None:
            logger.warning("DataManager 不可用，跳过批量更新")
            return

        start_date = config.get('data_collection.history.start_date', '20030101')
        stocks = stock_manager.get_all_stocks()
        results = {'ok': 0, 'warn': 0, 'fail': 0, 'skip': 0, 'details': []}

        for stock in stocks:
            if stock.type == 'sector' or is_sector_code(stock.code):
                logger.info(f"跳过板块代码: {stock.name} ({stock.code})")
                results['skip'] += 1
                continue
            try:
                unified = stock.unified_code
                logger.info(f"更新数据: {stock.name} ({unified})")
                df = dm.fetch(unified, start_date, freq="day")

                if df is not None and not df.empty:
                    # 过滤非交易日
                    df = self._filter_trading_days(df, unified)
                    # 完整性校验
                    market = "HK" if unified.endswith(".HK") else "A"
                    vr = _validate_completeness(unified, df, start_date,
                                                market=market, logger=logger)
                    results['details'].append(vr)
                    if vr['ok']:
                        results['ok'] += 1
                    else:
                        results['warn'] += 1
                else:
                    logger.warning(f"未获取到数据: {stock.name} ({unified})")
                    results['fail'] += 1

                logger.info(f"已更新 {stock.name}({unified})")
            except Exception as e:
                logger.error(f"更新 {stock.name} 失败: {e}")
                results['fail'] += 1

        logger.info(
            f"数据更新完成: 成功 {results['ok']}, 警告 {results['warn']}, "
            f"失败 {results['fail']}, 跳过 {results['skip']}"
        )
        return results

    @staticmethod
    def _resample(df, rule):
        """将日线数据重采样为周线或月线"""
        if df.empty:
            return df
        df = df.copy()
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
        agg_dict = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
        agg_dict = {k: v for k, v in agg_dict.items() if k in df.columns}
        try:
            result = df.resample(rule).agg(agg_dict)
        except ValueError:
            result = df.resample(rule + 'S').agg(agg_dict)
        return result.dropna().reset_index()

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化列名（兼容旧调用）"""
        return self._adapt_columns(df)

    def _standardize_realtime_data(self, code: str, data: Dict) -> Dict:
        """标准化实时数据"""
        return {
            'code': code,
            'name': data.get('name', ''),
            'price': data.get('now', data.get('close', 0)),
            'open': data.get('open', 0),
            'high': data.get('high', 0),
            'low': data.get('low', 0),
            'close': data.get('close', 0),
            'volume': data.get('volume', data.get('vol', 0)),
            'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

    def close_all(self):
        """关闭所有数据源"""
        if self._dm is not None:
            try:
                self._dm.close_all()
            except Exception:
                pass

    def validate_all_data(self) -> list:
        """校验所有股票的数据完整性，返回校验结果列表"""
        results = []
        start_date = config.get('data_collection.history.start_date', '20030101')
        stocks = stock_manager.get_all_stocks()

        for stock in stocks:
            if stock.type == 'sector' or is_sector_code(stock.code):
                continue
            try:
                unified = stock.unified_code
                market = "HK" if unified.endswith(".HK") else "A"
                # 获取上市日期（StockInfo > 本地缓存）
                list_date = self.get_listing_date(unified)
                df = self.get_historical_data(stock.code)
                if df is not None and not df.empty:
                    r = _validate_completeness(unified, df, start_date,
                                               market=market, logger=logger,
                                               list_date=list_date)
                else:
                    r = {"code": unified, "ok": False, "message": f"{unified}: 无数据"}
                    logger.error(r["message"])
                results.append(r)
            except Exception as e:
                results.append({"code": stock.code, "ok": False,
                                "message": f"{stock.code}: 校验异常 {e}"})
                logger.error(f"校验 {stock.name} 失败: {e}")

        # 汇总
        ok_count = sum(1 for r in results if r.get("ok"))
        fail_count = len(results) - ok_count
        logger.info(f"数据完整性校验完成: {ok_count}/{len(results)} 通过, {fail_count} 异常")
        return results


# 全局统一数据源实例
unified_data = UnifiedDataSource()
