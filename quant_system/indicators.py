"""
全新的技术指标计算模块
从零开始，确保每个计算步骤都清晰正确
"""

import os
import logging
from typing import List, Dict, Optional, Union, Tuple
from pathlib import Path

import pandas as pd
import numpy as np

from .config_manager import config
from .data_source import unified_data
from .stock_manager import stock_manager

logger = logging.getLogger(__name__)


class FreshTechnicalIndicators:
    """全新设计的技术指标计算器"""
    
    def __init__(self):
        # 统一使用 data_sourcing/data/ 目录，与 data_manager 共享同一份 CSV
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                      'data_sourcing', 'data')
        self.rsi_config = config.get_rsi_config()
        self._ensure_dir()
    
    def _ensure_dir(self):
        """确保数据目录存在"""
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
    
    def _validate_input_data(self, df: pd.DataFrame) -> bool:
        """验证输入数据质量"""
        if df.empty:
            logger.error("输入数据为空")
            return False
            
        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"缺少必要列: {missing_columns}")
            return False
            
        # 检查数据类型
        try:
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            if df['close'].isna().all():
                logger.error("收盘价数据全部为NaN")
                return False
        except Exception as e:
            logger.error(f"数据类型转换失败: {e}")
            return False
            
        return True
    
    def calculate_simple_ma(self, prices: pd.Series, period: int) -> pd.Series:
        """
        简单移动平均线 - 从零开始实现
        确保每一步都正确
        """
        logger.info(f"计算{period}日简单移动平均线")
        
        if len(prices) < period:
            logger.warning(f"数据长度({len(prices)})小于计算周期({period})")
            # 返回NaN填充
            return pd.Series([np.nan] * len(prices), index=prices.index)
        
        # 使用pandas内置的rolling方法，这是最可靠的
        ma = prices.rolling(window=period, min_periods=1).mean()
        
        logger.info(f"MA{period}计算完成，范围: {ma.min():.2f} - {ma.max():.2f}")
        return ma
    
    def calculate_simple_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """
        简单RSI计算 - 从零开始实现
        """
        logger.info(f"计算{period}日RSI")
        
        if len(prices) < period + 1:
            logger.warning(f"数据长度不足计算RSI，需要{period + 1}个数据点")
            return pd.Series([np.nan] * len(prices), index=prices.index)
        
        # 计算价格变化
        delta = prices.diff()
        
        # 分离上涨和下跌
        gains = delta.where(delta > 0, 0.0)
        losses = -delta.where(delta < 0, 0.0)
        
        # 使用Wilder平滑法（alpha = 1/period）
        avg_gains = gains.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
        avg_losses = losses.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
        
        # 计算RS
        rs = avg_gains / avg_losses.replace(0, 1e-10)  # 避免除零
        
        # 计算RSI
        rsi = 100 - (100 / (1 + rs))
        
        # 处理特殊情况
        rsi = rsi.clip(0, 100)  # 确保在0-100范围内
        
        logger.info(f"RSI{period}计算完成，范围: {rsi.min():.2f} - {rsi.max():.2f}")
        return rsi
    
    def calculate_fresh_indicators(self, code: str, start_date: str, end_date: str, 
                                 freq: str = "day") -> pd.DataFrame:
        """
        全新指标计算方法
        一步一步，确保每个环节都正确
        """
        logger.info(f"开始全新指标计算: {code}, {start_date} to {end_date}")
        
        # 1. 获取原始数据
        df = unified_data.get_historical_data(code, start_date, end_date, freq)
        if df.empty:
            logger.error(f"无法获取{code}的历史数据")
            return df
            
        # 2. 数据预处理和验证
        if not self._validate_input_data(df):
            return pd.DataFrame()
            
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        logger.info(f"获取到{len(df)}条数据，日期范围: {df['date'].min()} 到 {df['date'].max()}")
        
        # 3. 确保数值类型正确
        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 4. 计算移动平均线
        ma_periods = [5, 20, 60]
        for period in ma_periods:
            df[f'ma_{period}'] = self.calculate_simple_ma(df['close'], period)
            logger.info(f"MA{period}前5个值: {df[f'ma_{period}'].head().tolist()}")
        
        # 5. 计算RSI指标
        rsi_periods = [6, 12, 24]
        for period in rsi_periods:
            df[f'rsi_{period}'] = self.calculate_simple_rsi(df['close'], period)
            logger.info(f"RSI{period}前5个值: {df[f'rsi_{period}'].head().tolist()}")
        
        # 6. 计算MACD
        exp1 = df['close'].ewm(span=12, adjust=False).mean()
        exp2 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = exp1 - exp2
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['macd_histogram'] = df['macd'] - df['macd_signal']
        logger.info(f"MACD前5个值: {df['macd'].head().tolist()}")

        # 7. 添加基本的其他指标
        df['change_pct'] = df['close'].pct_change() * 100
        df['volatility_20'] = df['close'].pct_change().rolling(20).std() * np.sqrt(252)
        
        logger.info("全新指标计算完成")
        return df


# 从备份文件恢复原有的TechnicalIndicators类

class TechnicalIndicators:
    """技术指标计算器"""
    
    def __init__(self):
        # 统一使用 data_sourcing/data/ 目录，与 data_manager 共享同一份 CSV
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                      'data_sourcing', 'data')
        self.rsi_config = config.get_rsi_config()
        self._ensure_dir()
    
    def _ensure_dir(self):
        """确保数据目录存在"""
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
    
    def _get_indicator_path(self, code: str, indicator: str) -> str:
        """获取指标存储路径（使用 data_sourcing 统一命名格式）
        
        data_sourcing 命名格式: {unified_code.replace('.','_')}_{freq}.csv
        例如: 600519_SH_day.csv, 000001_SZ_day.csv
        """
        # 从 indicator 字符串中提取 freq (如 "indicators_day" → "day")
        freq = "day"
        if "_" in indicator:
            parts = indicator.split("_")
            if parts[-1] in ("day", "week", "month"):
                freq = parts[-1]
        
        # 解析统一代码
        unified = self._resolve_to_unified(code)
        safe_code = unified.replace(".", "_")
        return os.path.join(self.data_dir, f"{safe_code}_{freq}.csv")
    
    def _resolve_to_unified(self, code: str) -> str:
        """将任意格式代码转为 data_sourcing 统一格式（如 600519.SH）"""
        # 已经是统一格式
        if "." in code and code.split(".")[-1].upper() in ("SH", "SZ", "HK"):
            return code
        
        # 尝试通过 stock_manager 查找
        stock = stock_manager.get_stock_by_code(code)
        if stock:
            return stock.unified_code
        
        # 猜测市场
        pure = code.lstrip("shszSHSZhkHK") if code[:2].lower() in ("sh", "sz", "hk") else code
        if not pure:
            pure = code
        if pure.startswith("6") or pure.startswith("5"):
            return f"{pure}.SH"
        elif pure.startswith(("0", "3")):
            return f"{pure}.SZ"
        elif pure.startswith("8") or pure.startswith("9"):
            return f"{pure}.SH"
        return f"{pure}.SH"
    
    def calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """
        计算RSI指标（使用Wilder平滑法）
        """
        delta = prices.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)
        
        # Wilder's EMA: alpha = 1/period
        avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
        
        rs = avg_gain / avg_loss.replace(0, 1e-10)
        rsi = 100 - (100 / (1 + rs))
        return rsi.clip(0, 100)
    
    def calculate_ma(self, prices: pd.Series, periods: List[int] = None) -> Dict[int, pd.Series]:
        """
        计算移动平均线
        """
        if periods is None:
            periods = config.get('technical_indicators.ma.periods', [5, 10, 20, 60, 120, 250])
        
        mas = {}
        for period in periods:
            mas[period] = prices.rolling(window=period, min_periods=1).mean()
        
        return mas
    
    def calculate_all_indicators_from_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        从DataFrame计算所有技术指标
        用于重采样后的数据计算
        """
        if df.empty:
            logger.warning("输入数据为空")
            return df
        
        # 确保数据类型正确
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df['high'] = pd.to_numeric(df['high'], errors='coerce')
        df['low'] = pd.to_numeric(df['low'], errors='coerce')
        df['open'] = pd.to_numeric(df['open'], errors='coerce')
        
        # 移除无效数据
        df = df.dropna(subset=['close', 'high', 'low', 'open'])
        
        if df.empty:
            return df
        
        # 计算各类指标
        # 1. RSI
        rsi_periods = [6, 12, 24]
        for period in rsi_periods:
            df[f'rsi_{period}'] = self.calculate_rsi(df['close'], period)
        
        # 2. 移动平均线
        ma_result = self.calculate_ma(df['close'], [5, 10, 20, 60, 120, 250])
        for period, ma_series in ma_result.items():
            df[f'ma_{period}'] = ma_series
        
        # 3. MACD
        exp1 = df['close'].ewm(span=12, adjust=False).mean()
        exp2 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = exp1 - exp2
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['macd_histogram'] = df['macd'] - df['macd_signal']
        
        # 4. 布林带
        df['boll_middle'] = df['close'].rolling(window=20, min_periods=1).mean()
        boll_std = df['close'].rolling(window=20, min_periods=1).std()
        df['boll_upper'] = df['boll_middle'] + (boll_std * 2)
        df['boll_lower'] = df['boll_middle'] - (boll_std * 2)
        
        # 5. KDJ
        lowest_low = df['low'].rolling(window=9, min_periods=1).min()
        highest_high = df['high'].rolling(window=9, min_periods=1).max()
        rsv = (df['close'] - lowest_low) / (highest_high - lowest_low) * 100
        df['kdj_k'] = rsv.ewm(alpha=1/3, adjust=False).mean()
        df['kdj_d'] = df['kdj_k'].ewm(alpha=1/3, adjust=False).mean()
        df['kdj_j'] = 3 * df['kdj_k'] - 2 * df['kdj_d']
        
        # 6. 波动率
        df['volatility'] = df['close'].pct_change().rolling(20, min_periods=1).std() * np.sqrt(252)
        
        # 7. 成交量指标
        df['volume_ma_5'] = df['volume'].rolling(window=5, min_periods=1).mean()
        df['volume_ma_20'] = df['volume'].rolling(window=20, min_periods=1).mean()
        df['volume_ratio'] = df['volume'] / df['volume_ma_20']
        
        # 8. Williams %R
        df['wr_14'] = self.calculate_wr(df, 14)

        # 9. Bollinger 相对位置 (0=下轨, 1=上轨)
        boll_range = df['boll_upper'] - df['boll_lower']
        df['boll_position'] = (
            (df['close'] - df['boll_lower']) / boll_range.replace(0, np.nan)
        ).clip(0, 1).fillna(0.5)

        # 10. 综合评分（简化版）
        _score = pd.Series(0.0, index=df.index)
        _score += (df['rsi_6'].between(30, 70)).astype(float) * 10
        _score += df['macd_histogram'].fillna(0) * 10
        _score += ((df['close'] > df['ma_20']).astype(float) * 2 - 1) * 10
        _score += (df['kdj_j'].fillna(50) - 50) * 0.3
        df['overall_score'] = _score.clip(-100, 100).round(2)

        # 11. RSI百分位 (100周期滚动百分位秩)
        # rsi6_pct100 = 过去100天中RSI_6低于当前值的比例 * 100
        # 低值(如5) = RSI处于历史低位（超卖）
        if 'rsi_6' in df.columns:
            rsi_arr = df['rsi_6'].values.astype(float)
            pct_rsi = np.full(len(rsi_arr), np.nan)
            for i in range(1, len(rsi_arr)):
                window_start = max(0, i - 100)
                past = rsi_arr[window_start:i]
                valid = past[~np.isnan(past)]
                if len(valid) == 0:
                    continue
                cur = rsi_arr[i]
                if np.isnan(cur):
                    continue
                pct_rsi[i] = round(float((valid <= cur).sum()) / len(valid) * 100, 1)
            df['rsi6_pct100'] = pct_rsi

        # 12. PE_TTM百分位 (历史全量百分位秩，使用有序插入O(n log n))
        # pettm_pct10y = 历史所有正PE中低于当前PE的比例 * 100
        # 低值(如20) = PE处于历史低位（估值便宜）
        if 'pe_ttm' in df.columns:
            import bisect
            pe_arr = df['pe_ttm'].values.astype(float)
            pct_pe = np.full(len(pe_arr), np.nan)
            history_sorted = []
            for i in range(len(pe_arr)):
                cur_pe = pe_arr[i]
                if i > 0 and not np.isnan(cur_pe) and cur_pe > 0 and len(history_sorted) > 0:
                    pos = bisect.bisect_right(history_sorted, cur_pe)
                    pct_pe[i] = round(pos / len(history_sorted) * 100, 1)
                if not np.isnan(cur_pe) and cur_pe > 0:
                    bisect.insort(history_sorted, cur_pe)
            df['pettm_pct10y'] = pct_pe

        return df
    
    def calculate_wr(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Williams %R indicator"""
        high = df['high'].rolling(window=period).max()
        low = df['low'].rolling(window=period).min()
        wr = (high - df['close']) / (high - low) * (-100)
        return wr.fillna(-50)
    
    def calculate_major_cost(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Estimate major holder average cost using:
        1. VWAP (Volume-Weighted Average Price) over multiple windows
        2. Turnover-based cost (cost when cumulative turnover reaches 100-150%)
        """
        if df is None or df.empty:
            return df
        
        df = df.copy()
        typical_price = (df['high'] + df['low'] + df['close']) / 3.0
        
        # 1. VWAP for different windows
        for p in [20, 60, 120]:
            col = f'major_cost_{p}'
            if 'volume' in df.columns:
                pv = (typical_price * df['volume']).rolling(window=p, min_periods=1).sum()
                vol = df['volume'].rolling(window=p, min_periods=1).sum()
                df[col] = (pv / vol.replace(0, float('nan'))).round(2)
            else:
                df[col] = typical_price.rolling(window=p, min_periods=1).mean().round(2)
        
        # 2. Turnover-based major cost (use 60-day cumulative turnover proxy)
        if 'volume' in df.columns:
            total_60d_vol = df['volume'].rolling(60, min_periods=1).sum()
            pv_60d = (typical_price * df['volume']).rolling(60, min_periods=1).sum()
            df['major_cost_turnover'] = (pv_60d / total_60d_vol.replace(0, float('nan'))).round(2)
        else:
            df['major_cost_turnover'] = df['major_cost_60']
        
        return df
    
    def calculate_chip_distribution(self, df: pd.DataFrame, n_bins: int = 60) -> Dict:
        """
        Calculate chip distribution using chip movement algorithm.
        
        This simulates how shares (chips) accumulate at different price levels
        using daily OHLCV data. Each day, a portion of chips (based on turnover rate)
        moves to new price levels.
        """
        if df is None or df.empty or len(df) < 10:
            return {}
        
        try:
            import numpy as np
            
            # Use recent ~250 trading days (1 year)
            data = df.tail(250).copy().reset_index(drop=True)
            
            current_price = float(data['close'].iloc[-1])
            all_high = float(data['high'].max())
            all_low  = float(data['low'].min())
            
            if all_high <= all_low:
                return {}
            
            price_range = all_high - all_low
            bins = np.linspace(all_low, all_high, n_bins + 1)
            bin_centers = (bins[:-1] + bins[1:]) / 2
            chip_dist = np.zeros(n_bins)  # total chips at each price level
            
            # Get total shares (use volume sum as proxy for float shares)
            total_vol = float(data['volume'].sum())
            if total_vol <= 0:
                return {}
            
            for i, row in data.iterrows():
                try:
                    day_high = float(row['high'])
                    day_low  = float(row['low'])
                    day_vol  = float(row['volume']) if pd.notna(row['volume']) else 0
                    
                    if day_vol <= 0 or day_high <= day_low:
                        continue
                    
                    # Turnover rate for this day (proportion of total chips that traded)
                    turnover = day_vol / total_vol
                    turnover = min(turnover, 1.0)  # cap at 100%
                    
                    # Chips leaving: reduce existing chips proportionally
                    chip_dist *= (1 - turnover)
                    
                    # New chips arriving: triangular distribution over today's range
                    # Peak at midpoint, tapering to zero at high and low
                    for j, center in enumerate(bin_centers):
                        if day_low <= center <= day_high:
                            # Triangular distribution: peak at midpoint of range
                            mid = (day_high + day_low) / 2.0
                            span = (day_high - day_low) / 2.0
                            weight = max(0.0, 1.0 - abs(center - mid) / span) if span > 0 else 1.0
                            chip_dist[j] += day_vol * turnover * weight
                except Exception:
                    continue
            
            # Normalize so total = 100%
            chip_total = chip_dist.sum()
            if chip_total > 0:
                chip_pct = (chip_dist / chip_total * 100).tolist()
            else:
                chip_pct = [0.0] * n_bins
            
            # Find profit ratio: chips at price <= current_price
            profit_chip = chip_dist[bin_centers <= current_price].sum()
            profit_ratio = float(profit_chip / chip_total * 100) if chip_total > 0 else 0.0
            
            # Find main cost zone (highest density range)
            peak_idx = int(np.argmax(chip_dist))
            
            return {
                'price_bins': [round(float(x), 2) for x in bin_centers.tolist()],
                'distribution': [round(float(x), 4) for x in chip_pct],
                'current_price': round(current_price, 2),
                'profit_ratio': round(profit_ratio, 2),
                'main_cost_zone': {
                    'low':  round(float(bin_centers[max(0, peak_idx-2)]), 2),
                    'peak': round(float(bin_centers[peak_idx]), 2),
                    'high': round(float(bin_centers[min(n_bins-1, peak_idx+2)]), 2),
                }
            }
        except Exception as e:
            logger.error(f"calculate_chip_distribution error: {e}")
            return {}
    
    def save_indicators(self, code: str, df: pd.DataFrame, freq: str = "day"):
        """保存指标到CSV文件"""
        if df.empty:
            return
        
        path = self._get_indicator_path(code, f"indicators_{freq}")
        df.to_csv(path, index=False, encoding='utf-8')
        logger.info(f"指标已保存到: {path}")
    
    def load_indicators(self, code: str, freq: str = "day") -> pd.DataFrame:
        """从CSV文件加载指标"""
        path = self._get_indicator_path(code, f"indicators_{freq}")
        
        if os.path.exists(path):
            try:
                df = pd.read_csv(path, encoding='utf-8')
                # data_sourcing 日线的 date 列是 int 格式 (20030102)，需转为标准日期字符串
                if 'date' in df.columns and df['date'].dtype in ('int64', 'float64'):
                    df['date'] = pd.to_datetime(df['date'].astype(int).astype(str), format='%Y%m%d').dt.strftime('%Y-%m-%d')
                logger.info(f"成功加载指标文件: {path}")
                return df
            except Exception as e:
                logger.warning(f"加载指标文件失败 {path}: {e}")
        
        return pd.DataFrame()

    def calculate_all_indicators(self, code: str, start_date: str = None, end_date: str = None, freq: str = 'day') -> pd.DataFrame:
        """
        兼容旧接口：根据代码与日期范围获取历史数据并计算全部指标
        """
        logger.info(f"calculate_all_indicators called for {code} ({start_date} - {end_date}), freq={freq}")
        # 获取历史数据（使用 unified_data）
        try:
            df = unified_data.get_historical_data(code, start_date or '', end_date or '', freq=freq, adjust=True)
        except TypeError:
            # Some unified_data implementations may not accept adjust argument
            df = unified_data.get_historical_data(code, start_date or '', end_date or '', freq=freq)
        except Exception as e:
            logger.exception(f"获取历史数据失败: {e}")
            return pd.DataFrame()
        
        if df.empty:
            logger.warning(f"calculate_all_indicators: 未获取到数据 for {code}")
            return pd.DataFrame()
        
        # 标准化日期列
        try:
            if df['date'].dtype == 'object':
                df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d', errors='coerce')
            else:
                df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
        except Exception as e:
            logger.exception(f"日期标准化失败: {e}")
            return pd.DataFrame()
        
        # 调用现有的 from_df 计算函数
        try:
            ind_df = self.calculate_all_indicators_from_df(df)
            return ind_df
        except Exception as e:
            logger.exception(f"从DataFrame计算指标失败: {e}")
            return pd.DataFrame()


class IndicatorAnalyzer:
    """指标分析器"""
    
    def __init__(self):
        self.calculator = TechnicalIndicators()
    
    def get_latest_signals(self, code: str, freq: str = "day") -> Dict:
        """
        获取最新信号，包含日线、周线、月线及新闻情感指标
        """
        df = self.calculator.load_indicators(code, freq)
        
        if df.empty:
            df = self.calculator.calculate_all_indicators(code, freq=freq)
        
        if df.empty:
            return {}
        
        latest = df.iloc[-1]
        
        signals = {
            'code': code,
            'date': latest.get('date', ''),
            'price': latest.get('close', 0),
            'close': latest.get('close', 0),
            'open': latest.get('open', 0),
            'high': latest.get('high', 0),
            'low': latest.get('low', 0),
            'volume': latest.get('volume', 0),
            'amount': latest.get('amount', 0),
            'pct_chg': latest.get('pct_chg', 0),
            'change': latest.get('change', 0),
            'pe_ttm': latest.get('pe_ttm', 0),
            'pb': latest.get('pb', 0),
            # RSI
            'rsi_6': latest.get('rsi_6', 0),
            'rsi_12': latest.get('rsi_12', 0),
            'rsi_24': latest.get('rsi_24', 0),
            'rsi_signal': self._interpret_rsi(latest.get('rsi_6', 50)),
            # MACD
            'macd': latest.get('macd', 0),
            'macd_signal': latest.get('macd_signal', 0),
            'macd_histogram': latest.get('macd_histogram', 0),
            'macd_trend': 'up' if latest.get('macd_histogram', 0) > 0 else 'down',
            # 均线 (同时提供 ma_5 和 ma5 两种写法)
            'ma_5': latest.get('ma_5', 0),   'ma5': latest.get('ma_5', 0),
            'ma_10': latest.get('ma_10', 0), 'ma10': latest.get('ma_10', 0),
            'ma_20': latest.get('ma_20', 0), 'ma20': latest.get('ma_20', 0),
            'ma_60': latest.get('ma_60', 0), 'ma60': latest.get('ma_60', 0),
            'ma_120': latest.get('ma_120', 0), 'ma120': latest.get('ma_120', 0),
            'ma_250': latest.get('ma_250', 0), 'ma250': latest.get('ma_250', 0),
            'ma_trend': self._interpret_ma_trend(latest),
            # 布林带
            'boll_upper': latest.get('boll_upper', 0),
            'boll_middle': latest.get('boll_middle', 0),
            'boll_lower': latest.get('boll_lower', 0),
            'boll_position': latest.get('boll_position', 0.5),
            # KDJ
            'kdj_k': latest.get('kdj_k', 50),
            'kdj_d': latest.get('kdj_d', 50),
            'kdj_j': latest.get('kdj_j', 50),
            # 成交量
            'volume_ratio': latest.get('volume_ratio', 1),
            'volume_ma_5': latest.get('volume_ma_5', 0),
            'volume_ma_20': latest.get('volume_ma_20', 0),
            # 其他
            'wr_14': latest.get('wr_14', -50),
            'volatility': latest.get('volatility', 0),
            'overall_score': self._calculate_overall_score(latest),
        }

        # ---- 附加周线指标（前缀 w_）----
        _indicator_cols = [
            'close', 'open', 'high', 'low', 'volume', 'amount',
            'rsi_6', 'rsi_12', 'rsi_24',
            'macd', 'macd_signal', 'macd_histogram',
            'kdj_k', 'kdj_d', 'kdj_j',
            'boll_upper', 'boll_middle', 'boll_lower', 'boll_position',
            'ma_5', 'ma_20', 'ma_60',
            'volume_ratio', 'volume_ma_5', 'volume_ma_20',
            'wr_14', 'volatility',
        ]
        _defaults = {'rsi_6': 50, 'rsi_12': 50, 'rsi_24': 50,
                     'kdj_k': 50, 'kdj_d': 50, 'kdj_j': 50,
                     'boll_position': 0.5, 'volume_ratio': 1, 'wr_14': -50}
        for _prefix, _freq in (('w_', 'week'), ('m_', 'month')):
            try:
                _df = self.calculator.load_indicators(code, _freq)
                if _df.empty:
                    _df = self.calculator.calculate_all_indicators(code, freq=_freq)
                if not _df.empty:
                    _row = _df.iloc[-1]
                    for _col in _indicator_cols:
                        signals[f'{_prefix}{_col}'] = _row.get(_col, _defaults.get(_col, 0))
                    signals[f'{_prefix}overall_score'] = self._calculate_overall_score(_row)
            except Exception as _e:
                logger.debug(f"加载{_freq}线指标失败({code}): {_e}")

        # ---- 附加新闻情感指标 ----
        try:
            from .news_collector import sentiment_analyzer, NewsCollector
            _news_collector = NewsCollector()
            _news_df = _news_collector.load_news(code)
            if not _news_df.empty:
                _analyzed = sentiment_analyzer.analyze_news_df(_news_df.tail(20))
                if 'sentiment_score' in _analyzed.columns:
                    signals['news_sentiment'] = float(_analyzed['sentiment_score'].mean())
                    signals['news_count'] = int(len(_analyzed))
                    signals['news_positive'] = float(
                        (_analyzed['sentiment_score'] > 0.2).sum() / max(len(_analyzed), 1)
                    )
        except Exception as _e:
            logger.debug(f"加载新闻情感指标失败({code}): {_e}")
            signals.setdefault('news_sentiment', 0)
            signals.setdefault('news_count', 0)
            signals.setdefault('news_positive', 0.5)

        return signals
    
    def _interpret_rsi(self, rsi: float) -> str:
        """解读RSI信号"""
        if rsi > 70:
            return "超买"
        elif rsi < 30:
            return "超卖"
        elif rsi > 50:
            return "偏强"
        else:
            return "偏弱"
    
    def _interpret_ma_trend(self, row) -> str:
        """解读均线趋势"""
        close = row.get('close', 0)
        ma5 = row.get('ma_5', 0)
        ma20 = row.get('ma_20', 0)
        ma60 = row.get('ma_60', 0)
        
        if close > ma5 > ma20 > ma60:
            return "多头排列"
        elif close < ma5 < ma20 < ma60:
            return "空头排列"
        elif close > ma20:
            return "强势"
        else:
            return "弱势"
    
    def _calculate_overall_score(self, row) -> float:
        """计算综合评分"""
        score = 0
        
        # RSI评分
        rsi6 = row.get('rsi_6', 50)
        if 30 <= rsi6 <= 70:
            score += 10
        
        # MACD评分
        macd_hist = row.get('macd_histogram', 0)
        score += macd_hist * 10
        
        # 均线评分
        close = row.get('close', 0)
        ma20 = row.get('ma_20', 0)
        if close > ma20:
            score += 10
        else:
            score -= 10
        
        # KDJ评分
        j = row.get('kdj_j', 50)
        score += (j - 50) * 0.3
        
        return round(max(-100, min(100, score)), 2)
    
    def generate_report(self, code: str) -> str:
        """
        生成技术指标报告
        """
        stock = stock_manager.get_stock_by_code(code)
        name = stock.name if stock else code
        
        signals = self.get_latest_signals(code)
        
        if not signals:
            return f"无法获取 {name}({code}) 的技术指标"
        
        report = f"""
=== {name}({code}) 技术指标报告 ===
日期: {signals['date']}
当前价格: {signals['price']}

【RSI指标】
- RSI(6): {signals['rsi_6']:.2f} ({signals['rsi_signal']})
- RSI(12): {signals['rsi_12']:.2f}
- RSI(24): {signals['rsi_24']:.2f}

【MACD指标】
- MACD: {signals['macd']:.4f}
- 信号线: {signals['macd_signal']:.4f}
- 柱状图: {signals['macd_histogram']:.4f}
- 趋势: {signals['macd_trend']}

【均线排列】
- 趋势: {signals['ma_trend']}

【KDJ指标】
- K: {signals['kdj_k']:.2f}
- D: {signals['kdj_d']:.2f}
- J: {signals['kdj_j']:.2f}

【布林带位置】
- 相对位置: {signals['boll_position']:.2%}

【综合评分】
- 评分: {signals['overall_score']}
- 建议: {"看多" if signals['overall_score'] > 20 else ("看空" if signals['overall_score'] < -20 else "观望")}
"""
        return report


# 全局实例
technical_indicators = TechnicalIndicators()
indicator_analyzer = IndicatorAnalyzer()
fresh_technical_indicators = FreshTechnicalIndicators()