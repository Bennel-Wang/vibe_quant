"""
数据清洗模块
数据补缺、复权处理、数据对齐
"""

import os
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np

from .config_manager import config
from .stock_manager import stock_manager

logger = logging.getLogger(__name__)


class DataCleaner:
    """数据清洗器"""
    
    def __init__(self):
        self.data_dirs = config.get_data_dirs()
    
    def check_data_integrity(self, df: pd.DataFrame, required_cols: List[str] = None) -> Dict:
        """
        检查数据完整性
        
        Args:
            df: 数据DataFrame
            required_cols: 必需列
        
        Returns:
            完整性报告
        """
        if required_cols is None:
            required_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
        
        report = {
            'total_rows': len(df),
            'missing_cols': [],
            'missing_values': {},
            'duplicate_dates': 0,
            'date_gaps': [],
            'is_valid': True,
        }
        
        # 检查必需列
        for col in required_cols:
            if col not in df.columns:
                report['missing_cols'].append(col)
                report['is_valid'] = False
        
        # 检查缺失值
        for col in df.columns:
            missing_count = df[col].isnull().sum()
            if missing_count > 0:
                report['missing_values'][col] = missing_count
        
        # 检查重复日期
        if 'date' in df.columns:
            report['duplicate_dates'] = df['date'].duplicated().sum()
            if report['duplicate_dates'] > 0:
                report['is_valid'] = False
        
        # 检查日期断层
        if 'date' in df.columns and len(df) > 1:
            df_sorted = df.sort_values('date')
            df_sorted['date'] = pd.to_datetime(df_sorted['date'])
            
            # 找出缺失的交易日（简化版，未考虑节假日）
            date_diff = df_sorted['date'].diff().dt.days
            gaps = date_diff[date_diff > 5]  # 超过5天的间隔认为是断层
            
            if not gaps.empty:
                report['date_gaps'] = gaps.tolist()
        
        return report
    
    def fill_missing_values(self, df: pd.DataFrame, method: str = "ffill") -> pd.DataFrame:
        """
        填充缺失值
        
        Args:
            df: 数据DataFrame
            method: 填充方法 (ffill/bfill/interpolate)
        
        Returns:
            填充后的DataFrame
        """
        df = df.copy()
        
        # 价格数据使用前向填充
        price_cols = ['open', 'high', 'low', 'close']
        for col in price_cols:
            if col in df.columns:
                if method == "ffill":
                    df[col] = df[col].fillna(method='ffill')
                elif method == "bfill":
                    df[col] = df[col].fillna(method='bfill')
                elif method == "interpolate":
                    df[col] = df[col].interpolate()
        
        # 成交量使用0填充
        if 'volume' in df.columns:
            df['volume'] = df['volume'].fillna(0)
        
        # 金额使用0填充
        if 'amount' in df.columns:
            df['amount'] = df['amount'].fillna(0)
        
        return df
    
    def remove_duplicates(self, df: pd.DataFrame, subset: List[str] = None) -> pd.DataFrame:
        """
        移除重复数据
        
        Args:
            df: 数据DataFrame
            subset: 用于判断重复的列
        
        Returns:
            去重后的DataFrame
        """
        if subset is None:
            subset = ['date']
        
        before_count = len(df)
        df = df.drop_duplicates(subset=subset, keep='last')
        after_count = len(df)
        
        removed = before_count - after_count
        if removed > 0:
            logger.info(f"移除了 {removed} 条重复数据")
        
        return df
    
    def adjust_prices(self, df: pd.DataFrame, 
                      adjustment_factor: Optional[pd.Series] = None) -> pd.DataFrame:
        """
        价格复权处理
        
        Args:
            df: 数据DataFrame
            adjustment_factor: 复权因子，如果为None则使用Tushare获取
        
        Returns:
            复权后的DataFrame
        """
        df = df.copy()
        
        if adjustment_factor is not None:
            # 使用前复权
            price_cols = ['open', 'high', 'low', 'close']
            for col in price_cols:
                if col in df.columns:
                    df[f'{col}_adj'] = df[col] * adjustment_factor
        
        return df
    
    def align_data(self, data_dict: Dict[str, pd.DataFrame], 
                   date_col: str = 'date') -> Dict[str, pd.DataFrame]:
        """
        多股票数据对齐
        
        Args:
            data_dict: 股票代码到DataFrame的字典
            date_col: 日期列名
        
        Returns:
            对齐后的数据字典
        """
        # 获取所有日期
        all_dates = set()
        for df in data_dict.values():
            if date_col in df.columns:
                all_dates.update(df[date_col].tolist())
        
        all_dates = sorted(all_dates)
        
        # 对齐每个DataFrame
        aligned_dict = {}
        for code, df in data_dict.items():
            if date_col not in df.columns:
                aligned_dict[code] = df
                continue
            
            # 创建完整的日期索引
            df_aligned = pd.DataFrame({date_col: all_dates})
            df_aligned[date_col] = pd.to_datetime(df_aligned[date_col])
            
            # 合并数据
            df[date_col] = pd.to_datetime(df[date_col])
            df_aligned = df_aligned.merge(df, on=date_col, how='left')
            
            # 填充缺失值
            df_aligned = self.fill_missing_values(df_aligned)
            
            aligned_dict[code] = df_aligned
        
        return aligned_dict
    
    def detect_outliers(self, df: pd.DataFrame, 
                        columns: List[str] = None,
                        method: str = "iqr",
                        threshold: float = 3.0) -> pd.DataFrame:
        """
        检测异常值
        
        Args:
            df: 数据DataFrame
            columns: 要检测的列
            method: 检测方法 (iqr/zscore)
            threshold: 阈值
        
        Returns:
            异常值标记DataFrame
        """
        if columns is None:
            columns = ['open', 'high', 'low', 'close', 'volume']
        
        outliers = pd.DataFrame(index=df.index)
        
        for col in columns:
            if col not in df.columns:
                continue
            
            if method == "iqr":
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower = Q1 - 1.5 * IQR
                upper = Q3 + 1.5 * IQR
                outliers[col] = (df[col] < lower) | (df[col] > upper)
            
            elif method == "zscore":
                zscore = np.abs((df[col] - df[col].mean()) / df[col].std())
                outliers[col] = zscore > threshold
        
        return outliers
    
    def clean_data(self, df: pd.DataFrame, 
                   required_cols: List[str] = None,
                   remove_outliers: bool = False) -> pd.DataFrame:
        """
        执行完整的数据清洗流程
        
        Args:
            df: 原始数据
            required_cols: 必需列
            remove_outliers: 是否移除异常值
        
        Returns:
            清洗后的数据
        """
        if df.empty:
            return df
        
        # 1. 检查完整性
        integrity = self.check_data_integrity(df, required_cols)
        
        if not integrity['is_valid']:
            logger.warning(f"数据完整性检查失败: {integrity}")
        
        # 2. 移除重复
        df = self.remove_duplicates(df)
        
        # 3. 排序
        if 'date' in df.columns:
            df = df.sort_values('date')
        
        # 4. 填充缺失值
        df = self.fill_missing_values(df)
        
        # 5. 检测并处理异常值
        if remove_outliers:
            outliers = self.detect_outliers(df)
            # 这里可以选择用中位数替换异常值
            for col in outliers.columns:
                median = df[col].median()
                df.loc[outliers[col], col] = median
        
        return df
    
    def validate_data_consistency(self, df: pd.DataFrame) -> Dict:
        """
        验证数据一致性
        
        Args:
            df: 数据DataFrame
        
        Returns:
            验证报告
        """
        report = {
            'ohlc_consistency': True,
            'price_jump': [],
            'zero_volume_days': 0,
            'errors': [],
        }
        
        if df.empty:
            return report
        
        # 检查OHLC一致性
        if all(col in df.columns for col in ['open', 'high', 'low', 'close']):
            # high应该 >= low
            invalid_hl = df[df['high'] < df['low']]
            if not invalid_hl.empty:
                report['ohlc_consistency'] = False
                report['errors'].append(f"{len(invalid_hl)} 条记录 high < low")
            
            # high应该 >= open, close
            invalid_high = df[(df['high'] < df['open']) | (df['high'] < df['close'])]
            if not invalid_high.empty:
                report['ohlc_consistency'] = False
                report['errors'].append(f"{len(invalid_high)} 条记录 high < open 或 high < close")
            
            # low应该 <= open, close
            invalid_low = df[(df['low'] > df['open']) | (df['low'] > df['close'])]
            if not invalid_low.empty:
                report['ohlc_consistency'] = False
                report['errors'].append(f"{len(invalid_low)} 条记录 low > open 或 low > close")
        
        # 检查价格跳空
        if 'close' in df.columns:
            price_change = df['close'].pct_change().abs()
            jumps = price_change[price_change > 0.2]  # 超过20%的涨跌幅
            if not jumps.empty:
                report['price_jump'] = jumps.index.tolist()
        
        # 检查零成交量
        if 'volume' in df.columns:
            report['zero_volume_days'] = (df['volume'] == 0).sum()
        
        return report
    
    def generate_cleaning_report(self, code: str, 
                                  before_df: pd.DataFrame,
                                  after_df: pd.DataFrame) -> str:
        """
        生成清洗报告
        
        Args:
            code: 股票代码
            before_df: 清洗前数据
            after_df: 清洗后数据
        
        Returns:
            报告文本
        """
        stock = stock_manager.get_stock_by_code(code)
        name = stock.name if stock else code
        
        before_integrity = self.check_data_integrity(before_df)
        after_integrity = self.check_data_integrity(after_df)
        consistency = self.validate_data_consistency(after_df)
        
        report = f"""
=== {name}({code}) 数据清洗报告 ===

【清洗前】
- 总记录数: {before_integrity['total_rows']}
- 缺失列: {before_integrity['missing_cols']}
- 缺失值: {before_integrity['missing_values']}
- 重复日期: {before_integrity['duplicate_dates']}

【清洗后】
- 总记录数: {after_integrity['total_rows']}
- 缺失列: {after_integrity['missing_cols']}
- 缺失值: {after_integrity['missing_values']}
- 重复日期: {after_integrity['duplicate_dates']}

【一致性检查】
- OHLC一致性: {"通过" if consistency['ohlc_consistency'] else "失败"}
- 价格跳空: {len(consistency['price_jump'])} 处
- 零成交量天数: {consistency['zero_volume_days']}
"""
        
        if consistency['errors']:
            report += "\n【错误详情】\n"
            for error in consistency['errors']:
                report += f"- {error}\n"
        
        return report


class DataValidator:
    """数据验证器"""
    
    def __init__(self):
        self.cleaner = DataCleaner()
    
    def validate_all_data(self) -> Dict[str, Dict]:
        """
        验证所有股票数据
        
        Returns:
            验证结果字典
        """
        results = {}
        
        stocks = stock_manager.get_all_stocks()
        
        for stock in stocks:
            code = stock.code
            
            # 加载历史数据
            stock_info = stock
            history_path = os.path.join(
                config.get('data_storage.history_dir'),
                f"{stock_info.storage_code}_daily.csv"
            )
            
            if not os.path.exists(history_path):
                results[code] = {'status': 'missing', 'error': '数据文件不存在'}
                continue
            
            try:
                df = pd.read_csv(history_path)
                
                # 检查完整性
                integrity = self.cleaner.check_data_integrity(df)
                
                # 检查一致性
                consistency = self.cleaner.validate_data_consistency(df)
                
                results[code] = {
                    'status': 'valid' if integrity['is_valid'] and consistency['ohlc_consistency'] else 'invalid',
                    'integrity': integrity,
                    'consistency': consistency,
                }
                
            except Exception as e:
                results[code] = {'status': 'error', 'error': str(e)}
        
        return results


# 全局实例
data_cleaner = DataCleaner()
data_validator = DataValidator()
