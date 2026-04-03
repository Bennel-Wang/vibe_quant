"""
股票自动分类模块
根据行业、波动率、Beta等特征自动分类为: 成长型/防御型/周期价值型
"""

import logging
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass
from enum import Enum

import pandas as pd
import numpy as np

from .data_source import unified_data
from .stock_manager import stock_manager

logger = logging.getLogger(__name__)


class StockCategory(Enum):
    """股票类型枚举"""
    GROWTH = 'growth'          # 成长型 (科技、新能源、半导体等)
    DEFENSIVE = 'defensive'    # 防御型 (公用事业、消费、银行等)
    CYCLICAL = 'cyclical'      # 周期/价值型 (化工、地产、有色等)

    @property
    def label(self) -> str:
        return {
            'growth': '成长型',
            'defensive': '防御型',
            'cyclical': '周期价值型',
        }[self.value]

    @property
    def emoji(self) -> str:
        return {
            'growth': '🚀',
            'defensive': '🛡️',
            'cyclical': '🔄',
        }[self.value]


# 行业 → 类型映射 (基于A股行业特征)
INDUSTRY_CATEGORY_MAP = {
    # 成长型: 科技、新能源、医药创新、高端制造
    '半导体': StockCategory.GROWTH,
    '电气设备': StockCategory.GROWTH,
    '通信设备': StockCategory.GROWTH,
    '元器件': StockCategory.GROWTH,
    '软件服务': StockCategory.GROWTH,
    '互联网': StockCategory.GROWTH,
    'IT设备': StockCategory.GROWTH,
    '计算机应用': StockCategory.GROWTH,
    '电子元件': StockCategory.GROWTH,
    '光学光电子': StockCategory.GROWTH,
    '汽车整车': StockCategory.GROWTH,
    '汽车配件': StockCategory.GROWTH,
    '医疗器械': StockCategory.GROWTH,
    '化学制药': StockCategory.GROWTH,
    '生物制品': StockCategory.GROWTH,
    '专用机械': StockCategory.GROWTH,
    '航天航空': StockCategory.GROWTH,
    '仪器仪表': StockCategory.GROWTH,

    # 防御型: 公用事业、消费日用、金融
    '水力发电': StockCategory.DEFENSIVE,
    '火力发电': StockCategory.DEFENSIVE,
    '电力': StockCategory.DEFENSIVE,
    '银行': StockCategory.DEFENSIVE,
    '保险': StockCategory.DEFENSIVE,
    '白酒': StockCategory.DEFENSIVE,
    '食品饮料': StockCategory.DEFENSIVE,
    '乳品': StockCategory.DEFENSIVE,
    '调味品': StockCategory.DEFENSIVE,
    '中药': StockCategory.DEFENSIVE,
    '医药商业': StockCategory.DEFENSIVE,
    '燃气': StockCategory.DEFENSIVE,
    '供水供气': StockCategory.DEFENSIVE,
    '环保工程': StockCategory.DEFENSIVE,
    '高速公路': StockCategory.DEFENSIVE,
    '铁路运输': StockCategory.DEFENSIVE,
    '机场': StockCategory.DEFENSIVE,
    '港口': StockCategory.DEFENSIVE,
    '零售': StockCategory.DEFENSIVE,
    '商业百货': StockCategory.DEFENSIVE,

    # 周期/价值型: 化工、地产、有色、建材
    '化工原料': StockCategory.CYCLICAL,
    '基础化学': StockCategory.CYCLICAL,
    '钢铁': StockCategory.CYCLICAL,
    '煤炭开采': StockCategory.CYCLICAL,
    '石油开采': StockCategory.CYCLICAL,
    '有色金属': StockCategory.CYCLICAL,
    '水泥': StockCategory.CYCLICAL,
    '全国地产': StockCategory.CYCLICAL,
    '区域地产': StockCategory.CYCLICAL,
    '建筑材料': StockCategory.CYCLICAL,
    '工程机械': StockCategory.CYCLICAL,
    '船舶制造': StockCategory.CYCLICAL,
    '纺织': StockCategory.CYCLICAL,
    '家居用品': StockCategory.CYCLICAL,
    '造纸': StockCategory.CYCLICAL,
    '农林牧渔': StockCategory.CYCLICAL,
    '证券': StockCategory.CYCLICAL,
    '多元金融': StockCategory.CYCLICAL,
}


@dataclass
class StockClassification:
    """股票分类结果"""
    code: str
    name: str
    category: StockCategory
    industry: str
    method: str              # 分类方法: industry/volatility/default
    volatility_20d: float    # 20日波动率
    beta: float              # 相对大盘的Beta
    detail: str

    def to_dict(self) -> Dict:
        return {
            'code': self.code,
            'name': self.name,
            'category': self.category.value,
            'category_label': self.category.label,
            'category_emoji': self.category.emoji,
            'industry': self.industry,
            'method': self.method,
            'volatility_20d': self.volatility_20d,
            'beta': self.beta,
            'detail': self.detail,
        }


class StockClassifier:
    """股票自动分类器
    
    分类优先级:
    1. 行业映射 (最可靠, 基于A股行业经验)
    2. 波动率+Beta (当行业未知时的备选)
       - 高波动高Beta → 成长型
       - 低波动低Beta → 防御型
       - 中等 → 周期价值型
    """

    def classify(self, code: str) -> StockClassification:
        """分类单只股票"""
        stock = stock_manager.get_stock_by_code(code)
        name = stock.name if stock else code
        industry = stock.industry if stock else ''

        vol_20d = 0.0
        beta = 1.0

        # 尝试计算波动率和Beta
        try:
            vol_20d, beta = self._calc_vol_beta(code)
        except Exception as e:
            logger.debug(f"计算波动率/Beta失败 {code}: {e}")

        # 方法1: 行业映射
        if industry and industry in INDUSTRY_CATEGORY_MAP:
            category = INDUSTRY_CATEGORY_MAP[industry]
            return StockClassification(
                code=code, name=name, category=category,
                industry=industry, method='industry',
                volatility_20d=round(vol_20d, 4), beta=round(beta, 2),
                detail=f"行业[{industry}]→{category.label}",
            )

        # 方法2: 波动率+Beta
        if vol_20d > 0:
            if vol_20d > 0.03 and beta > 1.2:
                category = StockCategory.GROWTH
                method_detail = f"高波动({vol_20d:.3f})+高Beta({beta:.2f})→成长型"
            elif vol_20d < 0.02 and beta < 0.8:
                category = StockCategory.DEFENSIVE
                method_detail = f"低波动({vol_20d:.3f})+低Beta({beta:.2f})→防御型"
            else:
                category = StockCategory.CYCLICAL
                method_detail = f"中等波动({vol_20d:.3f})+Beta({beta:.2f})→周期价值型"

            return StockClassification(
                code=code, name=name, category=category,
                industry=industry or '未知', method='volatility',
                volatility_20d=round(vol_20d, 4), beta=round(beta, 2),
                detail=method_detail,
            )

        # 默认: 周期价值型
        return StockClassification(
            code=code, name=name, category=StockCategory.CYCLICAL,
            industry=industry or '未知', method='default',
            volatility_20d=0.0, beta=1.0,
            detail='数据不足，默认归为周期价值型',
        )

    def classify_all(self) -> Dict[str, StockClassification]:
        """分类所有监控中的股票"""
        results = {}
        stocks = stock_manager.get_stocks()
        for stock in stocks:
            try:
                code = stock.full_code
                result = self.classify(code)
                results[code] = result
            except Exception as e:
                logger.debug(f"分类失败 {stock.code}: {e}")
        return results

    def get_stocks_by_category(self, category: StockCategory) -> List[StockClassification]:
        """获取某一类型的所有股票"""
        all_classified = self.classify_all()
        return [c for c in all_classified.values() if c.category == category]

    def _calc_vol_beta(self, code: str) -> Tuple[float, float]:
        """计算20日波动率和Beta"""
        from datetime import datetime, timedelta

        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=120)).strftime('%Y%m%d')

        df = unified_data.get_historical_data(code, start_date, end_date)
        if df is None or df.empty or len(df) < 20:
            return 0.0, 1.0

        df['ret'] = df['close'].pct_change()
        vol_20d = float(df['ret'].tail(20).std())

        # 计算Beta
        try:
            idx_df = unified_data.get_historical_data('000001.SH', start_date, end_date)
            if idx_df is not None and not idx_df.empty:
                idx_df['idx_ret'] = idx_df['close'].pct_change()
                merged = pd.merge(
                    df[['date', 'ret']],
                    idx_df[['date', 'idx_ret']],
                    on='date', how='inner'
                ).dropna()
                if len(merged) > 10:
                    cov = merged['ret'].cov(merged['idx_ret'])
                    var = merged['idx_ret'].var()
                    if var > 0:
                        beta = cov / var
                        return vol_20d, float(beta)
        except Exception:
            pass

        return vol_20d, 1.0


# 全局实例
stock_classifier = StockClassifier()
