"""
策略匹配引擎
根据大盘环境 + 股票类型 → 推荐适用策略
"""

import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from .market_regime import MarketRegime, MarketAnalysis, market_regime_detector
from .stock_classifier import StockCategory, StockClassification, stock_classifier
from .strategy import strategy_manager

logger = logging.getLogger(__name__)


@dataclass
class StrategyRecommendation:
    """策略推荐结果"""
    strategy_name: str
    reason: str
    priority: int            # 1=最优先, 数字越大越靠后

    def to_dict(self) -> Dict:
        return {
            'strategy_name': self.strategy_name,
            'reason': self.reason,
            'priority': self.priority,
        }


# 策略-大盘环境映射表 (用户定义)
# key: MarketRegime, value: [(buy_strategy_name, sell_strategy_name, reason, priority)]
REGIME_STRATEGY_MAP: Dict[MarketRegime, List[Tuple[str, str, str, int]]] = {
    MarketRegime.OPTIMISTIC: [
        ('RSI超卖反弹买入', 'RSI超买卖出', '短线波段：大盘乐观适合快进快出', 1),
        ('MACD金叉买入', 'MACD死叉卖出', '短线趋势：大盘乐观MACD信号有效', 2),
        ('均线多头买入', '均线空头卖出', '短线均线：乐观市追涨MA突破', 3),
        ('短线突破买入', '短线趋势卖出', '短线突破：放量二次突破配合大盘', 4),
        ('中线1反身性买入', '中线1反身性卖出', '中线反身：乐观市反身增强效应最明显', 5),
    ],
    MarketRegime.CHAOTIC: [
        ('中线2三重保护买入', '中线2估值修复卖出', '中线三重保护：震荡市低估值+支撑有效', 1),
        ('震荡市RSI波段买入', '震荡市RSI波段卖出', '震荡RSI波段：专为震荡设计,Sharpe=0.39', 2),
        ('布林带下轨买入', '布林带上轨卖出', '布林带均值回归：震荡市效果最佳', 3),
        ('KDJ超卖买入', 'KDJ超买卖出', 'KDJ超卖反弹：混沌市用超卖捕捉底部', 4),
    ],
    MarketRegime.PESSIMISTIC: [
        # 悲观市以空仓为主，仅用防御策略小仓位
        ('防御股熊市超跌买入', '防御股熊市超跌卖出', '防御超跌：仅限防御型股票,小仓位快进快出', 1),
    ],
    MarketRegime.EXTREMELY_PESSIMISTIC: [
        ('长线4421极限买入', '长线泡沫卖出', '长线4421：极度悲观是长线买入最佳时机', 1),
    ],
}

# 全天候策略(无视大盘)
ALWAYS_ON_STRATEGIES = [
    ('超级长线价值买入', '超级长线卖出', '超级长线：无视大盘,只看企业价值', 10),
    ('成长股强势动量买入', '成长股强势动量卖出', '全天候成长：相对强度选股,不依赖大盘方向', 11),
    ('AI共振买入v19', 'AI共振卖出v19', 'AI共振：5轮迭代最优通用策略', 12),
]

# 股票类型 → 策略偏好映射 (在大盘策略基础上再做类型筛选)
CATEGORY_STRATEGY_PREFERENCE = {
    StockCategory.GROWTH: [
        '成长股强势动量买入', 'MACD金叉买入', '均线多头买入',
        '短线突破买入', '中线1反身性买入', 'AI共振买入v19',
    ],
    StockCategory.DEFENSIVE: [
        '防御股熊市超跌买入', '低PE价值买入', '中线2三重保护买入',
        '超级长线价值买入', '震荡市RSI波段买入',
    ],
    StockCategory.CYCLICAL: [
        '震荡市RSI波段买入', '布林带下轨买入', 'KDJ超卖买入',
        '中线超跌买入', '中线2三重保护买入',
    ],
}


class StrategyMatcher:
    """策略匹配引擎"""

    def get_recommendations(
        self,
        market: MarketAnalysis,
        stock_class: StockClassification,
    ) -> List[StrategyRecommendation]:
        """根据大盘环境+股票类型获取推荐策略
        
        Returns:
            排序后的策略推荐列表(优先级从高到低)
        """
        recommendations = []
        regime = market.regime

        # 1. 获取大盘环境对应的策略
        regime_strategies = REGIME_STRATEGY_MAP.get(regime, [])
        for buy_name, sell_name, reason, priority in regime_strategies:
            # 检查策略是否存在
            if strategy_manager.get_strategy(buy_name):
                recommendations.append(StrategyRecommendation(
                    strategy_name=buy_name,
                    reason=f"[{regime.label}]{reason}",
                    priority=priority,
                ))

        # 2. 添加全天候策略
        for buy_name, sell_name, reason, priority in ALWAYS_ON_STRATEGIES:
            if strategy_manager.get_strategy(buy_name):
                recommendations.append(StrategyRecommendation(
                    strategy_name=buy_name,
                    reason=reason,
                    priority=priority,
                ))

        # 3. 根据股票类型过滤/排序
        preferred = CATEGORY_STRATEGY_PREFERENCE.get(stock_class.category, [])
        for rec in recommendations:
            if rec.strategy_name in preferred:
                rec.priority -= 5  # 匹配股票类型的策略优先级提升

        recommendations.sort(key=lambda r: r.priority)
        return recommendations

    def get_best_strategy_pair(
        self,
        market: MarketAnalysis,
        stock_class: StockClassification,
    ) -> Optional[Tuple[str, str, str]]:
        """获取最优买卖策略对
        
        Returns:
            (buy_strategy, sell_strategy, reason) 或 None(悲观市空仓且非防御股)
        """
        regime = market.regime

        # 悲观市 + 非防御股 = 空仓
        if regime == MarketRegime.PESSIMISTIC and stock_class.category != StockCategory.DEFENSIVE:
            return None

        recs = self.get_recommendations(market, stock_class)
        if not recs:
            return None

        best = recs[0]
        buy_name = best.strategy_name

        # 找到对应的卖出策略
        sell_name = self._find_sell_strategy(buy_name)

        return (buy_name, sell_name, best.reason)

    def _find_sell_strategy(self, buy_name: str) -> str:
        """根据买入策略名找到对应的卖出策略"""
        # 策略命名约定: XXX买入 → XXX卖出
        sell_candidates = [
            buy_name.replace('买入', '卖出'),
        ]

        # 特殊映射
        special_map = {
            'RSI超卖反弹买入': 'RSI超买卖出',
            'MACD金叉买入': 'MACD死叉卖出',
            '均线多头买入': '均线空头卖出',
            '低PE价值买入': '高PE高位卖出',
        }
        if buy_name in special_map:
            sell_candidates.insert(0, special_map[buy_name])

        for name in sell_candidates:
            if strategy_manager.get_strategy(name):
                return name

        return buy_name.replace('买入', '卖出')

    def analyze_stock(self, code: str, date: Optional[str] = None) -> Dict:
        """完整分析一只股票: 大盘检测+分类+策略匹配
        
        Returns:
            {market, classification, recommendations, best_pair, is_empty_position}
        """
        market = market_regime_detector.detect(date)
        classification = stock_classifier.classify(code)
        recommendations = self.get_recommendations(market, classification)
        best_pair = self.get_best_strategy_pair(market, classification)

        is_empty = (
            market.regime == MarketRegime.PESSIMISTIC
            and classification.category != StockCategory.DEFENSIVE
        )

        return {
            'market': market.to_dict(),
            'classification': classification.to_dict(),
            'recommendations': [r.to_dict() for r in recommendations],
            'best_pair': {
                'buy': best_pair[0],
                'sell': best_pair[1],
                'reason': best_pair[2],
            } if best_pair else None,
            'is_empty_position': is_empty,
            'action_summary': '空仓观望' if is_empty else f"推荐策略: {best_pair[0] if best_pair else '无'}",
        }

    def analyze_all_stocks(self, date: Optional[str] = None) -> Dict:
        """分析所有监控股票
        
        Returns:
            {market, stocks: [{classification, recommendations, best_pair}]}
        """
        market = market_regime_detector.detect(date)
        all_classified = stock_classifier.classify_all()

        stock_results = []
        for code, classification in all_classified.items():
            recs = self.get_recommendations(market, classification)
            best_pair = self.get_best_strategy_pair(market, classification)
            is_empty = (
                market.regime == MarketRegime.PESSIMISTIC
                and classification.category != StockCategory.DEFENSIVE
            )

            stock_results.append({
                'code': code,
                'name': classification.name,
                'classification': classification.to_dict(),
                'best_pair': {
                    'buy': best_pair[0],
                    'sell': best_pair[1],
                    'reason': best_pair[2],
                } if best_pair else None,
                'is_empty_position': is_empty,
                'top_strategies': [r.to_dict() for r in recs[:3]],
            })

        return {
            'market': market.to_dict(),
            'stocks': stock_results,
        }


# 全局实例
strategy_matcher = StrategyMatcher()
