"""
大盘阶段 + 个股T/V评分 → 操作建议引擎
不推荐具体策略，只判断：买入 / 观望 / 空仓 / 可布局
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from .market_regime import MarketRegime, MarketAnalysis, market_regime_detector
from .scoring import compute_stock_score as _compute_stock_score_full

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# 操作建议常量
# ═══════════════════════════════════════════════════════════════════
ACTION_BUY    = 'buy'      # 买入
ACTION_LAYOUT = 'layout'   # 可布局（极度悲观/底部建仓）
ACTION_WATCH  = 'watch'    # 观望/轻仓
ACTION_EMPTY  = 'empty'    # 空仓

ACTION_DISPLAY = {
    ACTION_BUY:    {'label': '买入',   'badge': 'bg-success',             'icon': '🟢'},
    ACTION_LAYOUT: {'label': '可布局', 'badge': 'bg-primary',             'icon': '🔵'},
    ACTION_WATCH:  {'label': '观望',   'badge': 'bg-warning text-dark',   'icon': '🟡'},
    ACTION_EMPTY:  {'label': '空仓',   'badge': 'bg-secondary',           'icon': '⚪'},
}


def _decide_action(regime: MarketRegime, scores: Dict) -> Tuple[str, str]:
    """根据大盘阶段 + 个股 T/V 综合评分，返回 (action, reason)。

    规则（严格门槛优先）：
    ──────────────────────────────────────────────────────────────
    乐观  : T>=58 → 买入；T>=45 → 观望；T<35 → 空仓；其余观望
    混沌  : total>=58 → 谨慎买入；total>=47 → 观望；total<38 → 空仓
    悲观  : 全部空仓（大盘下跌趋势未止，不逆势操作）
    极度悲观: V>=65 且 total>=50 → 可布局；V>=55 → 观望；其余空仓
    ──────────────────────────────────────────────────────────────
    """
    t     = float(scores.get('t_score',     50) or 50)
    v     = float(scores.get('v_score',     30) or 30)
    total = float(scores.get('total_score', 45) or 45)

    if regime == MarketRegime.OPTIMISTIC:
        if t >= 58:
            return ACTION_BUY,   f'乐观市，T分={t:.0f}（趋势强劲，顺势买入）'
        if t >= 45:
            return ACTION_WATCH, f'乐观市，T分={t:.0f}（趋势一般，等待更强信号）'
        if t < 35:
            return ACTION_EMPTY, f'乐观市但个股T分={t:.0f}（无趋势，不参与）'
        return ACTION_WATCH,     f'乐观市，T分={t:.0f}（趋势偏弱，观望为主）'

    if regime == MarketRegime.CHAOTIC:
        if total >= 58:
            return ACTION_BUY,   f'混沌市，综合分={total:.0f}（评分优秀，谨慎买入）'
        if total >= 47:
            return ACTION_WATCH, f'混沌市，综合分={total:.0f}（评分一般，观望等待）'
        return ACTION_EMPTY,     f'混沌市，综合分={total:.0f}（评分偏低，空仓为主）'

    if regime == MarketRegime.PESSIMISTIC:
        return ACTION_EMPTY,     f'悲观市，大盘下跌未止，全部空仓等待（综合分={total:.0f}）'

    if regime == MarketRegime.EXTREMELY_PESSIMISTIC:
        if v >= 65 and total >= 50:
            return ACTION_LAYOUT, f'极度悲观，V分={v:.0f}且综合分={total:.0f}（底部价值高，可长线布局）'
        if v >= 55:
            return ACTION_WATCH,  f'极度悲观，V分={v:.0f}（有底部价值，轻仓观察）'
        return ACTION_EMPTY,      f'极度悲观，V分={v:.0f}（底部价值不足，继续空仓）'

    return ACTION_EMPTY, '未知阶段，默认空仓'


class StrategyMatcher:
    """大盘阶段+个股T/V评分 → 操作建议引擎"""

    def _compute_stock_scores(self, code: str, date: Optional[str] = None) -> Dict:
        """计算个股T/V评分，委托给 scoring.py 统一实现"""
        _default = {'t_score': 50.0, 'v_score': 30.0, 'total_score': 45.0}
        try:
            result = _compute_stock_score_full(code, date)
            t   = result.get('t_score')   if result.get('t_score')   is not None else result.get('trend_score_total')
            v   = result.get('v_score')   if result.get('v_score')   is not None else result.get('value_score')
            tot = result.get('total_score')
            return {
                't_score':     float(t)   if t   is not None else 50.0,
                'v_score':     float(v)   if v   is not None else 30.0,
                'total_score': float(tot) if tot is not None else 45.0,
            }
        except Exception as e:
            logger.debug(f"个股评分计算失败 {code}: {e}")
            return _default

    # ── 单股分析 ────────────────────────────────────────────────────

    def analyze_stock(self, code: str, date: Optional[str] = None) -> Dict:
        """完整分析一只股票：大盘检测 + 个股T/V评分 + 操作建议"""
        from .stock_manager import stock_manager
        stock = stock_manager.get_stock_by_code(code)
        name  = stock.name if stock else code

        market = market_regime_detector.detect(date)
        scores = self._compute_stock_scores(code, date)
        action, reason = _decide_action(market.regime, scores)

        return {
            'code':    code,
            'name':    name,
            'market':  market.to_dict(),
            'scores':  scores,
            'action':  action,
            'reason':  reason,
            'action_display': ACTION_DISPLAY.get(action, ACTION_DISPLAY[ACTION_EMPTY]),
            'is_empty_position': action == ACTION_EMPTY,
        }

    # ── 全量股票分析 ─────────────────────────────────────────────────

    def analyze_all_stocks(self, date: Optional[str] = None) -> Dict:
        """分析所有监控股票，返回大盘信息 + 每只股票操作建议"""
        from .stock_manager import stock_manager

        market      = market_regime_detector.detect(date)
        stocks_info = stock_manager.get_all_stocks()

        stock_results = []
        for stock in stocks_info:
            code      = stock.code
            full_code = getattr(stock, 'full_code', code)
            scores    = self._compute_stock_scores(code, date)
            action, reason = _decide_action(market.regime, scores)
            stock_results.append({
                'code':      code,
                'full_code': full_code,
                'name':      stock.name,
                'scores':    scores,
                'action':    action,
                'reason':    reason,
                'action_display': ACTION_DISPLAY.get(action, ACTION_DISPLAY[ACTION_EMPTY]),
                'is_empty_position': action == ACTION_EMPTY,
            })

        # 排序：买入 > 可布局 > 观望 > 空仓，同组内按 total_score 降序
        _order = {ACTION_BUY: 0, ACTION_LAYOUT: 1, ACTION_WATCH: 2, ACTION_EMPTY: 3}
        stock_results.sort(key=lambda s: (
            _order.get(s['action'], 9),
            -s['scores'].get('total_score', 0),
        ))

        return {
            'market': market.to_dict(),
            'stocks': stock_results,
        }


# 全局实例
strategy_matcher = StrategyMatcher()

