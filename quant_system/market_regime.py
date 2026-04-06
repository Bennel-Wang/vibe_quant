"""
大盘环境检测模块
自动判断当前大盘处于: 乐观/混沌/悲观/极度悲观
"""

import logging
from typing import Optional, Dict, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

import pandas as pd
import numpy as np

from .data_source import unified_data
from .indicators import technical_indicators

logger = logging.getLogger(__name__)

# 多指数配置: (代码, 标签, 权重)
# 港股恒生指数若本地无数据则自动跳过，权重重新归一化
BENCHMARKS = [
    ('000001.SH', '上证', 0.45),   # 上证综指
    ('399001.SZ', '深证', 0.35),   # 深证成指
    ('399006.SZ', '创业板', 0.10), # 创业板指
    ('HSI.HK', '港股', 0.10),   # 恒生指数（如无数据自动跳过）
]


class MarketRegime(Enum):
    """大盘环境枚举"""
    OPTIMISTIC = 'optimistic'        # 乐观
    CHAOTIC = 'chaotic'              # 混沌/震荡
    PESSIMISTIC = 'pessimistic'      # 悲观
    EXTREMELY_PESSIMISTIC = 'extremely_pessimistic'  # 极度悲观

    @property
    def label(self) -> str:
        return {
            'optimistic': '乐观',
            'chaotic': '混沌',
            'pessimistic': '悲观',
            'extremely_pessimistic': '极度悲观',
        }[self.value]

    @property
    def emoji(self) -> str:
        return {
            'optimistic': '🟢',
            'chaotic': '🟡',
            'pessimistic': '🔴',
            'extremely_pessimistic': '⚫',
        }[self.value]


@dataclass
class MarketAnalysis:
    """大盘分析结果"""
    regime: MarketRegime
    score: float             # 综合评分 -100 ~ +100 (历史兼容)
    t_score: float           # 趋势评分 0 ~ 100 (新)
    v_score: float           # 价值/底部评分 0 ~ 100 (新)
    idx_ret_5: float         # 5日涨幅
    idx_ret_20: float        # 20日涨幅
    idx_ret_60: float        # 60日涨幅
    ma_trend: str            # 均线趋势: 多头/空头/纠缠
    rsi_level: str           # RSI水平: 超买/中性/超卖
    detail: str              # 详细说明
    date: str                # 分析日期

    def to_dict(self) -> Dict:
        return {
            'regime': self.regime.value,
            'regime_label': self.regime.label,
            'regime_emoji': self.regime.emoji,
            'score': self.score,
            't_score': self.t_score,
            'v_score': self.v_score,
            'idx_ret_5': self.idx_ret_5,
            'idx_ret_20': self.idx_ret_20,
            'idx_ret_60': self.idx_ret_60,
            'ma_trend': self.ma_trend,
            'rsi_level': self.rsi_level,
            'detail': self.detail,
            'date': self.date,
        }


class MarketRegimeDetector:
    """大盘环境检测器（T/V双评分体系，复用 scoring_core 统一算法）

    T评分 / V评分 来自 scoring.py 的 scoring_core()，与股票详情页完全一致。

    判断标准（多指数加权综合T/V评分）:
    - T >= 50:               乐观 (趋势明确)
    - T < 35 AND V >= 55:    极度悲观 (深度熊市=4421布局机会)
    - T >= 28 (< 50):        混沌 (方向不明/震荡)
    - T < 28 AND V < 55:     悲观 (下跌趋势，尚未触底)
    """

    # T/V based thresholds (calibrated for scoring_core() full system)
    T_OPTIMISTIC = 50     # T >= 50 → 乐观 (明确趋势)
    T_CHAOTIC    = 28     # T >= 28 → 混沌 (unless V >= V_EXT_PESS)
    V_EXT_PESS   = 55     # V >= 55 AND T < 35 → 极度悲观 (深度超卖布局机会)

    def _score_single_index(self, df_ind: 'pd.DataFrame') -> Optional[Dict]:
        """计算单个指数的全部评分分量，返回 dict 或 None（数据不足时）"""
        try:
            from .scoring import scoring_core

            latest = df_ind.iloc[-1]
            close   = float(latest.get('close',  0)   or 0)
            ma_5    = float(latest.get('ma_5',   close) or close)
            ma_20   = float(latest.get('ma_20',  close) or close)
            ma_60   = float(latest.get('ma_60',  close) or close)
            ma_120  = float(latest.get('ma_120', close) or close)
            rsi_14  = float(latest.get('rsi_14', 50)  or 50)
            # rsi_6 用于 detail 显示
            rsi_6   = float(latest.get('rsi_6',  rsi_14) or rsi_14)
            vol_ratio = float(latest.get('volume_ratio', 1.0) or 1.0)

            idx_ret_5  = self._calc_return(df_ind, 5)
            idx_ret_20 = self._calc_return(df_ind, 20)
            idx_ret_60 = self._calc_return(df_ind, 60)

            # ── 原有综合评分（向后兼容）──────────────────────────────────────────
            score_20d = float(np.clip(idx_ret_20 * 5, -30, 30))
            score_60d = float(np.clip(idx_ret_60 * 2, -20, 20))
            ma_raw = 0
            if close > ma_5:    ma_raw += 5
            if ma_5   > ma_20:  ma_raw += 7
            if ma_20  > ma_60:  ma_raw += 8
            if ma_60  > ma_120: ma_raw += 5
            ma_score = (ma_raw - 12.5) * 2  # -25 ~ +25
            rsi_score = float(np.clip((rsi_14 - 50) * 0.3, -15, 15))
            vol_score = float(np.clip((vol_ratio - 1.0) * 10, -10, 10))
            score = score_20d + score_60d + ma_score + rsi_score + vol_score

            # ── 均线排列 / RSI 级别（用于 detail 字符串）──────────────────────
            if close > ma_5 > ma_20 > ma_60:
                ma_trend = '多头排列'
            elif close < ma_5 < ma_20 < ma_60:
                ma_trend = '空头排列'
            else:
                ma_trend = '均线纠缠'
            rsi_level = '超买' if rsi_6 > 70 else ('超卖' if rsi_6 < 30 else '中性')

            date_val = str(latest.get('date', ''))[:10]

            # ── T/V 评分：复用 scoring_core() ─────────────────────────────────
            sv = scoring_core(df_ind)
            t_score = round(float(sv.get('t_score', sv.get('trend_score_total', 50))), 1)
            v_score = round(float(sv.get('v_score', sv.get('value_score', 30))),  1)

            return dict(
                score=score, t_score=t_score, v_score=v_score,
                idx_ret_5=idx_ret_5, idx_ret_20=idx_ret_20, idx_ret_60=idx_ret_60,
                ma_trend=ma_trend, rsi_level=rsi_level, rsi_14=rsi_6, vol_ratio=vol_ratio,
                score_20d=score_20d, score_60d=score_60d, ma_score=ma_score,
                rsi_score=rsi_score, vol_score=vol_score, date=date_val,
            )
        except Exception as e:
            logger.debug(f"单指数评分失败: {e}")
            return None

    def detect(self, date: Optional[str] = None) -> MarketAnalysis:
        """检测大盘环境（多指数加权综合）

        Args:
            date: 指定日期(YYYYMMDD)，None则用最新数据

        Returns:
            MarketAnalysis 大盘分析结果
        """
        try:
            end_date   = date or datetime.now().strftime('%Y%m%d')
            start_date = (datetime.strptime(end_date, '%Y%m%d') - timedelta(days=400)).strftime('%Y%m%d')

            results = []   # [(label, weight, score_dict)]
            for code, label, weight in BENCHMARKS:
                try:
                    df = unified_data.get_historical_data(code, start_date, end_date)
                    if df is None or df.empty or len(df) < 60:
                        logger.info(f"指数 {code}({label}) 数据不足，跳过")
                        continue
                    df_ind = technical_indicators.calculate_all_indicators_from_df(df.copy())
                    if df_ind.empty:
                        continue
                    if date:
                        df_ind['date'] = pd.to_datetime(df_ind['date'])
                        df_ind = df_ind[df_ind['date'] <= pd.to_datetime(date)]
                        if df_ind.empty:
                            continue
                    s = self._score_single_index(df_ind)
                    if s is not None:
                        results.append((label, weight, s))
                except Exception as e:
                    logger.warning(f"指数 {code}({label}) 计算失败: {e}")

            if not results:
                logger.warning("所有指数数据不足，返回默认分析")
                return self._default_analysis(end_date)

            # 归一化权重（某些指数无数据时按比例分摊）
            total_w = sum(w for _, w, _ in results)
            norm = [(lbl, w / total_w, s) for lbl, w, s in results]

            def wavg(key):
                return sum(w * s[key] for _, w, s in norm)

            composite_score  = float(np.clip(wavg('score'),      -100, 100))
            composite_ret5   = wavg('idx_ret_5')
            composite_ret20  = wavg('idx_ret_20')
            composite_ret60  = wavg('idx_ret_60')
            composite_t      = float(np.clip(wavg('t_score'), 0, 100))
            composite_v      = float(np.clip(wavg('v_score'), 0, 100))

            # 用上证（或第一个可用指数）做趋势文字描述
            primary = next((s for lbl, _, s in norm if lbl == '上证'), norm[0][2])
            ma_trend  = primary['ma_trend']
            rsi_level = primary['rsi_level']
            score = round(composite_score, 2)
            t_score = round(composite_t, 1)
            v_score = round(composite_v, 1)

            # T/V双评分判定阶段：
            # T >= 55 → 乐观（趋势强劲，做趋势跟随）
            # T < 35 AND V >= 62 → 极度悲观（4421时刻，最佳长线布局）
            # T >= 30 → 混沌（方向不明，做综合高分股）
            # T < 30 AND V < 62 → 悲观（下跌趋势尚未触底）
            if t_score >= self.T_OPTIMISTIC:
                regime = MarketRegime.OPTIMISTIC
            elif t_score < 35 and v_score >= self.V_EXT_PESS:
                regime = MarketRegime.EXTREMELY_PESSIMISTIC
            elif t_score >= self.T_CHAOTIC:
                regime = MarketRegime.CHAOTIC
            else:
                regime = MarketRegime.PESSIMISTIC

            idx_parts = ' | '.join(f"{lbl} T={s['t_score']:.0f}/V={s['v_score']:.0f}" for lbl, _, s in norm)
            detail = (
                f"T评分{t_score:.0f}/V评分{v_score:.0f}({idx_parts}) | "
                f"20日{composite_ret20:+.2f}% | 60日{composite_ret60:+.2f}% | "
                f"{ma_trend}(上证) | RSI={primary['rsi_14']:.1f}({rsi_level})"
            )
            analysis_date = norm[0][2]['date'] or end_date

            return MarketAnalysis(
                regime=regime, score=score,
                t_score=t_score, v_score=v_score,
                idx_ret_5=round(composite_ret5, 2),
                idx_ret_20=round(composite_ret20, 2),
                idx_ret_60=round(composite_ret60, 2),
                ma_trend=ma_trend, rsi_level=rsi_level,
                detail=detail, date=analysis_date,
            )

        except Exception as e:
            logger.error(f"大盘环境检测失败: {e}", exc_info=True)
            return self._default_analysis(date or datetime.now().strftime('%Y%m%d'))

    def detect_for_period(self, start_date: str, end_date: str) -> Dict[str, MarketAnalysis]:
        """检测一段时期内每个月末的大盘环境

        Returns:
            Dict[date_str, MarketAnalysis]
        """
        results = {}
        try:
            primary_code = BENCHMARKS[0][0]
            df = unified_data.get_historical_data(primary_code, start_date, end_date)
            if df is None or df.empty:
                return results

            df['date'] = pd.to_datetime(df['date'])
            df['month'] = df['date'].dt.to_period('M')
            month_ends = df.groupby('month')['date'].max()

            for _, date in month_ends.items():
                date_str = date.strftime('%Y%m%d')
                analysis = self.detect(date_str)
                results[date_str] = analysis

        except Exception as e:
            logger.error(f"批量大盘检测失败: {e}")

        return results

    def _calc_return(self, df: pd.DataFrame, n: int) -> float:
        """计算N日收益率"""
        if len(df) < n + 1:
            return 0.0
        try:
            current = float(df.iloc[-1].get('close', 0) or 0)
            past = float(df.iloc[-1 - n].get('close', 0) or 0)
            if past > 0:
                return (current - past) / past * 100
        except Exception:
            pass
        return 0.0

    def _default_analysis(self, date: str) -> MarketAnalysis:
        """数据不足时的默认分析"""
        return MarketAnalysis(
            regime=MarketRegime.CHAOTIC,
            score=0.0,
            t_score=50.0,
            v_score=30.0,
            idx_ret_5=0.0,
            idx_ret_20=0.0,
            idx_ret_60=0.0,
            ma_trend='数据不足',
            rsi_level='未知',
            detail='数据不足，默认返回混沌环境',
            date=date,
        )


# 全局实例
market_regime_detector = MarketRegimeDetector()
