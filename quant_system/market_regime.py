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
    score: float             # 综合评分 -100 ~ +100
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
            'idx_ret_5': self.idx_ret_5,
            'idx_ret_20': self.idx_ret_20,
            'idx_ret_60': self.idx_ret_60,
            'ma_trend': self.ma_trend,
            'rsi_level': self.rsi_level,
            'detail': self.detail,
            'date': self.date,
        }


class MarketRegimeDetector:
    """大盘环境检测器
    
    评分体系 (-100 ~ +100):
    - idx_ret_20 (20日涨幅): 权重 30%
    - idx_ret_60 (60日涨幅): 权重 20%
    - 均线排列 (MA5>MA20>MA60): 权重 25%
    - RSI_14: 权重 15%
    - 量能 (volume_ratio): 权重 10%

    判断标准（多指数加权综合评分）:
    - score > 30:  乐观
    - -10 < score <= 30:  混沌
    - -55 < score <= -10: 悲观
    - score <= -55: 极度悲观
    """

    OPTIMISTIC_THRESHOLD = 30
    CHAOTIC_THRESHOLD = -10
    PESSIMISTIC_THRESHOLD = -55   # 已收严：须≤-55才判定为极度悲观

    def _score_single_index(self, df_ind: 'pd.DataFrame') -> Optional[Dict]:
        """计算单个指数的全部评分分量，返回 dict 或 None（数据不足时）"""
        try:
            latest = df_ind.iloc[-1]
            close   = float(latest.get('close',  0)   or 0)
            ma_5    = float(latest.get('ma_5',   close) or close)
            ma_20   = float(latest.get('ma_20',  close) or close)
            ma_60   = float(latest.get('ma_60',  close) or close)
            ma_120  = float(latest.get('ma_120', close) or close)
            rsi_14  = float(latest.get('rsi_14', 50)  or 50)
            vol_ratio = float(latest.get('volume_ratio', 1.0) or 1.0)

            idx_ret_5  = self._calc_return(df_ind, 5)
            idx_ret_20 = self._calc_return(df_ind, 20)
            idx_ret_60 = self._calc_return(df_ind, 60)

            score_20d = float(np.clip(idx_ret_20 * 5, -30, 30))
            score_60d = float(np.clip(idx_ret_60 * 2, -20, 20))

            ma_raw = 0
            if close > ma_5:   ma_raw += 5
            if ma_5   > ma_20: ma_raw += 7
            if ma_20  > ma_60: ma_raw += 8
            if ma_60  > ma_120: ma_raw += 5
            ma_score = (ma_raw - 12.5) * 2  # -25 ~ +25

            if close > ma_5 > ma_20 > ma_60:
                ma_trend = '多头排列'
            elif close < ma_5 < ma_20 < ma_60:
                ma_trend = '空头排列'
            else:
                ma_trend = '均线纠缠'

            rsi_score = float(np.clip((rsi_14 - 50) * 0.3, -15, 15))
            rsi_level = '超买' if rsi_14 > 70 else ('超卖' if rsi_14 < 30 else '中性')

            vol_score = float(np.clip((vol_ratio - 1.0) * 10, -10, 10))

            score = score_20d + score_60d + ma_score + rsi_score + vol_score
            date_val = str(latest.get('date', ''))[:10]

            return dict(
                score=score, idx_ret_5=idx_ret_5, idx_ret_20=idx_ret_20, idx_ret_60=idx_ret_60,
                ma_trend=ma_trend, rsi_level=rsi_level, rsi_14=rsi_14, vol_ratio=vol_ratio,
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

            # 用上证（或第一个可用指数）做趋势文字描述
            primary = next((s for lbl, _, s in norm if lbl == '上证'), norm[0][2])
            ma_trend  = primary['ma_trend']
            rsi_level = primary['rsi_level']
            score = round(composite_score, 2)

            # 判定阶段
            if score > self.OPTIMISTIC_THRESHOLD:
                regime = MarketRegime.OPTIMISTIC
            elif score > self.CHAOTIC_THRESHOLD:
                regime = MarketRegime.CHAOTIC
            elif score > self.PESSIMISTIC_THRESHOLD:
                regime = MarketRegime.PESSIMISTIC
            else:
                regime = MarketRegime.EXTREMELY_PESSIMISTIC

            idx_parts = ' | '.join(f"{lbl}{s['score']:.1f}分" for lbl, _, s in norm)
            detail = (
                f"综合评分{score:.1f}({idx_parts}) | "
                f"20日{composite_ret20:+.2f}% | 60日{composite_ret60:+.2f}% | "
                f"{ma_trend}(上证) | RSI={primary['rsi_14']:.1f}({rsi_level})"
            )
            analysis_date = norm[0][2]['date'] or end_date

            return MarketAnalysis(
                regime=regime, score=score,
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
