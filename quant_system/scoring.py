"""
股票综合评分模块 — 单一数据源，全局复用
双轨评分系统 v2.0：价值体系(V1-V7) + 趋势体系(T1-T7)

对外接口：
    score_classification(total_score) -> (rating, rating_color)
    scoring_core(df_ind)              -> dict  (完整原始数据)
    compute_stock_score(code, date)   -> dict  (含 t_score/v_score/total_score 等)
"""

import time
import logging
import threading
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# ── 评分结果 TTL 缓存（10分钟，线程安全）──────────────────────────────────────
_score_cache: dict = {}       # key -> (result_dict, timestamp)
_score_cache_ttl = 600        # seconds
_score_cache_lock = threading.Lock()


def _cache_get(key: str):
    with _score_cache_lock:
        entry = _score_cache.get(key)
    if entry is None:
        return None
    result, ts = entry
    if time.time() - ts > _score_cache_ttl:
        return None
    return result


def _cache_set(key: str, value: dict):
    with _score_cache_lock:
        _score_cache[key] = (value, time.time())


# ── 评分等级 ─────────────────────────────────────────────────────────────────

def score_classification(total_score):
    if total_score >= 80:
        return '顶尖', '#4caf50'
    elif total_score >= 75:
        return '优秀', '#8bc34a'
    elif total_score >= 65:
        return '良好', '#8bc34a'
    elif total_score >= 50:
        return '及格', '#ff9800'
    else:
        return '不及格', '#f44336'


# ── 核心评分引擎 ─────────────────────────────────────────────────────────────

def scoring_core(df_ind) -> dict:
    """双轨评分系统 v2.0: 价值体系(V1-V7) + 趋势体系(T1-T7)

    价值体系: 估值极底/多周期超卖/历史价格底部/深度调整/筑底信号/空头钝化/形态安全
    趋势体系: 估值合理/趋势强度/动量强度/RSI健康/对空钝化/趋势形态/量价配合
    最终得分: 0.7×max(value,trend) + 0.3×min(value,trend), 范围[0,100]
    参数: df_ind — 包含技术指标的 DataFrame（至少20行）
    """
    df_ind = df_ind.copy()
    for _fc in ('pe_ttm', 'pb', 'pettm_pct10y'):
        if _fc in df_ind.columns:
            df_ind[_fc] = df_ind[_fc].ffill()

    n = len(df_ind)
    latest = df_ind.iloc[-1]

    def safe(v, default=0.0):
        try:
            f = float(v)
            return f if (f == f) and f != float('inf') and f != float('-inf') else default
        except Exception:
            return default

    def clamp(v, lo=0.0, hi=100.0):
        return max(lo, min(hi, v))

    close = safe(latest.get('close'))
    rsi6  = safe(latest.get('rsi_6'),  50)
    rsi12 = safe(latest.get('rsi_12'), 50)
    rsi24 = safe(latest.get('rsi_24'), 50)
    ma5   = safe(latest.get('ma_5'),  close)
    ma20  = safe(latest.get('ma_20'), close)
    ma60  = safe(latest.get('ma_60'), close)
    macd_val  = safe(latest.get('macd'))
    macd_sig  = safe(latest.get('macd_signal'))
    macd_hist = safe(latest.get('macd_histogram'))
    boll_upper  = safe(latest.get('boll_upper'),  close * 1.05)
    boll_lower  = safe(latest.get('boll_lower'),  close * 0.95)
    boll_middle = safe(latest.get('boll_middle'), close)
    boll_pos = clamp(
        (close - boll_lower) / (boll_upper - boll_lower), -0.1, 1.1
    ) if boll_upper != boll_lower else 0.5
    kdj_j = safe(latest.get('kdj_j'), 50)
    wr14  = safe(latest.get('wr_14'), -50)

    vol_col = 'volume' if 'volume' in df_ind.columns else 'vol'
    if 'volume_ratio' in df_ind.columns:
        vol_ratio = safe(latest.get('volume_ratio'), 1.0)
    elif vol_col in df_ind.columns and n >= 20:
        recent_v = df_ind[vol_col].iloc[-5:].mean()
        avg_v20  = df_ind[vol_col].iloc[-20:].mean()
        vol_ratio = recent_v / avg_v20 if avg_v20 > 0 else 1.0
    else:
        vol_ratio = 1.0

    pe_raw = safe(latest.get('pe_ttm'), float('nan'))
    pb_raw = safe(latest.get('pb'),     float('nan'))
    pe_avail = (pe_raw == pe_raw)
    pb_avail = (pb_raw == pb_raw)

    def m_pct(days):
        if n > days:
            prev = safe(df_ind.iloc[-(days + 1)].get('close'), close)
            return (close - prev) / prev * 100 if prev > 0 else 0.0
        return 0.0

    m5  = m_pct(5)
    m20 = m_pct(20)
    m60 = m_pct(60)

    lookback = min(n, 500)
    price_window = df_ind['close'].iloc[-lookback:].dropna()
    if len(price_window) > 10:
        h500, l500 = price_window.max(), price_window.min()
        price_pos_pct = clamp((close - l500) / (h500 - l500), 0, 1) if h500 != l500 else 0.5
    else:
        price_pos_pct = 0.5

    def hist_pct(series, value):
        s = series.dropna()
        if len(s) < 10 or value != value:
            return None
        return (s < value).sum() / len(s)

    pe_hist, pb_hist = None, None
    if pe_avail and pe_raw > 0 and 'pe_ttm' in df_ind.columns:
        pe_s = df_ind['pe_ttm'].iloc[-lookback:]
        pe_s = pe_s[pe_s > 0]
        if len(pe_s) >= 10:
            pe_hist = hist_pct(pe_s, pe_raw)
    if pb_avail and pb_raw > 0 and 'pb' in df_ind.columns:
        pb_s = df_ind['pb'].iloc[-lookback:]
        pb_s = pb_s[pb_s > 0]
        if len(pb_s) >= 10:
            pb_hist = hist_pct(pb_s, pb_raw)

    # ═══════════════════════════════════════════════════════════════
    # VALUE SYSTEM (价值体系 V1-V7)
    # ═══════════════════════════════════════════════════════════════

    def _pe_val(pct):
        if pct is None: return 50
        if pct < 0.05: return 98
        if pct < 0.10: return 88
        if pct < 0.20: return 75
        if pct < 0.35: return 60
        if pct < 0.50: return 45
        if pct < 0.65: return 28
        if pct < 0.80: return 14
        if pct < 0.90: return 6
        return 2

    def _pb_val(pct, pb_abs):
        base = _pe_val(pct)
        if pb_abs is not None and pb_abs > 0:
            if pb_abs < 1.0: base = min(100, base + 10)
            elif pb_abs < 1.5: base = min(100, base + 5)
        return base

    if pe_avail and pe_raw < 0:
        v1_pe = 10
    elif pe_hist is not None:
        v1_pe = _pe_val(pe_hist)
    else:
        v1_pe = 50
    v1_pb = _pb_val(pb_hist, pb_raw if pb_avail else None)
    if pe_avail and pb_avail:
        V1 = clamp(0.55 * v1_pe + 0.45 * v1_pb)
    elif pe_avail:
        V1 = clamp(float(v1_pe))
    elif pb_avail:
        V1 = clamp(float(v1_pb))
    else:
        V1 = 50.0

    def _rsi_val(r):
        if r < 15: return 100
        if r < 20: return 95
        if r < 25: return 88
        if r < 30: return 80
        if r < 35: return 70
        if r < 40: return 57
        if r < 45: return 42
        if r < 50: return 28
        if r < 55: return 15
        if r < 60: return 8
        return 0

    V2 = 0.40 * _rsi_val(rsi6) + 0.35 * _rsi_val(rsi12) + 0.25 * _rsi_val(rsi24)
    if rsi6 < 30 and rsi12 < 35 and rsi24 < 40:
        V2 = min(100, V2 + 12)
    V2 = clamp(V2)

    pp = price_pos_pct
    if pp < 0.05:   V3 = 100
    elif pp < 0.10: V3 = 90
    elif pp < 0.20: V3 = 78
    elif pp < 0.30: V3 = 63
    elif pp < 0.40: V3 = 48
    elif pp < 0.50: V3 = 35
    elif pp < 0.65: V3 = 20
    elif pp < 0.80: V3 = 8
    else:           V3 = 0
    V3 = clamp(V3)

    if m60 < -40:   V4 = 100
    elif m60 < -30: V4 = 85
    elif m60 < -20: V4 = 68
    elif m60 < -10: V4 = 45
    elif m60 < -5:  V4 = 25
    elif m60 < 0:   V4 = 10
    else:           V4 = 0
    if m20 < -15: V4 = min(100, V4 + 12)
    elif m20 < -10: V4 = min(100, V4 + 6)
    V4 = clamp(V4)

    v5_pts = 0
    v5_details = []
    if kdj_j < 0:
        v5_pts += 30; v5_details.append(f'KDJ_J={kdj_j:.1f}极度超卖')
    elif kdj_j < 20:
        v5_pts += 15; v5_details.append(f'KDJ_J={kdj_j:.1f}超卖')
    if wr14 < -90:
        v5_pts += 25; v5_details.append(f'WR={wr14:.1f}深度超卖')
    elif wr14 < -80:
        v5_pts += 15; v5_details.append(f'WR={wr14:.1f}超卖')
    if boll_pos < 0.10:
        v5_pts += 25; v5_details.append('价格近布林下轨')
    elif boll_pos < 0.20:
        v5_pts += 12; v5_details.append('价格接近布林下轨')
    if vol_ratio < 0.7:
        v5_pts += 20; v5_details.append(f'量比={vol_ratio:.2f}缩量探底')
    elif vol_ratio < 0.8:
        v5_pts += 10; v5_details.append(f'量比={vol_ratio:.2f}略缩量')
    if not v5_details:
        v5_details.append('无明显筑底信号')
    V5 = clamp(v5_pts)

    v6_pts = 0
    v6_details = []
    if n >= 25 and m20 < 0 and m5 < 0:
        dr5 = m5 / 5
        dr20 = m20 / 20
        if abs(dr5) < abs(dr20) * 0.8:
            v6_pts += 30; v6_details.append('下跌节奏放缓')
    elif m5 >= 0 and m20 < 0:
        v6_pts += 30; v6_details.append('短期止跌回升')
    if n >= 30:
        recent_low_5 = df_ind['close'].iloc[-6:].min()
        prior_low_20 = df_ind['close'].iloc[-26:-6].min()
        if recent_low_5 >= prior_low_20 * 0.99:
            v6_pts += 35; v6_details.append('近期低点守住支撑')
        elif recent_low_5 >= prior_low_20 * 0.96:
            v6_pts += 15; v6_details.append('低点小幅突破')
    if close > boll_lower:
        v6_pts += 20; v6_details.append('价格在布林下轨上方')
    if m5 > 0 and m60 < 0:
        v6_pts += 15; v6_details.append('短期相对长期超额回报')
    if not v6_details:
        v6_details.append('无明显止跌信号')
    V6 = clamp(v6_pts)

    v7_details = []
    if n >= 60:
        rl20  = df_ind['close'].iloc[-20:].min()
        pl20  = df_ind['close'].iloc[-40:-20].min()
        pl220 = df_ind['close'].iloc[-60:-40].min()
        if rl20 > pl20 * 1.02 and pl20 > pl220 * 1.02:
            V7 = 92; v7_details.append('连续抬底，反转信号强')
        elif rl20 > pl20 * 1.00:
            V7 = 72; v7_details.append('近期低点抬升，底部稳健')
        elif rl20 > pl20 * 0.97:
            V7 = 50; v7_details.append('低点轻微下移，谨慎')
        elif rl20 > pl20 * 0.92:
            V7 = 22; v7_details.append('低点明显下移，一浪比一浪低')
        else:
            V7 = 0; v7_details.append('连续大幅新低，不宜抄底')
        if pl20 < pl220 and rl20 > pl20 * 1.01:
            V7 = min(100, V7 + 10); v7_details.append('前低后高，潜在底部反转')
    else:
        V7 = 50; v7_details.append('数据不足，默认中性')
    V7 = clamp(V7)

    value_score = clamp(
        0.15 * V1 + 0.22 * V2 + 0.15 * V3 +
        0.12 * V4 + 0.15 * V5 + 0.13 * V6 + 0.08 * V7
    )

    # ═══════════════════════════════════════════════════════════════
    # TREND SYSTEM (趋势体系 T1-T7)
    # ═══════════════════════════════════════════════════════════════

    def _pe_trend(pct, pe_abs=None):
        if pe_abs is not None and pe_abs < 0: return 15
        if pct is None: return 60
        if pct > 0.95: return 0
        if pct > 0.85: return 20
        if pct > 0.75: return 40
        if pct > 0.60: return 62
        if pct > 0.40: return 78
        if pct > 0.20: return 88
        return 92

    def _pb_trend(pct, pb_abs):
        base = _pe_trend(pct)
        if pb_abs is not None and pb_abs > 0 and pb_abs < 1.0:
            base = min(100, base + 5)
        return base

    if pe_avail and pe_raw < 0:
        t1_pe = 15
    elif pe_hist is not None:
        t1_pe = _pe_trend(pe_hist, pe_raw)
    else:
        t1_pe = 60
    t1_pb = _pb_trend(pb_hist, pb_raw if pb_avail else None)
    if pe_avail and pb_avail:
        T1 = clamp(0.55 * t1_pe + 0.45 * t1_pb)
    elif pe_avail:
        T1 = clamp(float(t1_pe))
    elif pb_avail:
        T1 = clamp(float(t1_pb))
    else:
        T1 = 60.0

    t2_details = []
    if close > ma5 and ma5 > ma20 and ma20 > ma60:
        ma_score = 90; t2_details.append('多头完全排列')
    elif close > ma20 and ma20 > ma60:
        ma_score = 75; t2_details.append('中长期多头')
    elif close > ma60:
        ma_score = 55; t2_details.append('站上MA60')
    elif close > ma20:
        ma_score = 35; t2_details.append('短期多头，未站MA60')
    elif close > ma5:
        ma_score = 20; t2_details.append('仅短期多头')
    else:
        ma_score = 5;  t2_details.append('空头排列')
    macd_bonus = 0
    if macd_val > 0 and macd_val > macd_sig:
        macd_bonus = 20; t2_details.append('MACD零轴上方金叉')
    elif macd_hist > 0:
        macd_bonus = 10; t2_details.append('MACD柱上翻')
    elif macd_val > macd_sig:
        macd_bonus = 5;  t2_details.append('MACD金叉(零轴下)')
    T2 = clamp(ma_score + macd_bonus)

    def _m5t(m):
        if m > 20:  return 55
        if m > 10:  return 88
        if m > 3:   return 100
        if m > 0:   return 72
        if m > -5:  return 42
        if m > -10: return 18
        return 0

    def _m20t(m):
        if m > 35:  return 55
        if m > 20:  return 90
        if m > 8:   return 100
        if m > 3:   return 80
        if m > 0:   return 60
        if m > -5:  return 38
        if m > -10: return 18
        return 5

    def _m60t(m):
        if m > 60:  return 50
        if m > 35:  return 85
        if m > 15:  return 100
        if m > 5:   return 88
        if m > 0:   return 68
        return 10

    T3 = clamp(0.40 * _m5t(m5) + 0.35 * _m20t(m20) + 0.25 * _m60t(m60))

    def _rsi_trend(r):
        if 45 <= r <= 62:  return 100
        if 40 <= r < 45:   return 85
        if 62 < r <= 70:   return 78
        if 35 <= r < 40:   return 68
        if 70 < r <= 78:   return 50
        if 25 <= r < 35:   return 45
        if 78 < r <= 85:   return 22
        if r > 85:         return 8
        return 30

    T4 = clamp(0.55 * _rsi_trend(rsi6) + 0.45 * _rsi_trend(rsi12))

    t5_pts = 0
    t5_details = []
    if vol_col in df_ind.columns and n >= 20:
        recent20 = df_ind.iloc[-20:].copy()
        prev_close_s = recent20['close'].shift(1)
        up_mask   = recent20['close'] >= prev_close_s
        down_mask = ~up_mask
        up_vols   = recent20.loc[up_mask,   vol_col]
        down_vols = recent20.loc[down_mask, vol_col]
        if len(up_vols) > 0 and len(down_vols) > 0:
            avg_up = up_vols.mean()
            avg_dn = down_vols.mean()
            if avg_up > avg_dn * 1.3:
                t5_pts += 35; t5_details.append('上涨日量能远大于下跌日')
            elif avg_up > avg_dn * 1.1:
                t5_pts += 20; t5_details.append('上涨日量能大于下跌日')
            elif avg_up > avg_dn * 0.9:
                t5_pts += 10; t5_details.append('量能均衡')
    if boll_pos > 0.5:
        t5_pts += 25; t5_details.append('价格站在布林中轨上方')
    elif boll_pos > 0.35:
        t5_pts += 10; t5_details.append('价格近布林中轨')
    if n >= 25:
        dr5_t  = m5  / 5
        dr20_t = m20 / 20
        if dr5_t > dr20_t * 0.5:
            t5_pts += 20; t5_details.append('短期日均涨幅强于中期')
    if close > ma5 and close > ma20:
        t5_pts += 20; t5_details.append('价格站稳均线上方')
    elif close > ma20:
        t5_pts += 10
    if not t5_details:
        t5_details.append('无明显积累信号')
    T5 = clamp(t5_pts)

    t6_details = []
    if n >= 60:
        rh20   = df_ind['close'].iloc[-20:].max()
        ph20   = df_ind['close'].iloc[-40:-20].max()
        rl20_t = df_ind['close'].iloc[-20:].min()
        pl20_t = df_ind['close'].iloc[-40:-20].min()
        hh = rh20   > ph20   * 1.01
        hl = rl20_t > pl20_t * 1.01
        if hh and hl:
            t6_base = 100; t6_details.append('一浪比一浪高，高低点均抬升')
        elif hh:
            t6_base = 68;  t6_details.append('高点抬升，低点未抬升')
        elif hl:
            t6_base = 62;  t6_details.append('低点抬升，高点未突破')
        elif rh20 >= ph20 * 0.99 and rl20_t >= pl20_t * 0.99:
            t6_base = 42;  t6_details.append('震荡整理')
        else:
            t6_base = 10;  t6_details.append('高低点均下移，趋势不佳')
        fast_rise_bonus = 0
        if m60 > 15 and m5 < 0 and abs(m5) < m60 / 12:
            fast_rise_bonus = 20; t6_details.append('快速上涨后小幅回调，趋势健康')
        elif m20 > 8 and m5 < 0 and abs(m5) < m20 / 4:
            fast_rise_bonus = 12; t6_details.append('中期上涨后小幅回调')
        T6 = clamp(t6_base + fast_rise_bonus)
    else:
        T6 = 50; t6_details.append('数据不足，默认中性')

    t7_details = []
    if vol_ratio > 3:
        if m5 > 0: T7 = 55; t7_details.append('异常放量上涨，注意阶段高点')
        else:       T7 = 15; t7_details.append('异常放量下跌，出货风险')
    elif vol_ratio > 1.5:
        if m5 > 0: T7 = 100; t7_details.append('放量上涨，趋势确认')
        else:       T7 = 25;  t7_details.append('放量下跌，趋势破坏')
    elif vol_ratio > 1.1:
        if m5 > 0: T7 = 82; t7_details.append('温和放量上涨')
        else:       T7 = 40; t7_details.append('轻度放量下跌')
    elif vol_ratio > 0.7:
        if m5 > 0: T7 = 65; t7_details.append('缩量上涨，待量能确认')
        else:       T7 = 72; t7_details.append('缩量回调，健康整理')
    else:
        if m5 > 0: T7 = 55; t7_details.append('极度缩量上涨')
        else:       T7 = 78; t7_details.append('极度缩量回调，惜售信号')
    T7 = clamp(T7)

    trend_score_total = clamp(
        0.12 * T1 + 0.22 * T2 + 0.18 * T3 +
        0.12 * T4 + 0.13 * T5 + 0.13 * T6 + 0.10 * T7
    )

    total_score = round(clamp(
        0.7 * max(value_score, trend_score_total) +
        0.3 * min(value_score, trend_score_total)
    ), 2)

    if trend_score_total > value_score + 5:
        dominant = '趋势主导'
    elif value_score > trend_score_total + 5:
        dominant = '价值主导'
    else:
        dominant = '趋势+价值均衡'

    rating, rating_color = score_classification(total_score)

    return {
        'close': close,
        'total_score': total_score,
        'rating': rating,
        'rating_color': rating_color,
        'dominant': dominant,
        # T/V 双评分（对外统一命名）
        't_score':           round(trend_score_total, 1),
        'v_score':           round(value_score, 1),
        # 向后兼容旧字段名
        'popular_score':     round(trend_score_total, 1),
        'value_score':       round(value_score, 1),
        'trend_score_total': round(trend_score_total, 1),
        # 价值子分
        'V1': round(V1, 1), 'V2': round(V2, 1), 'V3': round(V3, 1),
        'V4': round(V4, 1), 'V5': round(V5, 1), 'V6': round(V6, 1), 'V7': round(V7, 1),
        # 趋势子分
        'T1': round(T1, 1), 'T2': round(T2, 1), 'T3': round(T3, 1),
        'T4': round(T4, 1), 'T5': round(T5, 1), 'T6': round(T6, 1), 'T7': round(T7, 1),
        # 原始因子
        'rsi6': round(rsi6, 2), 'rsi12': round(rsi12, 2), 'rsi24': round(rsi24, 2),
        'boll_pos': round(boll_pos, 3),
        'm5': round(m5, 2), 'm20': round(m20, 2), 'm60': round(m60, 2),
        'vol_ratio': round(vol_ratio, 2),
        'macd_val': round(macd_val, 4), 'macd_sig': round(macd_sig, 4),
        'price_pos_pct': round(price_pos_pct * 100, 1),
        'kdj_j': round(kdj_j, 1), 'wr14': round(wr14, 1),
        'pe_ttm': pe_raw if pe_avail else None,
        'pb': pb_raw if pb_avail else None,
        'pe_hist_pct': round(pe_hist * 100, 1) if pe_hist is not None else None,
        'pb_hist_pct': round(pb_hist * 100, 1) if pb_hist is not None else None,
        # 细节文字
        't2_details': t2_details, 't5_details': t5_details,
        't6_details': t6_details, 't7_details': t7_details,
        'v5_details': v5_details, 'v6_details': v6_details, 'v7_details': v7_details,
    }


# 向后兼容别名（web_app.py 内部旧调用名）
_scoring_core = scoring_core


# ── 单股评分入口（供外部模块直接调用）─────────────────────────────────────────

def compute_stock_score(code: str, date: Optional[str] = None) -> dict:
    """计算单只股票的综合评分（带 TTL 缓存）。

    Returns dict with keys:
        total_score, t_score, v_score, rating, rating_color, dominant,
        + all sub-scores (V1..V7, T1..T7) and raw factors
    """
    cache_key = f"{code}_{date or 'latest'}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    default_result = {
        'total_score': None, 't_score': None, 'v_score': None,
        'rating': '-', 'rating_color': '#999',
    }
    try:
        from .data_source import unified_data
        from .indicators import technical_indicators
        from datetime import datetime, timedelta

        if date:
            end_date   = date
            start_date = (datetime.strptime(date, '%Y%m%d') - timedelta(days=600)).strftime('%Y%m%d')
            df = unified_data.get_historical_data(code, start_date, end_date)
        else:
            df = unified_data.get_historical_data(code)

        if df is None or df.empty:
            return default_result

        if 'trade_date' in df.columns and 'date' not in df.columns:
            df = df.rename(columns={'trade_date': 'date'})
        if 'vol' in df.columns and 'volume' not in df.columns:
            df = df.rename(columns={'vol': 'volume'})
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df_ind = technical_indicators.calculate_all_indicators_from_df(df)
        if df_ind.empty or len(df_ind) < 20:
            return default_result

        result = scoring_core(df_ind)
        _cache_set(cache_key, result)
        return result

    except Exception as e:
        logger.error(f"compute_stock_score({code}) error: {e}")
        return default_result
