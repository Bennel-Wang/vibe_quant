"""
策略层模块
支持自然语言描述策略、量化策略描述，以及两者之间的互相翻译
"""

import os
import json
import logging
import re
from typing import List, Dict, Optional, Any, Union
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path

import pandas as pd
import numpy as np

from .config_manager import config
from .stock_manager import stock_manager
from .indicators import indicator_analyzer
from .feature_extractor import feature_extractor, strategy_classifier, AIModelClient

logger = logging.getLogger(__name__)


class ActionType(Enum):
    """操作类型"""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"
    WAIT = "wait"


@dataclass
class StrategyRule:
    """策略规则"""
    condition: str  # 条件描述
    action: str  # 动作 (buy/sell/hold)
    position_ratio: float = 1.0  # 仓位比例 (0-1)
    reason: str = ""  # 理由
    connector: str = 'OR'  # 与前一条规则的连接符: 'AND' | 'OR'（第一条规则此字段无效）


@dataclass
class StrategyDecision:
    """策略决策结果"""
    code: str
    action: str  # buy/sell/hold
    position_ratio: float  # 建议仓位比例
    confidence: float  # 置信度
    reasoning: str  # 决策理由
    rules_triggered: List[str]  # 触发的规则
    timestamp: str


class StrategyParser:
    """策略解析器 - 将自然语言转换为量化规则"""

    # ── 阈值表（日/周/月 × 超卖强度）──────────────────────────────────
    _RSI_OVERSOLD = {
        'day':   {'extreme': 20, 'strong': 25, 'mild': 30},
        'week':  {'extreme': 25, 'strong': 30, 'mild': 35},
        'month': {'extreme': 30, 'strong': 35, 'mild': 40},
    }
    _RSI_OVERBOUGHT = {
        'day':   {'extreme': 80, 'strong': 75, 'mild': 70},
        'week':  {'extreme': 78, 'strong': 73, 'mild': 68},
        'month': {'extreme': 75, 'strong': 70, 'mild': 65},
    }
    _KDJ_J_OVERSOLD   = {'extreme': 10, 'strong': 15, 'mild': 20}
    _KDJ_J_OVERBOUGHT = {'extreme': 90, 'strong': 85, 'mild': 80}
    _WR_OVERSOLD      = {'extreme': -85, 'strong': -80, 'mild': -75}
    _WR_OVERBOUGHT    = {'extreme': -15, 'strong': -20, 'mild': -25}
    _TF_PREFIX = {'day': '', 'week': 'w_', 'month': 'm_'}

    def __init__(self):
        self.ai_client = AIModelClient()

    # ─────────────────────────────────────────────────────────────────────
    # 辅助方法
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _detect_timeframe(text: str) -> str:
        """从文本片段中检测时间周期"""
        if re.search(r'长期|月线|月(?!均线)|m_|月周期', text):
            return 'month'
        if re.search(r'中期|周线|周(?!期)|w_|周周期', text):
            return 'week'
        return 'day'  # 短期/日线/默认

    @staticmethod
    def _detect_intensity(text: str) -> str:
        """检测程度副词"""
        if re.search(r'远远|极度|严重|非常|特别|极其|超级|深度|高度|十分|完全|激增|急剧', text):
            return 'extreme'
        if re.search(r'很|明显|相当|比较|较为|颇为|大幅|大大|大量|显著', text):
            return 'strong'
        return 'mild'

    @staticmethod
    def _detect_action(text: str):
        """检测操作方向：buy/sell/exclude/hold/None。
        注意：不匹配「超买/超卖」中的「买/卖」，只识别明确的操作词。"""
        if re.search(r'不买|不.*买入|不做|不操作|回避|禁止|避免|排除|屏蔽', text):
            return 'exclude'
        # 使用明确词，避免「超卖」「超买」误匹配
        if re.search(r'卖出|卖掉|平仓|减仓|离场|出场|做空|止盈|止损', text):
            return 'sell'
        if re.search(r'买入|建仓|加仓|入场|开仓|做多', text):
            return 'buy'
        if re.search(r'持有|持仓|观望|等待', text):
            return 'hold'
        return None

    @staticmethod
    def _detect_position(text: str) -> float:
        """从文本中提取仓位比例，忽略百分位/历史分位表达"""
        # 先去掉百分位表达，避免 "RSI低于95%的时间" → position=0.95
        cleaned = re.sub(r'\d+\s*%\s*(?:的时间|历史分位|分位以?[上下]|以上|以下|低位|高位)', '', text)
        cleaned = re.sub(r'(?:低于|高于|超过|不足|处于)\s*\d+\s*%', '', cleaned)
        m = re.search(r'(\d+)\s*%', cleaned)
        if m:
            return max(0.0, min(1.0, float(m.group(1)) / 100))
        if re.search(r'全仓|满仓|全部', text):
            return 1.0
        if re.search(r'半仓|一半', text):
            return 0.5
        if re.search(r'轻仓|小仓|少量', text):
            return 0.3
        return 0.5

    @staticmethod
    def _rsi_period_var(text: str, pfx: str) -> str:
        """根据文本选择RSI周期变量名"""
        if re.search(r'24|长周期|rsi24|rsi_24', text, re.I):
            return f'{pfx}rsi_24'
        if re.search(r'12|中周期|rsi12|rsi_12', text, re.I):
            return f'{pfx}rsi_12'
        return f'{pfx}rsi_6'

    @staticmethod
    def _describe_cond(cond: str) -> str:
        """将条件表达式转为简短中文说明"""
        desc = cond
        _map = [
            # 百分位变量（前缀变体必须在基础变体之前，防止被子串规则误替换）
            (r'w_rsi6_pct100', '周RSI百分位(100周)'),
            (r'm_rsi6_pct100', '月RSI百分位(100月)'),
            (r'rsi6_pct100',   '日RSI百分位(100日)'),
            (r'pettm_pct10y',  'PETTM百分位(10年)'),
            # 大盘相对强弱
            (r'rel_strength_5',  '5日超额收益'),
            (r'rel_strength_10', '10日超额收益'),
            (r'rel_strength_20', '20日超额收益'),
            (r'rel_strength_60', '60日超额收益'),
            (r'idx_pct_chg',     '大盘日涨跌幅'),
            (r'idx_ret_5',       '大盘5日涨幅'),
            (r'idx_ret_10',      '大盘10日涨幅'),
            (r'idx_ret_20',      '大盘20日涨幅'),
            (r'idx_ret_60',      '大盘60日涨幅'),
            # 普通指标（前缀变体必须在基础变体之前）
            (r'w_rsi_6', '周RSI(6)'),   (r'w_rsi_12','周RSI(12)'), (r'w_rsi_24','周RSI(24)'),
            (r'm_rsi_6', '月RSI(6)'),   (r'm_rsi_12','月RSI(12)'), (r'm_rsi_24','月RSI(24)'),
            (r'rsi_6',   '日RSI(6)'),   (r'rsi_12',  '日RSI(12)'), (r'rsi_24',  '日RSI(24)'),
            (r'w_macd_histogram','周MACD柱'), (r'm_macd_histogram','月MACD柱'), (r'macd_histogram', 'MACD柱'),
            (r'w_kdj_j', '周KDJ-J'),   (r'm_kdj_j', '月KDJ-J'),  (r'kdj_j',   'KDJ-J'),
            (r'w_boll_position','周布林位置'), (r'm_boll_position','月布林位置'), (r'boll_position','布林位置'),
            (r'w_volume_ratio','周量比'), (r'volume_ratio','量比'),
            (r'w_wr_14','周威廉%R'),    (r'm_wr_14','月威廉%R'),   (r'wr_14','威廉%R'),
            (r'w_overall_score','周综合评分'),(r'm_overall_score','月综合评分'),(r'overall_score','综合评分'),
            (r'pe_ttm','市盈率PE'),
            (r'news_sentiment','新闻情感'),  (r'news_positive','正面新闻数'),
            (r'news_count',   '新闻总数'),
            (r'close','收盘价'), (r'ma_20','20均'), (r'ma_60','60均'), (r'ma_5','5均'),
        ]
        for pat, zh in _map:
            desc = re.sub(pat, zh, desc)
        desc = desc.replace(' and ', ' 且 ').replace(' or ', ' 或 ')
        return desc

    def _parse_fragment(self, fragment: str) -> List[str]:
        """
        解析单个文本片段，返回条件表达式列表（同一片段内的所有条件将用 and 拼接）
        """
        tf = self._detect_timeframe(fragment)
        pfx = self._TF_PREFIX[tf]
        intensity = self._detect_intensity(fragment)
        conds = []

        # ── 百分位表达处理（优先级最高）─────────────────────────────────────
        # 将"RSI低于95%的时间"直接映射到 rsi6_pct100 < 95（而非旧的绝对RSI阈值）
        # 将"PETTM低于30%的历史"映射到 pettm_pct10y < 30
        _pct_pattern_low = re.search(
            r'(?:低于|小于|不超过|处于.*?以下)\s*(?:历史|历史上|过去)?\s*(\d+)\s*%\s*(?:的时间|以下|的历史|分位|分位以下|历史低)?'
            r'|历史\s*(\d+)\s*%\s*(?:以下|低位|的时间)',
            fragment)
        _pct_pattern_high = re.search(
            r'(?:高于|大于|超过|处于.*?以上)\s*(?:历史|历史上|过去)?\s*(\d+)\s*%\s*(?:的时间|以上|的历史|分位|分位以上|历史高)?'
            r'|历史\s*(\d+)\s*%\s*(?:以上|高位|的时间)',
            fragment)

        if _pct_pattern_low or _pct_pattern_high:
            m = _pct_pattern_low or _pct_pattern_high
            pct_val = int(next(g for g in m.groups() if g is not None))
            is_low = _pct_pattern_low is not None

            # 确定目标变量：PETTM > RSI > KDJ > 默认RSI百分位
            if re.search(r'pe|pettm|pe_ttm|市盈率|估值', fragment, re.I):
                var = 'pettm_pct10y'
            elif re.search(r'kdj|kd(?!\d)|随机指标', fragment, re.I):
                # KDJ 没有百分位列，回退到绝对值
                var_j = f'{pfx}kdj_j'
                thresh_map = self._KDJ_J_OVERSOLD if is_low else self._KDJ_J_OVERBOUGHT
                # 将百分位粗略映射到强度
                it = 'extreme' if (pct_val <= 10 if is_low else pct_val >= 90) else 'mild'
                op = '<' if is_low else '>'
                conds.append(f'{var_j} {op} {thresh_map[it]}')
                var = None
            else:
                # RSI 百分位 → 使用 rsi6_pct100 / w_rsi6_pct100 / m_rsi6_pct100
                if tf == 'week':
                    var = 'w_rsi6_pct100'
                elif tf == 'month':
                    var = 'm_rsi6_pct100'
                else:
                    var = 'rsi6_pct100'

            if var:
                op = '<' if is_low else '>'
                conds.append(f'{var} {op} {pct_val}')

            _pct_handled = True
        else:
            _pct_handled = False

        # ── RSI ────────────────────────────────────────────────
        if not _pct_handled and re.search(r'rsi|强弱指|相对强弱', fragment, re.I):
            var = self._rsi_period_var(fragment, pfx)
            # 优先匹配明确数值阈值（如"RSI低于10"、"RSI高于70"）
            _m_low  = re.search(r'(?:低于|小于|<=?)\s*(\d+(?:\.\d+)?)\s*(?!%)', fragment)
            _m_high = re.search(r'(?:高于|大于|超过|>=?)\s*(\d+(?:\.\d+)?)\s*(?!%)', fragment)
            if _m_low:
                conds.append(f'{var} < {_m_low.group(1)}')
            elif _m_high:
                conds.append(f'{var} > {_m_high.group(1)}')
            elif re.search(r'超跌|超卖|过低|偏低|低位|跌深|深度回调|弱势|回调', fragment):
                conds.append(f'{var} < {self._RSI_OVERSOLD[tf][intensity]}')
            elif re.search(r'超买|超涨|过高|偏高|高位|强势|上涨', fragment):
                conds.append(f'{var} > {self._RSI_OVERBOUGHT[tf][intensity]}')

        # ── PETTM 直接数值比较（非百分位）─────────────────────────────────
        if not _pct_handled and re.search(r'pe_ttm|pettm|市盈率', fragment, re.I):
            m_num = re.search(r'(?:低于|小于|<)\s*(\d+(?:\.\d+)?)', fragment)
            m_num_h = re.search(r'(?:高于|大于|>)\s*(\d+(?:\.\d+)?)', fragment)
            if m_num:
                conds.append(f'pe_ttm < {m_num.group(1)}')
            elif m_num_h:
                conds.append(f'pe_ttm > {m_num_h.group(1)}')

        # ── KDJ ────────────────────────────────────────────────
        if not _pct_handled and re.search(r'kdj|kd(?!\d)|随机指标', fragment, re.I):
            var_j = f'{pfx}kdj_j'
            _m_kl = re.search(r'(?:低于|小于|<=?)\s*(\d+(?:\.\d+)?)\s*(?!%)', fragment)
            _m_kh = re.search(r'(?:高于|大于|>=?)\s*(\d+(?:\.\d+)?)\s*(?!%)', fragment)
            if _m_kl:
                conds.append(f'{var_j} < {_m_kl.group(1)}')
            elif _m_kh:
                conds.append(f'{var_j} > {_m_kh.group(1)}')
            elif re.search(r'超跌|超卖|低位|弱势', fragment):
                conds.append(f'{var_j} < {self._KDJ_J_OVERSOLD[intensity]}')
            elif re.search(r'超买|超涨|高位|强势', fragment):
                conds.append(f'{var_j} > {self._KDJ_J_OVERBOUGHT[intensity]}')

        # ── MACD ───────────────────────────────────────────────
        if re.search(r'macd|指数平滑均线|异同移动', fragment, re.I):
            var_h = f'{pfx}macd_histogram'
            if re.search(r'金叉|向上|正值|多头|上穿', fragment):
                conds.append(f'{var_h} > 0')
            elif re.search(r'死叉|向下|负值|空头|下穿', fragment):
                conds.append(f'{var_h} < 0')

        # ── 均线 ───────────────────────────────────────────────
        if re.search(r'均线|移动平均|ma\d|ma_\d|年线|季线|月均', fragment, re.I):
            close_v = 'close' if tf == 'day' else f'{pfx}close'
            if   re.search(r'250|年线', fragment): ma_v = f'{pfx}ma_250'
            elif re.search(r'120|半年', fragment): ma_v = f'{pfx}ma_120'
            elif re.search(r'60|季',   fragment): ma_v = f'{pfx}ma_60'
            elif re.search(r'20|月均', fragment): ma_v = f'{pfx}ma_20'
            elif re.search(r'10',      fragment): ma_v = f'{pfx}ma_10'
            elif re.search(r'5|周均',  fragment): ma_v = f'{pfx}ma_5'
            else:                                  ma_v = f'{pfx}ma_20'
            if re.search(r'上穿|突破|金叉|站上|高于|大于|超过', fragment):
                conds.append(f'{close_v} > {ma_v}')
            elif re.search(r'下穿|跌破|死叉|跌至|低于|小于|不足', fragment):
                conds.append(f'{close_v} < {ma_v}')

        # ── 布林带 ─────────────────────────────────────────────
        if re.search(r'布林|boll|通道|带(?:内|外)', fragment, re.I):
            var_p = f'{pfx}boll_position'
            if re.search(r'下轨|下带|触底|低位|超跌', fragment):
                conds.append(f'{var_p} < 0.2')
            elif re.search(r'上轨|上带|触顶|高位|超涨', fragment):
                conds.append(f'{var_p} > 0.8')

        # ── 成交量 ─────────────────────────────────────────────
        if re.search(r'成交量|量比|放量|缩量|量能', fragment, re.I):
            var_vr = f'{pfx}volume_ratio'
            if re.search(r'放量|大量|巨量|上升', fragment):
                thresh = {'extreme': 3.0, 'strong': 2.5, 'mild': 2.0}[intensity]
                conds.append(f'{var_vr} > {thresh}')
            elif re.search(r'缩量|小量|不足|萎缩', fragment):
                conds.append(f'{var_vr} < 0.5')

        # ── 威廉指标 ───────────────────────────────────────────
        if re.search(r'威廉|wr|williams', fragment, re.I):
            var_wr = f'{pfx}wr_14'
            if re.search(r'超跌|超卖', fragment):
                conds.append(f'{var_wr} < {self._WR_OVERSOLD[intensity]}')
            elif re.search(r'超买|超涨', fragment):
                conds.append(f'{var_wr} > {self._WR_OVERBOUGHT[intensity]}')

        # ── 大盘相对强弱（比大盘强/跑赢大盘 → rel_strength_N；大盘涨跌 → idx_ret_N）────
        _rel_match = re.search(
            r'比大盘(?:强|弱|好|差)|跑赢大盘|跑输大盘|强于大盘|弱于大盘|超越大盘|落后大盘|'
            r'走势.*比.*大盘|相对大盘(?:偏?强|偏?弱)',
            fragment, re.I)
        _idx_dir_match = re.search(
            r'大盘(?:上涨|上行|走强|涨|收涨)|大盘(?:下跌|下行|走弱|跌|收跌)',
            fragment, re.I)
        if _rel_match or _idx_dir_match:
            # 确定比较窗口期
            if re.search(r'短期|近5日|5日|5天', fragment):
                _rn = 5
            elif re.search(r'近10日|10日|10天', fragment):
                _rn = 10
            elif re.search(r'中期|近20日?|20日|20天|一个月', fragment):
                _rn = 20
            elif re.search(r'长期|近60日?|60日|60天|季度', fragment):
                _rn = 60
            else:
                _rn = 5  # 默认短期
            if _rel_match:
                _rel_var = f'rel_strength_{_rn}'
                _m_pct = re.search(r'(\d+(?:\.\d+)?)\s*%', fragment)
                if re.search(r'比大盘强|跑赢大盘|强于大盘|超越大盘|走势.*比.*大盘.*强', fragment, re.I):
                    if _m_pct:
                        conds.append(f'{_rel_var} > {float(_m_pct.group(1))}')
                    else:
                        _t = {'extreme': 5.0, 'strong': 3.0, 'mild': 0.0}[intensity]
                        conds.append(f'{_rel_var} > {_t}')
                elif re.search(r'比大盘弱|跑输大盘|弱于大盘|落后大盘', fragment, re.I):
                    if _m_pct:
                        conds.append(f'{_rel_var} < -{float(_m_pct.group(1))}')
                    else:
                        _t = {'extreme': 5.0, 'strong': 3.0, 'mild': 0.0}[intensity]
                        conds.append(f'{_rel_var} < -{_t}')
            if _idx_dir_match:
                _idx_var = f'idx_ret_{_rn}'
                if re.search(r'大盘(?:上涨|上行|走强|涨|收涨)', fragment, re.I):
                    conds.append(f'{_idx_var} > 0')
                elif re.search(r'大盘(?:下跌|下行|走弱|跌|收跌)', fragment, re.I):
                    conds.append(f'{_idx_var} < 0')

        # ── 大盘市场情绪（使用指数涨跌幅，不使用 overall_score）─────────
        # idx_ret_20: 上证指数近20日涨跌幅；rel_strength_5: 个股相对大盘5日超额
        if re.search(r'大盘|市场(?:整体|环境|情绪)?|整体市场', fragment, re.I) and not (_rel_match or _idx_dir_match):
            negated = bool(re.search(r'没有|不是|不(?:太|很|算|那么)|未(?:见|呈)', fragment))
            if re.search(r'悲观|看空|熊市|崩盘|极差', fragment, re.I):
                # 极端: 近20日跌>15%  强烈: 跌>10%  轻度: 跌>5%
                thresh = {'extreme': -15, 'strong': -10, 'mild': -5}[intensity]
                if negated:
                    conds.append(f'idx_ret_20 > {thresh}')   # 大盘没有很悲观 → 指数跌幅不超阈值
                else:
                    conds.append(f'idx_ret_20 < {thresh}')
            elif re.search(r'乐观|看多|牛市|好|强|涨', fragment, re.I):
                # 极端: 近20日涨>8%  强烈: 涨>4%  轻度: 涨>1%
                thresh = {'extreme': 8, 'strong': 4, 'mild': 1}[intensity]
                if negated:
                    conds.append(f'idx_ret_20 < {thresh}')
                else:
                    conds.append(f'idx_ret_20 > {thresh}')

        # ── 新闻（增强版：支持数量统计和情感方向）────────────────────────
        if re.search(r'新闻|消息|情感|舆论|利好|利空', fragment, re.I):
            if re.search(r'正面新闻(?:数量|数目|条数)?|利好新闻(?:数量|数目|条数)?', fragment):
                thresh = {'extreme': 5, 'strong': 4, 'mild': 3}[intensity]
                conds.append(f'news_positive >= {thresh}')
            elif re.search(r'新闻数量|消息数量|新闻数目|新闻(?:条数|数目)', fragment):
                _nc_thresh = {'extreme': 8, 'strong': 5, 'mild': 3}[intensity]
                conds.append(f'news_count >= {_nc_thresh}')
            elif re.search(r'正面|利好|积极|看好', fragment):
                thresh = {'extreme': 0.5, 'strong': 0.35, 'mild': 0.2}[intensity]
                conds.append(f'news_sentiment > {thresh}')
            elif re.search(r'负面|利空|消极|看空', fragment):
                thresh = {'extreme': -0.5, 'strong': -0.35, 'mild': -0.2}[intensity]
                conds.append(f'news_sentiment < {thresh}')

        # ── 综合评分 ───────────────────────────────────────────
        if re.search(r'综合评分|overall_score|score', fragment, re.I):
            var_s = f'{pfx}overall_score'
            if re.search(r'低|差|弱|负|看空', fragment):
                thresh = {'extreme': -60, 'strong': -40, 'mild': -20}[intensity]
                conds.append(f'{var_s} < {thresh}')
            elif re.search(r'高|好|强|正|看多', fragment):
                thresh = {'extreme': 60, 'strong': 40, 'mild': 20}[intensity]
                conds.append(f'{var_s} > {thresh}')

        # ── 兜底：当没有检测到具体指标时，「超卖/超跌」→ RSI超卖，「超买/超涨」→ RSI超买 ──
        if not conds:
            if re.search(r'超跌|超卖|深度回调|极度低估', fragment):
                var = f'{pfx}rsi_6'
                conds.append(f'{var} < {self._RSI_OVERSOLD[tf][intensity]}')
            elif re.search(r'超买|超涨|高度偏高|极度高估', fragment):
                var = f'{pfx}rsi_6'
                conds.append(f'{var} > {self._RSI_OVERBOUGHT[tf][intensity]}')

        return conds

    @staticmethod
    def _split_clauses_with_connectors(text: str):
        """
        将自然语言按连接词分割，返回 [(clause_text, connector_before), ...]。
        connector_before='OR' 表示此子句与上一子句为 OR 关系（第一子句始终为 OR）。
        connector_before='AND' 表示此子句与上一子句为 AND 关系。
        连接词规则：
          - "且/并且/而且/同时/但是/然而/不过/只是/除非" → AND
          - "或/或者" → OR
          - 逗号/分号/换行/顿号 → AND（策略中连续条件通常为 AND 关系）
        """
        normalized = text
        # AND 类连词：并列条件 + 转折词（但是/然而/不过 等均视为 AND 关系）
        normalized = re.sub(
            r'[,，;；\n、]?\s*(且|并且|而且|同时|但是|然而|不过|只是|除非|否则)\s*',
            ' __AND__ ', normalized)
        normalized = re.sub(r'[,，;；\n、]?\s*(或者?)\s*', ' __OR__ ', normalized)
        normalized = re.sub(r'[,，;；\n、]+', ' __AND__ ', normalized)

        parts = re.split(r'(__AND__|__OR__)', normalized)
        result = []
        next_conn = 'OR'
        for part in parts:
            stripped = part.strip()
            if stripped == '__AND__':
                next_conn = 'AND'
            elif stripped == '__OR__':
                next_conn = 'OR'
            elif stripped:
                result.append((stripped, next_conn))
                next_conn = 'AND'
        return result

    def _smart_local_parse(self, description: str):
        """
        智能本地解析器：将中文自然语言策略描述转换为量化规则。
        每个子句生成一条独立规则，并按 AND/OR 连接词设置 connector 字段。
        返回 (rules: List[StrategyRule], exclusion_rules: List[StrategyRule])
        """
        rules: List[StrategyRule] = []
        excl_rules: List[StrategyRule] = []

        overall_action_raw = self._detect_action(description)
        overall_action = overall_action_raw if overall_action_raw in ('buy', 'sell') else 'buy'
        position_ratio = self._detect_position(description)

        clauses_with_conn = self._split_clauses_with_connectors(description)

        # 按动作方向分桶，每桶保存 (cond_str, reason, connector)
        buy_items: List[tuple] = []
        sell_items: List[tuple] = []
        excl_items: List[tuple] = []

        for clause, conn in clauses_with_conn:
            clause_action = self._detect_action(clause)
            frags = self._parse_fragment(clause)
            if not frags:
                continue

            cond_str = ' and '.join(frags)
            reason = self._describe_cond(cond_str) or clause[:40]

            if clause_action == 'exclude':
                excl_items.append((cond_str, reason, conn))
            elif clause_action == 'sell':
                sell_items.append((cond_str, reason, conn))
            elif clause_action == 'buy':
                buy_items.append((cond_str, reason, conn))
            else:
                # 无明确动作 → 归入全局方向
                if overall_action == 'sell':
                    sell_items.append((cond_str, reason, conn))
                elif overall_action == 'exclude':
                    excl_items.append((cond_str, reason, conn))
                else:
                    buy_items.append((cond_str, reason, conn))

        # 将同向规则列表转换为 StrategyRule（第一条 connector 固定为 OR）
        def _emit_rules(items, action, ratio):
            out = []
            for i, (cond, reason, conn) in enumerate(items):
                out.append(StrategyRule(
                    condition=cond, action=action,
                    position_ratio=ratio, reason=reason,
                    connector='OR' if i == 0 else conn))
            return out

        rules.extend(_emit_rules(buy_items, 'buy', position_ratio))
        rules.extend(_emit_rules(sell_items, 'sell', position_ratio))
        for i, (cond, reason, conn) in enumerate(excl_items):
            excl_rules.append(StrategyRule(
                condition=cond, action='hold', position_ratio=0,
                reason=reason, connector='OR' if i == 0 else conn))

        # 如果整体动作是 buy 但 buy_items 为空（例如所有子句无条件），追加兜底规则
        if overall_action == 'buy' and not buy_items and not sell_items and not excl_items:
            rules.append(StrategyRule(
                condition='True', action='hold', position_ratio=0,
                reason='未能识别具体量化条件，保持观望'))

        if not rules and not excl_rules:
            rules.append(StrategyRule(
                condition='True', action='hold', position_ratio=0,
                reason='未能识别具体量化条件，保持观望'))

        return rules, excl_rules

    def parse_natural_language(self, description: str):
        """
        将自然语言策略描述转换为量化规则。
        返回 (rules: List[StrategyRule], exclusion_rules: List[StrategyRule])

        优先调用 AI；AI 不可用或返回格式错误时，自动切换到智能本地解析器。
        """
        # ---- 构建 AI 提示（请求同时返回正面规则和负面清单）----
        prompt = f"""你是一个量化交易策略专家，请将以下自然语言交易策略转换为结构化量化规则。

策略描述：
{description}

=== 可用指标变量（严格使用以下名称）===
日线: close,open,high,low,volume,amount,pct_chg | rsi_6,rsi_12,rsi_24 | macd,macd_signal,macd_histogram | kdj_k,kdj_d,kdj_j | boll_upper,boll_middle,boll_lower,boll_position | ma_5,ma_10,ma_20,ma_60,ma_120,ma_250 | volume_ratio,wr_14,volatility,overall_score | pe_ttm,pb
百分位日线: rsi6_pct100(日线RSI_6在过去100日的百分位,0~100),pettm_pct10y(PE_TTM在过去10年的百分位,0~100)
周线(前缀w_): w_rsi_6,w_rsi_12,w_rsi_24,w_macd,w_macd_histogram,w_kdj_j,w_boll_position,w_ma_5,w_ma_20,w_ma_60,w_volume_ratio,w_wr_14,w_overall_score,w_close
百分位周线: w_rsi6_pct100(周线RSI_6在过去100周的百分位,0~100)
月线(前缀m_): m_rsi_6,m_rsi_12,m_rsi_24,m_macd,m_macd_histogram,m_kdj_j,m_boll_position,m_ma_5,m_ma_20,m_ma_60,m_volume_ratio,m_wr_14,m_overall_score,m_close
百分位月线: m_rsi6_pct100(月线RSI_6在过去100月的百分位,0~100)
新闻: news_sentiment(-1~1),news_count,news_positive(0~1)
大盘/指数（⚠️大盘相关条件必须使用这里的变量，禁止使用overall_score）: idx_pct_chg(当日涨跌幅) | idx_ret_5,idx_ret_10,idx_ret_20,idx_ret_60(近N日累计涨跌幅,%) | rel_strength_5,rel_strength_10,rel_strength_20,rel_strength_60(个股相对大盘N日超额收益,%)

⚠️ 重要区别：
- overall_score = 个股自身技术综合得分（RSI+MACD+KDJ+均线），仅代表**该股票**的技术状态，不是大盘指标
- 大盘乐观/悲观/情绪 必须使用 idx_ret_20（近20日指数涨跌幅），禁止用 overall_score 表示市场/大盘状态

=== 百分位变量说明 ===
- "RSI低于95%的时间" / "RSI处于历史低位（95%时间以上比这更高）" → rsi6_pct100 < 95
- "周线RSI低于80%的时间" → w_rsi6_pct100 < 80
- "PETTM处于历史30%以下" → pettm_pct10y < 30
- 百分位值越低说明当前值在历史中越低，适合捕捉超跌机会

=== 关键阈值参考 ===
日线RSI超卖:<20(轻度)/<15(极度) | 日线RSI超买:>80(轻度)/>85(极度)
周线RSI超卖:<25(轻度)/<20(极度) | 月线RSI超卖:<25(轻度)/<20(极度)
KDJ_J超卖:<20 | KDJ_J超买:>80 | 布林下轨:boll_position<0.2 | 量比放量:volume_ratio>2
大盘乐观:idx_ret_20>4（近20日涨幅，用指数数据） | 大盘悲观:idx_ret_20<-10（近20日跌幅，用指数数据）

=== 趋势结构与波浪形态翻译指南 ===
"多头排列 / 均线向上排列 / 趋势向上": ma_5 > ma_20 and ma_20 > ma_60
"一浪比一浪高 / 向上趋势形成": ma_5 > ma_20 and ma_20 > ma_60（等同多头排列）
"回调不破前期高点 / 回调不破支撑": close > ma_20（价格在20均线之上）
"突破点买入 / 价格突破": boll_position > 0.8 or (pct_chg > 2 and close > ma_20)
"回调点买入 / 逢回调买入": close > ma_60 and close < ma_20（回调至ma60-ma20之间，多头排列下）
"板块或大盘乐观 / 市场乐观": idx_ret_20 > 4（大盘近20日涨幅超4%，用指数数据；中长线可用 idx_ret_60 > 5）
"中线 / 中期趋势": 优先使用周线指标(w_前缀)，如 w_ma_5 > w_ma_20 表示中期上升趋势
"周线多头排列 / 中线向上": w_ma_5 > w_ma_20 and w_ma_20 > w_ma_60
"周线回调买入": w_close > w_ma_20 and w_boll_position < 0.5（周线回调但仍在均线上方）

=== 复合趋势买入条件（示例）===
"向上趋势中突破买入": condition="ma_5 > ma_20 and ma_20 > ma_60 and boll_position > 0.8 and idx_ret_20 > 1"
"向上趋势中回调买入": condition="ma_5 > ma_20 and close > ma_60 and close < ma_20 and idx_ret_20 > -5"
"中线超跌+大盘乐观买入": condition="w_rsi6_pct100 < 30 and ma_5 > ma_60 and idx_ret_20 > -5"

=== 输出格式（严格JSON，不含注释）===
{{
  "rules": [
    {{"condition": "rsi6_pct100 < 95", "action": "buy", "position_ratio": 0.5, "reason": "日线RSI处于历史低位（95%时间以上比此更高）", "connector": "OR"}},
    {{"condition": "w_rsi6_pct100 < 90", "action": "buy", "position_ratio": 0.5, "reason": "周线RSI百分位较低", "connector": "AND"}}
  ],
  "exclusion_rules": [
    {{"condition": "idx_ret_20 < -15 and rel_strength_5 < -3", "reason": "大盘近20日跌超15%且个股相对更弱，等待市场企稳", "action": "hold", "position_ratio": 0, "connector": "OR"}}
  ]
}}
"""

        rules: List[StrategyRule] = []
        excl_rules: List[StrategyRule] = []
        ai_succeeded = False

        try:
            response = self.ai_client.call(prompt)
            logger.debug(f"AI原始响应: {str(response)[:500]}")

            # 提取 JSON
            parsed = None
            for start_ch, end_ch in (('{', '}'), ('[', ']')):
                s = response.find(start_ch)
                e = response.rfind(end_ch)
                if s != -1 and e > s:
                    try:
                        parsed = json.loads(response[s:e+1])
                        break
                    except Exception:
                        pass

            def _build_rules(data_list) -> List[StrategyRule]:
                result = []
                for item in (data_list or []):
                    if not isinstance(item, dict):
                        continue
                    try:
                        pr = float(item.get('position_ratio', 0.5))
                        if pr > 1.0:
                            pr /= 100.0
                        pr = max(0.0, min(1.0, pr))
                        result.append(StrategyRule(
                            condition=item.get('condition', 'True'),
                            action=item.get('action', 'hold'),
                            position_ratio=pr,
                            reason=item.get('reason', ''),
                        ))
                    except Exception as e2:
                        logger.warning(f"规则解析跳过: {e2}, data={item}")
                return result

            if isinstance(parsed, dict) and ('rules' in parsed or 'exclusion_rules' in parsed):
                rules = _build_rules(parsed.get('rules', []))
                excl_rules = _build_rules(parsed.get('exclusion_rules', []))
                ai_succeeded = bool(rules or excl_rules)
            elif isinstance(parsed, list) and all(isinstance(x, dict) for x in parsed):
                rules = _build_rules(parsed)
                ai_succeeded = bool(rules)

        except Exception as e:
            logger.warning(f"AI调用失败，使用本地解析器: {e}")

        # ---- 如果 AI 没有返回有效规则，使用智能本地解析器 ----
        if not ai_succeeded:
            logger.info('AI未返回有效规则，使用智能本地解析器')
            rules, excl_rules = self._smart_local_parse(description)

        return rules, excl_rules


    
    def translate_to_natural_language(self, rules: List[StrategyRule]) -> str:
        """
        将量化规则翻译为自然语言
        
        Args:
            rules: 策略规则列表
        
        Returns:
            自然语言描述
        """
        rules_json = json.dumps([asdict(r) for r in rules], ensure_ascii=False, indent=2)
        
        prompt = f"""
请将以下量化交易规则转换为通俗易懂的自然语言策略描述：

规则列表：
{rules_json}

请输出：
1. 策略的整体描述
2. 每条规则的自然语言解释
3. 策略的风险提示
"""
        
        try:
            response = self.ai_client.call(prompt)
            return response
        except Exception as e:
            logger.error(f"翻译策略失败: {e}")
            return "策略翻译失败"


class QuantStrategy:
    """量化策略"""
    
    def __init__(self, name: str = "", description: str = ""):
        self.name = name
        self.description = description
        self.rules: List[StrategyRule] = []
        self.exclusion_rules: List[StrategyRule] = []
        self.max_position_ratio: float = 1.0  # 该策略允许的最大仓位上限 (0-1)
        self.market_regime: List[str] = []     # 适用的大盘阶段，空列表=全阶段适用
        self.parser = StrategyParser()
    
    def from_natural_language(self, description: str):
        """从自然语言创建策略"""
        self.description = description
        rules, excl_rules = self.parser.parse_natural_language(description)
        self.rules = rules
        self.exclusion_rules = excl_rules
        return self
    
    def from_rules(self, rules: List[StrategyRule]):
        """从规则创建策略"""
        self.rules = rules
        return self
    
    def add_rule(self, condition: str, action: str, 
                 position_ratio: float = 1.0, reason: str = "", connector: str = 'OR'):
        """添加规则"""
        self.rules.append(StrategyRule(
            condition=condition,
            action=action,
            position_ratio=position_ratio,
            reason=reason,
            connector=connector
        ))
        return self
    
    def add_exclusion_rule(self, condition: str, reason: str = "", connector: str = 'OR'):
        """添加排除规则（负面清单），触发时阻止任何交易信号"""
        self.exclusion_rules.append(StrategyRule(
            condition=condition,
            action="hold",
            position_ratio=0,
            reason=reason,
            connector=connector
        ))
        return self
    
    def to_natural_language(self) -> str:
        """转换为自然语言"""
        return self.parser.translate_to_natural_language(self.rules)
    
    def evaluate_condition(self, condition: str, indicators: Dict) -> bool:
        """
        评估条件
        
        Args:
            condition: 条件表达式
            indicators: 指标字典
        
        Returns:
            条件是否满足
        """
        try:
            # 创建安全的评估环境（变量名作为局部变量传入eval）
            # ---- 日线指标 ----
            safe_locals = {
                # 价格与成交量
                'close': indicators.get('close', indicators.get('price', 0)),
                'price': indicators.get('price', indicators.get('close', 0)),
                'open': indicators.get('open', 0),
                'high': indicators.get('high', 0),
                'low': indicators.get('low', 0),
                'volume': indicators.get('volume', 0),
                'amount': indicators.get('amount', 0),
                'pct_chg': indicators.get('pct_chg', 0),
                'change': indicators.get('change', 0),
                # 基本面
                'pe_ttm': indicators.get('pe_ttm', 0),
                'pb': indicators.get('pb', 0),
                # 百分位指标（日线 RSI_6 在过去100日的百分位，PE在过去10年的百分位）
                'rsi6_pct100': indicators.get('rsi6_pct100', 50),
                'pettm_pct10y': indicators.get('pettm_pct10y', 50),
                # RSI
                'rsi_6': indicators.get('rsi_6', 50),
                'rsi_12': indicators.get('rsi_12', 50),
                'rsi_24': indicators.get('rsi_24', 50),
                # MACD
                'macd': indicators.get('macd', 0),
                'macd_signal': indicators.get('macd_signal', 0),
                'macd_histogram': indicators.get('macd_histogram', 0),
                # KDJ
                'kdj_k': indicators.get('kdj_k', 50),
                'kdj_d': indicators.get('kdj_d', 50),
                'kdj_j': indicators.get('kdj_j', 50),
                # 布林带
                'boll_upper': indicators.get('boll_upper', 0),
                'boll_middle': indicators.get('boll_middle', 0),
                'boll_lower': indicators.get('boll_lower', 0),
                'boll_position': indicators.get('boll_position', 0.5),
                # 均线 (支持 ma_5 和 ma5 两种写法)
                'ma_5': indicators.get('ma_5', indicators.get('ma5', 0)),
                'ma_10': indicators.get('ma_10', indicators.get('ma10', 0)),
                'ma_20': indicators.get('ma_20', indicators.get('ma20', 0)),
                'ma_60': indicators.get('ma_60', indicators.get('ma60', 0)),
                'ma_120': indicators.get('ma_120', indicators.get('ma120', 0)),
                'ma_250': indicators.get('ma_250', indicators.get('ma250', 0)),
                # 成交量指标
                'volume_ratio': indicators.get('volume_ratio', 1),
                'volume_ma_5': indicators.get('volume_ma_5', 0),
                'volume_ma_20': indicators.get('volume_ma_20', 0),
                # 其他技术指标
                'wr_14': indicators.get('wr_14', -50),
                'volatility': indicators.get('volatility', 0),
                'overall_score': indicators.get('overall_score', 0),
            }
            # 均线别名：ma5 等同于 ma_5
            safe_locals['ma5'] = safe_locals['ma_5']
            safe_locals['ma10'] = safe_locals['ma_10']
            safe_locals['ma20'] = safe_locals['ma_20']
            safe_locals['ma60'] = safe_locals['ma_60']
            safe_locals['ma120'] = safe_locals['ma_120']
            safe_locals['ma250'] = safe_locals['ma_250']

            # ---- 周线指标（前缀 w_）----
            _w_indicator_cols = [
                'close', 'open', 'high', 'low', 'volume', 'amount',
                'rsi_6', 'rsi_12', 'rsi_24',
                'macd', 'macd_signal', 'macd_histogram',
                'kdj_k', 'kdj_d', 'kdj_j',
                'boll_upper', 'boll_middle', 'boll_lower', 'boll_position',
                'ma_5', 'ma_20', 'ma_60',
                'volume_ratio', 'volume_ma_5', 'volume_ma_20',
                'wr_14', 'volatility', 'overall_score',
                'rsi6_pct100',  # RSI_6 百分位（周线: w_rsi6_pct100 / 月线: m_rsi6_pct100）
            ]
            _w_defaults = {'rsi_6': 50, 'rsi_12': 50, 'rsi_24': 50,
                           'kdj_k': 50, 'kdj_d': 50, 'kdj_j': 50,
                           'boll_position': 0.5, 'volume_ratio': 1, 'wr_14': -50}
            for _col in _w_indicator_cols:
                _default = _w_defaults.get(_col, 0)
                safe_locals[f'w_{_col}'] = indicators.get(f'w_{_col}', _default)
            # 周线均线别名
            safe_locals['w_price'] = safe_locals['w_close']

            # ---- 月线指标（前缀 m_）----
            for _col in _w_indicator_cols:
                _default = _w_defaults.get(_col, 0)
                safe_locals[f'm_{_col}'] = indicators.get(f'm_{_col}', _default)
            safe_locals['m_price'] = safe_locals['m_close']

            # ---- 新闻情感指标 ----
            safe_locals['news_sentiment'] = indicators.get('news_sentiment', 0)
            safe_locals['news_count'] = indicators.get('news_count', 0)
            safe_locals['news_positive'] = indicators.get('news_positive', 0.5)

            # ---- 大盘相对强弱指标 ----
            for _n in (5, 10, 20, 60):
                safe_locals[f'rel_strength_{_n}'] = indicators.get(f'rel_strength_{_n}', 0.0)
                safe_locals[f'idx_ret_{_n}'] = indicators.get(f'idx_ret_{_n}', 0.0)
            safe_locals['idx_pct_chg'] = indicators.get('idx_pct_chg', 0.0)

            # 常用数学函数
            safe_locals['abs'] = abs
            safe_locals['max'] = max
            safe_locals['min'] = min
            
            # 直接在受限环境中评估条件（condition应使用变量名如 rsi_6）
            result = eval(condition, {"__builtins__": {}}, safe_locals)
            return bool(result)
            
        except Exception as e:
            logger.error(f"评估条件失败 '{condition}': {e}")
            return False
    
    def execute(self, code: str, context: Dict = None) -> StrategyDecision:
        """
        执行策略
        
        Args:
            code: 股票代码
            context: 额外上下文
        
        Returns:
            策略决策
        """
        # 获取技术指标
        indicators = indicator_analyzer.get_latest_signals(code)
        
        if not indicators:
            return StrategyDecision(
                code=code,
                action="hold",
                position_ratio=0,
                confidence=0,
                reasoning="无法获取技术指标",
                rules_triggered=[],
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
        
        triggered_rules = []
        buy_signals = 0
        sell_signals = 0
        total_position = 0
        
        for rule in self.rules:
            if self.evaluate_condition(rule.condition, indicators):
                triggered_rules.append(rule)
                
                if rule.action == "buy":
                    buy_signals += 1
                    total_position += rule.position_ratio
                elif rule.action == "sell":
                    sell_signals += 1
                    total_position -= rule.position_ratio
        
        # Check exclusion rules (negative list)
        for exc_rule in self.exclusion_rules:
            if self.evaluate_condition(exc_rule.condition, indicators):
                return StrategyDecision(
                    code=code,
                    action="hold",
                    position_ratio=0,
                    confidence=0.5,
                    reasoning=f"排除规则触发: {exc_rule.condition} - {exc_rule.reason}",
                    rules_triggered=[],
                    timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                )
        
        # 决策逻辑
        if buy_signals > sell_signals and buy_signals > 0:
            action = "buy"
            position_ratio = min(total_position / buy_signals, self.max_position_ratio)
            confidence = min(buy_signals / len(self.rules) * 2, 1.0) if self.rules else 0
        elif sell_signals > buy_signals and sell_signals > 0:
            action = "sell"
            position_ratio = min(abs(total_position) / sell_signals, self.max_position_ratio)
            confidence = min(sell_signals / len(self.rules) * 2, 1.0) if self.rules else 0
        else:
            action = "hold"
            position_ratio = 0
            confidence = 0.5
        
        # 构建理由
        reasoning_parts = []
        for rule in triggered_rules:
            reasoning_parts.append(f"- {rule.reason} (条件: {rule.condition})")
        
        reasoning = "\n".join(reasoning_parts) if reasoning_parts else "未触发任何规则"
        
        return StrategyDecision(
            code=code,
            action=action,
            position_ratio=position_ratio,
            confidence=confidence,
            reasoning=reasoning,
            rules_triggered=[r.condition for r in triggered_rules],
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'name': self.name,
            'description': self.description,
            'max_position_ratio': self.max_position_ratio,
            'market_regime': self.market_regime,
            'rules': [asdict(r) for r in self.rules],
            'exclusion_rules': [asdict(r) for r in self.exclusion_rules],
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'QuantStrategy':
        """从字典创建"""
        strategy = cls(name=data.get('name', ''), description=data.get('description', ''))
        raw_max = data.get('max_position_ratio', 1.0)
        strategy.max_position_ratio = max(0.0, min(1.0, float(raw_max)))
        strategy.market_regime = data.get('market_regime', [])
        for rule_data in data.get('rules', []):
            strategy.rules.append(StrategyRule(**rule_data))
        for exc_data in data.get('exclusion_rules', []):
            strategy.exclusion_rules.append(StrategyRule(**exc_data))
        return strategy


class StrategyManager:
    """策略管理器"""
    
    def __init__(self):
        self.strategies: Dict[str, QuantStrategy] = {}
        self._strategies_file = os.path.join(
            config.get('data_storage.data_dir', './data'), 'strategies.json'
        )
        #self._load_builtin_strategies()
        self._load_strategies()
    
    def _load_builtin_strategies(self):
        """加载内置策略"""
        # RSI策略
        rsi_strategy = QuantStrategy("RSI策略", "基于RSI指标的超买超卖策略")
        rsi_strategy.add_rule(
            condition="rsi_6 < 30",
            action="buy",
            position_ratio=0.5,
            reason="RSI超卖，买入信号"
        )
        rsi_strategy.add_rule(
            condition="rsi_6 > 70",
            action="sell",
            position_ratio=0.5,
            reason="RSI超买，卖出信号"
        )
        rsi_strategy.add_exclusion_rule("rsi_6 > 40 and rsi_6 < 60", "RSI处于中性区间，不操作")
        self.strategies['rsi'] = rsi_strategy
        
        # MACD策略
        macd_strategy = QuantStrategy("MACD策略", "基于MACD的趋势跟踪策略")
        macd_strategy.add_rule(
            condition="macd_histogram > 0 and macd > macd_signal",
            action="buy",
            position_ratio=0.6,
            reason="MACD金叉，买入信号"
        )
        macd_strategy.add_rule(
            condition="macd_histogram < 0 and macd < macd_signal",
            action="sell",
            position_ratio=0.6,
            reason="MACD死叉，卖出信号"
        )
        self.strategies['macd'] = macd_strategy
        
        # 均线策略
        ma_strategy = QuantStrategy("均线策略", "基于移动平均线的趋势策略")
        ma_strategy.add_rule(
            condition="overall_score > 20",
            action="buy",
            position_ratio=0.4,
            reason="综合评分大于20，趋势向上"
        )
        ma_strategy.add_rule(
            condition="overall_score < -20",
            action="sell",
            position_ratio=0.4,
            reason="综合评分小于-20，趋势向下"
        )
        self.strategies['ma'] = ma_strategy
        
        # 综合策略
        combined_strategy = QuantStrategy("综合策略", "多指标综合判断策略")
        combined_strategy.add_rule(
            condition="rsi_6 < 35 and macd_histogram > 0",
            action="buy",
            position_ratio=0.5,
            reason="RSI超卖且MACD向上，强烈买入信号"
        )
        combined_strategy.add_rule(
            condition="rsi_6 > 65 and macd_histogram < 0",
            action="sell",
            position_ratio=0.5,
            reason="RSI超买且MACD向下，强烈卖出信号"
        )
        combined_strategy.add_rule(
            condition="kdj_j < 20 and rsi_6 < 40",
            action="buy",
            position_ratio=0.3,
            reason="KDJ和RSI双超卖，买入信号"
        )
        self.strategies['combined'] = combined_strategy
    
    def _load_strategies(self):
        """从文件加载已保存策略；若文件不存在或为空则加载默认示例策略"""
        loaded = False
        if os.path.exists(self._strategies_file):
            try:
                with open(self._strategies_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                for key, strategy_data in data.items():
                    self.strategies[key] = QuantStrategy.from_dict(strategy_data)
                loaded = bool(data)
                self._strategies_file_mtime = os.path.getmtime(self._strategies_file)
                logger.info(f"从文件加载了 {len(data)} 个策略")
            except Exception as e:
                logger.error(f"加载策略文件失败: {e}")
        
        if not loaded:
            self._add_default_strategies()

    def reload_from_file(self):
        """重新从文件读取策略，合并到内存（热重载，无需重启服务）。
        仅当文件修改时间变化时才执行，避免频繁IO。"""
        if not os.path.exists(self._strategies_file):
            return
        try:
            mtime = os.path.getmtime(self._strategies_file)
            if mtime == getattr(self, '_strategies_file_mtime', None):
                return  # 文件未变化，跳过
            with open(self._strategies_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            for key, strategy_data in data.items():
                self.strategies[key] = QuantStrategy.from_dict(strategy_data)
            # 删除文件中已不存在的策略（同步清理）
            file_keys = set(data.keys())
            to_remove = [k for k in list(self.strategies.keys()) if k not in file_keys]
            for k in to_remove:
                del self.strategies[k]
            self._strategies_file_mtime = mtime
            logger.info(f"热重载策略文件: {len(data)} 个策略")
        except Exception as e:
            logger.error(f"热重载策略文件失败: {e}")
    
    def _add_default_strategies(self):
        """添加5个内置示例策略"""
        # 1. RSI超卖策略
        s1 = QuantStrategy(
            name="RSI超卖反弹策略",
            description="当RSI(14)低于30时买入信号，当RSI高于70时卖出信号。这是一个经典的超买超卖指标，适用于震荡行情中捕捉低点买入和高点卖出的机会。"
        )
        s1.add_rule("rsi_12 < 30", "buy", 0.5, "RSI低于30，超卖区间，买入信号")
        s1.add_rule("rsi_12 > 70", "sell", 1.0, "RSI高于70，超买区间，卖出信号")
        self.strategies['rsi_oversold'] = s1
        
        # 2. 双均线金叉策略
        s2 = QuantStrategy(
            name="MA均线金叉策略",
            description="当5日均线向上穿越20日均线（金叉）时买入，当5日均线向下穿越20日均线（死叉）时卖出。这是最经典的趋势跟随策略，适合趋势明显的市场。"
        )
        s2.add_rule("ma5 > ma20", "buy", 0.6, "5日均线上穿20日均线（金叉），趋势向上")
        s2.add_rule("ma5 < ma20", "sell", 0.6, "5日均线下穿20日均线（死叉），趋势向下")
        self.strategies['ma_cross'] = s2
        
        # 3. MACD金叉策略
        s3 = QuantStrategy(
            name="MACD金叉策略",
            description="当MACD的DIF线向上穿越DEA线（金叉）时买入，当DIF向下穿越DEA（死叉）时卖出。MACD结合了趋势和动量，信号相对可靠。"
        )
        s3.add_rule("macd > 0", "buy", 0.5, "MACD DIF线高于零轴，多头动能")
        s3.add_rule("macd < 0", "sell", 0.5, "MACD DIF线低于零轴，空头动能")
        self.strategies['macd_cross'] = s3
        
        # 4. RSI+MA综合策略
        s4 = QuantStrategy(
            name="RSI+均线综合策略",
            description="多条件组合：当RSI(12)低于40且股价高于20日均线时买入（即处于超卖但趋势向上），当RSI高于75或价格跌破20日均线时卖出。"
        )
        s4.add_rule("rsi_12 < 40 and close > ma20", "buy", 0.6, "RSI超卖且价格在均线上方，趋势向上的低位买入")
        s4.add_rule("rsi_12 > 75", "sell", 1.0, "RSI超买，高位卖出")
        s4.add_rule("close < ma20", "sell", 0.5, "价格跌破20日均线，趋势转弱")
        self.strategies['combined'] = s4
        
        # 5. 布林带策略
        s5 = QuantStrategy(
            name="布林带突破策略",
            description="当价格触及布林带下轨（超卖）时买入，当价格触及布林带上轨（超买）时卖出。适用于均值回归的震荡行情。"
        )
        s5.add_rule("close < boll_lower", "buy", 0.5, "价格触及布林带下轨，超卖区间")
        s5.add_rule("close > boll_upper", "sell", 1.0, "价格触及布林带上轨，超买区间")
        self.strategies['bollinger'] = s5
        
        logger.info("已加载5个默认示例策略")
    
    def save_strategies(self):
        """保存用户策略到文件（内置策略不保存）"""
        builtin_keys = {'rsi', 'macd', 'ma'}
        data = {
            key: strategy.to_dict()
            for key, strategy in self.strategies.items()
            if key not in builtin_keys
        }
        try:
            os.makedirs(os.path.dirname(self._strategies_file), exist_ok=True)
            with open(self._strategies_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"策略已保存: {self._strategies_file}")
        except Exception as e:
            logger.error(f"保存策略失败: {e}")
    
    def get_strategy(self, name: str) -> Optional[QuantStrategy]:
        """获取策略。支持按 key（内部标识）或按策略显示名称查找。"""
        # 直接按 key 查找
        s = self.strategies.get(name)
        if s:
            return s
        # 按策略对象的 display name 查找
        for key, strategy in self.strategies.items():
            if strategy.name == name:
                return strategy
        return None
    
    def add_strategy(self, name: str, strategy: QuantStrategy):
        """添加策略"""
        self.strategies[name] = strategy
    
    def list_strategies(self) -> List[str]:
        """列出所有策略"""
        return list(self.strategies.keys())
    
    def run_strategy(self, strategy_name: str, code: str) -> StrategyDecision:
        """
        运行策略
        
        Args:
            strategy_name: 策略名称
            code: 股票代码
        
        Returns:
            策略决策
        """
        strategy = self.get_strategy(strategy_name)
        if not strategy:
            raise ValueError(f"策略不存在: {strategy_name}")
        
        return strategy.execute(code)
    
    def run_all_strategies(self, code: str) -> Dict[str, StrategyDecision]:
        """
        运行所有策略
        
        Args:
            code: 股票代码
        
        Returns:
            策略决策字典
        """
        results = {}
        for name, strategy in self.strategies.items():
            try:
                results[name] = strategy.execute(code)
            except Exception as e:
                logger.error(f"运行策略 {name} 失败: {e}")
        
        return results
    
    def run_strategy_split(self, buy_strategy_name: str, sell_strategy_name: str, code: str) -> StrategyDecision:
        """使用独立的买入策略和卖出策略运行决策"""
        buy_strat = self.get_strategy(buy_strategy_name)
        sell_strat = self.get_strategy(sell_strategy_name)
        if not buy_strat:
            raise ValueError(f"买入策略不存在: {buy_strategy_name}")
        if not sell_strat:
            raise ValueError(f"卖出策略不存在: {sell_strategy_name}")
        combined = merge_buy_sell_strategies(buy_strat, sell_strat)
        return combined.execute(code)

    def create_strategy_from_description(self, name: str, description: str) -> QuantStrategy:
        """
        从自然语言描述创建策略
        
        Args:
            name: 策略名称
            description: 策略描述
        
        Returns:
            策略对象
        """
        strategy = QuantStrategy(name, description)
        strategy.from_natural_language(description)
        self.strategies[name] = strategy
        return strategy


class AIDecisionMaker:
    """AI决策器"""
    
    def __init__(self):
        self.ai_client = AIModelClient()
    
    def make_decision(self, code: str, strategy_description: str = None) -> StrategyDecision:
        """
        AI综合决策
        
        Args:
            code: 股票代码
            strategy_description: 策略描述（可选）
        
        Returns:
            AI决策
        """
        stock = stock_manager.get_stock_by_code(code)
        indicators = indicator_analyzer.get_latest_signals(code)
        features = feature_extractor.extract_all_features(code)
        
        if not indicators:
            return StrategyDecision(
                code=code,
                action="hold",
                position_ratio=0,
                confidence=0,
                reasoning="无法获取数据",
                rules_triggered=[],
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
        
        # 构建策略要求部分（避免在f-string中使用转义字符）
        strategy_part = f"【策略要求】\n{strategy_description}" if strategy_description else ""
        
        prompt = f"""
请基于以下数据对 {stock.name if stock else code}({code}) 做出交易决策：

【技术指标】
- RSI(6): {indicators.get('rsi_6', 'N/A')}
- MACD柱状图: {indicators.get('macd_histogram', 'N/A')}
- 均线趋势: {indicators.get('ma_trend', 'N/A')}
- 综合评分: {indicators.get('overall_score', 'N/A')}
- KDJ-J: {indicators.get('kdj_j', 'N/A')}

【特征分析】
- 趋势强度: {features['technical'].get('trend_strength', 'N/A')}
- 情感分数: {features['sentiment'].get('avg_sentiment', 'N/A')}

{strategy_part}

请输出JSON格式的决策结果：
{{
  "action": "buy/sell/hold",
  "position_ratio": 0.0-1.0,
  "confidence": 0.0-1.0,
  "reasoning": "详细理由",
  "risk_assessment": "风险评估"
}}
"""
        
        try:
            response = self.ai_client.call(prompt)
            logger.debug(f"AI原始响应(决策): {str(response)[:1000]}")
            
            # 提取JSON - 尝试更健壮的方法
            try:
                start = response.find('{')
                end = response.rfind('}')
                if start != -1 and end != -1 and end > start:
                    json_match = response[start:end+1]
                    decision_data = json.loads(json_match)
                else:
                    decision_data = json.loads(response)
            except Exception as parse_e:
                logger.exception(f"解析AI决策JSON失败: {parse_e}, 原始响应片段: {response[:500]}")
                raise
            
            # 验证字段类型
            action = decision_data.get('action', 'hold')
            pos_ratio = decision_data.get('position_ratio', 0)
            try:
                pos_ratio = float(pos_ratio)
            except Exception:
                pos_ratio = 0.0
            # 如果AI返回百分比形式（>1），按百分比转换
            if pos_ratio > 1.0:
                pos_ratio = pos_ratio / 100.0
            pos_ratio = max(0.0, min(1.0, pos_ratio))
            confidence = float(decision_data.get('confidence', 0)) if decision_data.get('confidence') is not None else 0.0
            reasoning_text = str(decision_data.get('reasoning', ''))
            risk_text = str(decision_data.get('risk_assessment', ''))
            
            return StrategyDecision(
                code=code,
                action=action,
                position_ratio=pos_ratio,
                confidence=confidence,
                reasoning=reasoning_text + "\n风险评估: " + risk_text,
                rules_triggered=["AI决策"],
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            
        except Exception as e:
            logger.exception(f"AI决策失败: {e}")
            # 保存原始响应以便排查（如果存在）
            try:
                resp = response if 'response' in locals() else '<no ai response>'
                with open(os.path.join(config.get('data_storage.data_dir', './data'), 'tmp_ai_last_decision_response.txt'), 'w', encoding='utf-8') as f:
                    f.write(str(resp))
            except Exception:
                logger.warning('保存AI决策响应失败')
            
            return StrategyDecision(
                code=code,
                action="hold",
                position_ratio=0,
                confidence=0,
                reasoning=f"AI决策失败: {e}",
                rules_triggered=[],
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )


# 全局实例
def merge_buy_sell_strategies(buy_strat: QuantStrategy, sell_strat: QuantStrategy) -> QuantStrategy:
    """将买入策略的 buy 规则与卖出策略的 sell 规则合并为一个临时策略用于回测/决策。
    两侧的 exclusion_rules 都会保留。若两个策略相同则直接返回原对象。"""
    if buy_strat.name == sell_strat.name:
        return buy_strat
    combined = QuantStrategy(
        name=f"{buy_strat.name}(买) + {sell_strat.name}(卖)",
        description=f"买入策略: {buy_strat.name} | 卖出策略: {sell_strat.name}"
    )
    combined.rules.extend([r for r in buy_strat.rules if r.action == 'buy'])
    combined.rules.extend([r for r in sell_strat.rules if r.action == 'sell'])
    combined.exclusion_rules.extend(buy_strat.exclusion_rules)
    combined.exclusion_rules.extend(sell_strat.exclusion_rules)
    # 取买入策略的最大仓位上限（决定建仓规模）
    combined.max_position_ratio = buy_strat.max_position_ratio
    return combined


strategy_manager = StrategyManager()
ai_decision_maker = AIDecisionMaker()
