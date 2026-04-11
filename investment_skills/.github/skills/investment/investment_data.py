"""
investment_data.py — 股票数据抓取工具
基于 akshare（免费，无需 API Key）实现 skill.yml 中定义的所有数据接口。

安装依赖:
    pip install akshare ta requests

用法:
    python investment_data.py 宁德时代
    python investment_data.py 000333
"""

import sys
import json
import warnings
from datetime import datetime, timedelta

# 强制 UTF-8 输出（解决 Windows GBK 终端乱码）
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

warnings.filterwarnings("ignore")

try:
    import akshare as ak
    import pandas as pd
    import ta as ta_lib
except ImportError as e:
    print(f"[错误] 缺少依赖: {e}")
    print("请运行: pip install akshare ta")
    sys.exit(1)


def _rsi(series: "pd.Series", length: int = 14) -> "pd.Series":
    """计算 RSI，使用 ta 库"""
    return ta_lib.momentum.RSIIndicator(close=series, window=length).rsi()


def _macd(series: "pd.Series"):
    """计算 MACD，返回 (diff, signal)"""
    m = ta_lib.trend.MACD(close=series)
    return m.macd_diff(), m.macd_signal()


# ─────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────

def find_stock_code(name_or_code: str) -> tuple[str, str, str]:
    """
    根据股票名称或代码查找标准代码和市场。
    返回: (code, name, market)  market: '0'=深市, '1'=沪市, '1'=北交所
    """
    try:
        df = ak.stock_zh_a_spot_em()
        # 按代码精确匹配
        row = df[df["代码"] == name_or_code]
        if row.empty:
            # 按名称模糊匹配
            row = df[df["名称"].str.contains(name_or_code, na=False)]
        if row.empty:
            raise ValueError(f"未找到股票: {name_or_code}")
        row = row.iloc[0]
        code = row["代码"]
        name = row["名称"]
        # 判断市场
        if code.startswith("6"):
            market = "1"  # 沪市
        elif code.startswith("8") or code.startswith("4"):
            market = "0"  # 北交所/深市
        else:
            market = "0"  # 深市
        return code, name, market
    except Exception as e:
        print(f"[警告] 查找股票失败: {e}")
        return name_or_code, name_or_code, "0"


def get_realtime_quote(code: str, name: str) -> dict:
    """实时行情"""
    try:
        df = ak.stock_zh_a_spot_em()
        row = df[df["代码"] == code]
        if row.empty:
            return {}
        r = row.iloc[0]
        return {
            "price": float(r.get("最新价", 0)),
            "change_pct": float(r.get("涨跌幅", 0)),
            "change_amount": float(r.get("涨跌额", 0)),
            "volume": float(r.get("成交量", 0)),
            "amount": float(r.get("成交额", 0)),
            "turnover_rate": float(r.get("换手率", 0)),
            "pe_ratio": float(r.get("市盈率-动态", 0) or 0),
            "pb_ratio": float(r.get("市净率", 0) or 0),
            "market_cap": float(r.get("总市值", 0)),
            "52w_high": float(r.get("52周最高", 0) or 0),
            "52w_low": float(r.get("52周最低", 0) or 0),
        }
    except Exception as e:
        print(f"[警告] 实时行情获取失败: {e}")
        return {}


def get_kline_and_indicators(code: str, days: int = 120) -> dict:
    """K线数据 + 技术指标（均线、RSI、MACD）"""
    try:
        start = (datetime.now() - timedelta(days=days * 2)).strftime("%Y%m%d")
        hist = ak.stock_zh_a_hist(
            symbol=code, period="daily", start_date=start, adjust="qfq"
        )
        hist = hist.tail(days).reset_index(drop=True)

        close = hist["收盘"]
        high = hist["最高"]
        low = hist["最低"]
        volume = hist["成交量"]

        # 均线
        ma5 = close.rolling(5).mean().iloc[-1]
        ma10 = close.rolling(10).mean().iloc[-1]
        ma30 = close.rolling(30).mean().iloc[-1]
        ma60 = close.rolling(60).mean().iloc[-1]
        ma120 = close.rolling(120).mean().iloc[-1]

        # RSI
        rsi5 = _rsi(close, length=5).iloc[-1]
        rsi14 = _rsi(close, length=14).iloc[-1]

        # RSI 背离检测（近20根K线）
        recent = hist.tail(20)
        rc = recent["收盘"]
        rsi_series = _rsi(close, length=14).tail(20)
        bullish_divergence = False
        if len(rc) >= 10:
            # 价格创新低但RSI未创新低 → 底背离
            price_new_low = rc.iloc[-1] < rc.iloc[:-1].min()
            rsi_not_new_low = rsi_series.iloc[-1] > rsi_series.iloc[:-1].min()
            bullish_divergence = price_new_low and rsi_not_new_low

        # MACD
        macd_diff_series, macd_signal_series = _macd(close)
        if macd_diff_series is not None and not macd_diff_series.empty:
            macd_diff = float(macd_diff_series.iloc[-1] or 0)
            macd_signal = float(macd_signal_series.iloc[-1] or 0)
        else:
            macd_diff, macd_signal = 0.0, 0.0

        # 成交量趋势（近5日vs近20日均量）
        vol_5avg = volume.tail(5).mean()
        vol_20avg = volume.tail(20).mean()
        volume_trend = "放量" if vol_5avg > vol_20avg * 1.3 else (
            "缩量" if vol_5avg < vol_20avg * 0.7 else "正常"
        )

        current_price = float(close.iloc[-1])

        # 均线位置
        def ma_position(ma_val):
            if pd.isna(ma_val):
                return "数据不足"
            return "上方 ✅" if current_price > ma_val else "下方 ❌"

        return {
            "current_price": current_price,
            "ma5": round(ma5, 2) if not pd.isna(ma5) else None,
            "ma10": round(ma10, 2) if not pd.isna(ma10) else None,
            "ma30": round(ma30, 2) if not pd.isna(ma30) else None,
            "ma60": round(ma60, 2) if not pd.isna(ma60) else None,
            "ma120": round(ma120, 2) if not pd.isna(ma120) else None,
            "ma5_position": ma_position(ma5),
            "ma10_position": ma_position(ma10),
            "ma30_position": ma_position(ma30),
            "ma60_position": ma_position(ma60),
            "rsi_5d": round(float(rsi5), 1) if not pd.isna(rsi5) else None,
            "rsi_14d": round(float(rsi14), 1) if not pd.isna(rsi14) else None,
            "bullish_divergence": bullish_divergence,
            "macd_diff": round(macd_diff, 4),
            "macd_signal": round(macd_signal, 4),
            "volume_trend": volume_trend,
            "ma_trend": _judge_ma_trend(current_price, ma5, ma10, ma30, ma60),
        }
    except Exception as e:
        print(f"[警告] K线/指标获取失败: {e}")
        return {}


def get_weekly_rsi(code: str) -> float | None:
    """获取周线 RSI"""
    try:
        start = (datetime.now() - timedelta(days=365 * 2)).strftime("%Y%m%d")
        hist = ak.stock_zh_a_hist(symbol=code, period="weekly", start_date=start, adjust="qfq")
        rsi = _rsi(hist["收盘"], length=14)
        return round(float(rsi.iloc[-1]), 1) if not pd.isna(rsi.iloc[-1]) else None
    except Exception as e:
        print(f"[警告] 周线RSI获取失败: {e}")
        return None


def _judge_ma_trend(price, ma5, ma10, ma30, ma60) -> str:
    """判断均线趋势"""
    vals = [v for v in [ma5, ma10, ma30, ma60] if v is not None and not (isinstance(v, float) and pd.isna(v))]
    if len(vals) < 3:
        return "数据不足"
    is_bull = all(vals[i] > vals[i + 1] for i in range(len(vals) - 1))
    is_bear = all(vals[i] < vals[i + 1] for i in range(len(vals) - 1))
    if is_bull:
        return "多头排列 ✅（5>10>30>60）"
    elif is_bear:
        return "空头排列 ❌（5<10<30<60）"
    else:
        return "震荡排列 ⚠️（均线缠绕）"


def get_sector_info(code: str) -> dict:
    """板块信息"""
    try:
        # 获取股票所属行业板块
        df = ak.stock_individual_info_em(symbol=code)
        info = dict(zip(df["item"], df["value"]))
        sector_name = info.get("行业", "未知")

        # 获取该行业板块近期涨跌
        try:
            sectors = ak.stock_board_industry_name_em()
            match = sectors[sectors["板块名称"].str.contains(
                sector_name[:3] if len(sector_name) >= 3 else sector_name, na=False
            )]
            if not match.empty:
                s = match.iloc[0]
                return {
                    "sector_name": s["板块名称"],
                    "sector_change_pct": float(s.get("涨跌幅", 0)),
                    "sector_change_5d": float(s.get("5日涨跌", 0) or 0),
                    "sector_strength": _judge_sector(float(s.get("涨跌幅", 0))),
                    "stock_info": info,
                }
        except Exception:
            pass

        return {"sector_name": sector_name, "sector_strength": "未知", "stock_info": info}
    except Exception as e:
        print(f"[警告] 板块信息获取失败: {e}")
        return {}


def _judge_sector(change_pct: float) -> str:
    if change_pct >= 2:
        return "强势 ✅"
    elif change_pct >= 0:
        return "中性 ⚠️"
    else:
        return "弱势 ❌"


def get_money_flow(code: str) -> dict:
    """资金流向"""
    try:
        # 判断市场
        market = "沪A" if code.startswith("6") else "深A"
        df = ak.stock_individual_fund_flow(stock=code, market=market)
        if df is None or df.empty:
            return {}
        recent = df.tail(5)
        net_5d = float(recent["主力净流入-净额"].sum())
        today_net = float(df.iloc[-1]["主力净流入-净额"])
        return {
            "net_main_inflow_today": round(today_net / 1e8, 2),  # 亿元
            "net_main_inflow_5d": round(net_5d / 1e8, 2),
            "flow_signal": "主力净流入 ✅" if net_5d > 0 else "主力净流出 ❌",
        }
    except Exception as e:
        print(f"[警告] 资金流向获取失败: {e}")
        return {}


def get_market_index() -> dict:
    """大盘指数状态"""
    try:
        results = {}
        for symbol, name in [("sh000001", "上证指数"), ("sh000300", "沪深300"), ("sz399006", "创业板指")]:
            try:
                df = ak.stock_zh_index_daily(symbol=symbol)
                df = df.tail(120)
                close = df["close"]
                ma5 = close.rolling(5).mean().iloc[-1]
                ma10 = close.rolling(10).mean().iloc[-1]
                ma30 = close.rolling(30).mean().iloc[-1]
                ma60 = close.rolling(60).mean().iloc[-1]
                rsi14 = _rsi(close, length=14).iloc[-1]
                change_5d = (float(close.iloc[-1]) / float(close.iloc[-6]) - 1) * 100 if len(close) >= 6 else 0
                results[name] = {
                    "price": round(float(close.iloc[-1]), 2),
                    "change_5d_pct": round(change_5d, 2),
                    "rsi14": round(float(rsi14), 1),
                    "ma_trend": _judge_ma_trend(float(close.iloc[-1]), ma5, ma10, ma30, ma60),
                }
            except Exception:
                continue

        # 综合大盘判断
        sh = results.get("上证指数", {})
        if sh:
            rsi = sh.get("rsi14", 50)
            trend_str = sh.get("ma_trend", "")
            if "多头" in trend_str:
                overall_trend = "上升趋势 ✅"
            elif "空头" in trend_str:
                overall_trend = "下降趋势 ❌"
            else:
                overall_trend = "震荡行情 ⚠️"

            rsi_label = "超卖 🟢" if rsi < 30 else ("过热 🔴" if rsi > 70 else "正常 ⚪")
            results["综合判断"] = {
                "overall_trend": overall_trend,
                "rsi_label": rsi_label,
                "suitable_for_trading": "空头" not in trend_str,
            }
        return results
    except Exception as e:
        print(f"[警告] 大盘数据获取失败: {e}")
        return {}


# ─────────────────────────────────────────────
# 综合评分 & 决策
# ─────────────────────────────────────────────

def score_and_decide(market: dict, sector: dict, tech: dict, flow: dict) -> dict:
    """按《投资秘笈》六大要素打分，输出最终决策"""
    scores = {}

    # 1. 大盘评分 (0-10)
    m = market.get("综合判断", {})
    if not m.get("suitable_for_trading", True):
        scores["大盘"] = 0
    else:
        trend = m.get("overall_trend", "")
        rsi_lbl = m.get("rsi_label", "")
        s = 5
        if "上升" in trend:
            s += 3
        if "超卖" in rsi_lbl:
            s += 2
        elif "过热" in rsi_lbl:
            s -= 2
        scores["大盘"] = min(10, max(0, s))

    # 2. 板块评分 (0-10)
    strength = sector.get("sector_strength", "")
    s = 5
    if "强势" in strength:
        s += 3
    elif "弱势" in strength:
        s -= 3
    scores["板块"] = min(10, max(0, s))

    # 3. 技术面评分 (0-10)
    s = 5
    rsi14 = tech.get("rsi_14d", 50) or 50
    rsi5 = tech.get("rsi_5d", 50) or 50
    if rsi14 <= 30 or rsi5 <= 30:
        s += 2
    elif rsi14 >= 70:
        s -= 2
    if tech.get("bullish_divergence"):
        s += 2
    ma_trend = tech.get("ma_trend", "")
    if "多头" in ma_trend:
        s += 1
    elif "空头" in ma_trend:
        s -= 2
    if tech.get("volume_trend") == "放量":
        s += 1
    scores["技术面"] = min(10, max(0, s))

    # 4. 主力/资金评分 (0-10)
    signal = flow.get("flow_signal", "")
    s = 5
    if "净流入" in signal:
        s += 3
    elif "净流出" in signal:
        s -= 3
    scores["主力"] = min(10, max(0, s))

    # 综合加权 (大盘30% + 板块20% + 技术35% + 主力15%)
    weights = {"大盘": 0.30, "板块": 0.20, "技术面": 0.35, "主力": 0.15}
    total = sum(scores[k] * weights[k] for k in scores)

    # 决策
    if not m.get("suitable_for_trading", True):
        decision = "不买 ❌"
        mode = "不操作"
        reason = "大盘处于下降趋势，等待大盘企稳后再考虑"
    elif total >= 7.5:
        decision = "买入 ✅"
        mode = "右侧买入" if "多头" in tech.get("ma_trend", "") else "左侧试探"
        reason = "综合评分高，大盘、板块、技术面三者共振向好"
    elif total >= 5.5:
        decision = "观望 ⏳"
        mode = "等待"
        reason = "综合评分中等，信号不够明确，等待更好的买点"
    else:
        decision = "不买 ❌"
        mode = "不操作"
        reason = "综合评分偏低，当前不是合适的入场时机"

    return {
        "scores": scores,
        "total_score": round(total, 1),
        "decision": decision,
        "mode": mode,
        "reason": reason,
    }


# ─────────────────────────────────────────────
# 主入口
# ─────────────────────────────────────────────

def analyze(name_or_code: str):
    print(f"\n{'='*60}")
    print(f"  [股票分析] {name_or_code}")
    print(f"{'='*60}\n")

    # 查找股票
    code, name, market = find_stock_code(name_or_code)
    print(f"股票: {name} ({code})\n")

    # 并行获取数据
    print("正在获取数据...")
    market_data = get_market_index()
    quote = get_realtime_quote(code, name)
    tech = get_kline_and_indicators(code)
    tech["rsi_weekly"] = get_weekly_rsi(code)
    sector = get_sector_info(code)
    flow = get_money_flow(code)

    # 评分决策
    result = score_and_decide(market_data, sector, tech, flow)

    # 输出报告
    print("\n🏛️  大盘环境")
    print("-" * 40)
    for idx_name, idx_data in market_data.items():
        if isinstance(idx_data, dict):
            print(f"  {idx_name}: {json.dumps(idx_data, ensure_ascii=False)}")

    print("\n🔥  板块分析")
    print("-" * 40)
    print(f"  所属板块: {sector.get('sector_name', '未知')}")
    print(f"  板块强度: {sector.get('sector_strength', '未知')}")

    print("\n📈  技术面")
    print("-" * 40)
    print(f"  当前价格: {tech.get('current_price', quote.get('price', '?'))}")
    print(f"  均线趋势: {tech.get('ma_trend', '?')}")
    print(f"  MA5({tech.get('ma5','?')}): {tech.get('ma5_position','?')}")
    print(f"  MA10({tech.get('ma10','?')}): {tech.get('ma10_position','?')}")
    print(f"  MA30({tech.get('ma30','?')}): {tech.get('ma30_position','?')}")
    print(f"  MA60({tech.get('ma60','?')}): {tech.get('ma60_position','?')}")
    print(f"  RSI(5日): {tech.get('rsi_5d','?')}")
    print(f"  RSI(14日): {tech.get('rsi_14d','?')}")
    print(f"  RSI(周线): {tech.get('rsi_weekly','?')}")
    print(f"  RSI底背离: {'✅ 是' if tech.get('bullish_divergence') else '❌ 否'}")
    print(f"  成交量趋势: {tech.get('volume_trend','?')}")

    print("\n🧠  主力/资金")
    print("-" * 40)
    print(f"  今日主力净流入: {flow.get('net_main_inflow_today','?')} 亿元")
    print(f"  5日主力净流入: {flow.get('net_main_inflow_5d','?')} 亿元")
    print(f"  信号: {flow.get('flow_signal','未知')}")

    print("\n⚖️  综合评分")
    print("-" * 40)
    for k, v in result["scores"].items():
        bar = "█" * v + "░" * (10 - v)
        print(f"  {k:6s}: {bar} {v}/10")
    print(f"  {'总分':6s}: {'█' * int(result['total_score'])}{'░' * (10 - int(result['total_score']))} {result['total_score']}/10")

    print(f"\n{'='*60}")
    print(f"  🎯 决策: {result['decision']}")
    print(f"  模式: {result['mode']}")
    print(f"  理由: {result['reason']}")
    print(f"{'='*60}\n")

    print("⚠️  本分析仅供参考，不构成投资建议。投资有风险，入市须谨慎。\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python investment_data.py <股票名称或代码>")
        print("示例: python investment_data.py 宁德时代")
        print("      python investment_data.py 000333")
    else:
        analyze(" ".join(sys.argv[1:]))
