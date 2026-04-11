"""
批量读取默认股票列表并输出“重点票为什么这么判”的摘要。

用法:
    python .github\skills\default-watchlist-review\default_stock_list_review.py
    python .github\skills\default-watchlist-review\default_stock_list_review.py --top 6
    python .github\skills\default-watchlist-review\default_stock_list_review.py --send-wechat
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _ensure_utf8() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")


def _find_project_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "config" / "stocks.yaml").exists() and (parent / "quant_system").exists():
            return parent
    raise FileNotFoundError("未找到项目根目录，需同时包含 config\\stocks.yaml 和 quant_system")


PROJECT_ROOT = _find_project_root()
STOCKS_PATH = PROJECT_ROOT / "config" / "stocks.yaml"


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://quote.eastmoney.com/",
        }
    )
    return session


SESSION = _make_session()


@dataclass
class MarketState:
    name: str
    market: str
    price: float
    change_pct: float
    ma20: float | None
    ma60: float | None
    weekly_rsi: float | None
    trend: str


@dataclass
class StockReview:
    name: str
    code: str
    market: str
    industry: str
    price: float
    change_pct: float
    ma20: float | None
    ma60: float | None
    dist20: float | None
    daily_rsi: float | None
    weekly_rsi: float | None
    trend: str
    volume_state: str
    higher_lows: bool
    score_breakdown: dict[str, int]
    score_total: int
    action: str
    reason_lines: list[str]


def _warn(message: str) -> None:
    print(f"[警告] {message}", file=sys.stderr)


def _sleep_brief() -> None:
    time.sleep(0.12)


def _get_json(url: str) -> dict[str, Any]:
    last_error: Exception | None = None
    for _ in range(4):
        try:
            _sleep_brief()
            response = SESSION.get(url, timeout=20)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(0.8)
    raise RuntimeError(f"请求失败: {url}") from last_error


def _secid(market: str, code: str) -> str:
    code = str(code)
    if market == "sh":
        return f"1.{code}"
    if market == "sz":
        return f"0.{code}"
    if market == "hk":
        return "100.HSI" if code == "HSI" else f"116.{code.zfill(5)}"
    return f"0.{code}"


def _ma(values: list[float], window: int) -> float | None:
    if len(values) < window:
        return None
    return sum(values[-window:]) / window


def _rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) < period + 1:
        return None
    gains: list[float] = []
    losses: list[float] = []
    for idx in range(1, len(values)):
        delta = values[idx] - values[idx - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for idx in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[idx]) / period
        avg_loss = (avg_loss * (period - 1) + losses[idx]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - 100 / (1 + rs)


def _fetch_quote(market: str, code: str) -> dict[str, Any]:
    url = (
        "https://push2.eastmoney.com/api/qt/stock/get"
        f"?secid={_secid(market, code)}&fields=f43,f57,f58,f169,f170,f46,f44,f45,f47,f48,f116,f117"
    )
    try:
        return _get_json(url).get("data", {})
    except RuntimeError as exc:
        _warn(f"实时行情获取失败 {market}.{code}: {exc}")
        return {}


def _fetch_kline(market: str, code: str, klt: int, limit: int) -> list[dict[str, float]]:
    url = (
        "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        f"?secid={_secid(market, code)}"
        "&fields1=f1,f2,f3,f4,f5,f6"
        "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
        f"&klt={klt}&fqt=1&end=20500101&lmt={limit}"
    )
    try:
        data = _get_json(url).get("data", {})
    except RuntimeError as exc:
        _warn(f"K线获取失败 {market}.{code} klt={klt}: {exc}")
        return []
    rows: list[dict[str, float]] = []
    for line in data.get("klines", []):
        parts = line.split(",")
        rows.append(
            {
                "open": float(parts[1]),
                "close": float(parts[2]),
                "high": float(parts[3]),
                "low": float(parts[4]),
                "volume": float(parts[5]),
            }
        )
    return rows


def _scale_price(market: str, raw_price: Any) -> float:
    scale = 1000 if market == "hk" else 100
    return float(raw_price or 0) / scale


def _market_trend(price: float, ma20: float | None, ma60: float | None) -> str:
    if ma20 is None or ma60 is None:
        return "未知"
    if price > ma20 > ma60:
        return "上升"
    if price < ma20 < ma60:
        return "下跌"
    return "震荡"


def _stock_trend(price: float, ma5: float | None, ma20: float | None, ma60: float | None) -> str:
    if ma5 is None or ma20 is None or ma60 is None:
        return "未知"
    if ma5 > ma20 > ma60 and price > ma5:
        return "多头"
    if ma5 < ma20 < ma60 and price < ma5:
        return "空头"
    return "震荡"


def _volume_state(volumes: list[float]) -> str:
    vol5 = _ma(volumes, 5)
    vol20 = _ma(volumes, 20)
    if vol5 is None or vol20 is None:
        return "正常"
    if vol5 > vol20 * 1.25:
        return "放量"
    if vol5 < vol20 * 0.8:
        return "缩量"
    return "正常"


def _higher_lows(rows: list[dict[str, float]]) -> bool:
    if len(rows) < 4:
        return False
    return rows[-1]["low"] > rows[-2]["low"] > rows[-3]["low"]


def _load_watchlist() -> dict[str, Any]:
    return yaml.safe_load(STOCKS_PATH.read_text(encoding="utf-8"))


def _review_markets(config: dict[str, Any]) -> tuple[list[MarketState], bool, bool]:
    markets: list[MarketState] = []
    for item in config["indices"]:
        market = item["market"]
        code = str(item["code"])
        quote = _fetch_quote(market, code)
        daily = _fetch_kline(market, code, klt=101, limit=130)
        weekly = _fetch_kline(market, code, klt=102, limit=80)
        closes = [row["close"] for row in daily]
        weekly_closes = [row["close"] for row in weekly]
        price = _scale_price(market, quote.get("f43")) if quote else (closes[-1] if closes else 0.0)
        ma20 = _ma(closes, 20)
        ma60 = _ma(closes, 60)
        weekly_rsi = _rsi(weekly_closes, 14)
        markets.append(
            MarketState(
                name=item["name"],
                market=market,
                price=round(price, 2),
                change_pct=round(float(quote.get("f170") or 0) / 100, 2),
                ma20=round(ma20, 2) if ma20 is not None else None,
                ma60=round(ma60, 2) if ma60 is not None else None,
                weekly_rsi=round(weekly_rsi, 1) if weekly_rsi is not None else None,
                trend=_market_trend(price, ma20, ma60),
            )
        )

    a_share_ok = sum(1 for item in markets if item.market in {"sh", "sz"} and item.trend in {"上升", "震荡"}) >= 3
    hk_ok = any(item.name == "恒生指数" and item.trend in {"上升", "震荡"} for item in markets)
    return markets, a_share_ok, hk_ok


def _classify_market_score(market_ok: bool) -> int:
    return 2 if market_ok else 0


def _build_reasons(
    review: StockReview,
    position_score: int,
    kline_score: int,
    ma_score: int,
    rsi_score: int,
    volume_score: int,
    market_score: int,
) -> list[str]:
    reasons: list[str] = []
    if review.ma20 is not None and review.ma60 is not None:
        reasons.append(
            f"现价 {review.price}，MA20={review.ma20}，MA60={review.ma60}，当前结构为 {review.trend}。"
        )
    if review.dist20 is not None:
        reasons.append(f"相对 20 日线乖离 {review.dist20:+.2f}%，位置评分 {position_score}/2。")
    if review.daily_rsi is not None:
        weekly = f"，周 RSI {review.weekly_rsi}" if review.weekly_rsi is not None else ""
        reasons.append(f"日 RSI {review.daily_rsi}{weekly}，RSI 评分 {rsi_score}/2。")
    reasons.append(
        f"量能状态 {review.volume_state}、低点抬高 {'有' if review.higher_lows else '无'}，K线评分 {kline_score}/2，量能评分 {volume_score}/2。"
    )
    reasons.append(
        f"大盘环境评分 {market_score}/2，均线趋势评分 {ma_score}/2，总分 {review.score_total}/12，对应结论为 {review.action}。"
    )
    return reasons[:4]


def _review_stock(item: dict[str, Any], market_ok: bool) -> StockReview:
    market = item["market"]
    code = str(item["code"])
    quote = _fetch_quote(market, code)
    daily = _fetch_kline(market, code, klt=101, limit=130)
    weekly = _fetch_kline(market, code, klt=102, limit=80)

    closes = [row["close"] for row in daily]
    weekly_closes = [row["close"] for row in weekly]
    volumes = [row["volume"] for row in daily]

    price = _scale_price(market, quote.get("f43")) if quote else (closes[-1] if closes else 0.0)
    ma5 = _ma(closes, 5)
    ma20 = _ma(closes, 20)
    ma60 = _ma(closes, 60)
    dist20 = ((price / ma20) - 1) * 100 if ma20 else None
    daily_rsi = _rsi(closes, 14)
    weekly_rsi = _rsi(weekly_closes, 14)
    trend = _stock_trend(price, ma5, ma20, ma60)
    volume_state = _volume_state(volumes)
    higher_lows = _higher_lows(daily)
    recent_high = max(closes[-20:]) if len(closes) >= 20 else (max(closes) if closes else price)
    prev_close = closes[-2] if len(closes) >= 2 else (closes[-1] if closes else price)

    if not closes:
        review = StockReview(
            name=item["name"],
            code=code.zfill(5) if market == "hk" and code.isdigit() else code,
            market=market,
            industry=item.get("industry", ""),
            price=round(price, 3 if market == "hk" else 2),
            change_pct=round(float(quote.get("f170") or 0) / 100, 2),
            ma20=None,
            ma60=None,
            dist20=None,
            daily_rsi=None,
            weekly_rsi=None,
            trend="数据不足",
            volume_state="未知",
            higher_lows=False,
            score_breakdown={
                "股价位置": 0,
                "K线": 0,
                "均线": 0,
                "RSI": 0,
                "量能": 0,
                "大盘": _classify_market_score(market_ok),
            },
            score_total=_classify_market_score(market_ok),
            action="观望",
            reason_lines=[
                "东方财富接口暂时未返回该标的的完整日线数据，本轮未做完整技术评分。",
                f"当前仅保留市场环境分 {_classify_market_score(market_ok)}/2，因此先归为观望而不是直接下结论。",
            ],
        )
        return review

    position_score = 0
    if dist20 is not None:
        if -6 <= dist20 <= 3:
            position_score = 2
        elif -10 <= dist20 <= 8:
            position_score = 1

    kline_score = 0
    if price >= prev_close and price >= recent_high * 0.98:
        kline_score = 2
    elif price >= prev_close or higher_lows:
        kline_score = 1

    ma_score = 2 if trend == "多头" else 1 if trend == "震荡" else 0

    rsi_score = 0
    if daily_rsi is not None:
        if 40 <= daily_rsi <= 65 and (weekly_rsi is None or weekly_rsi < 70):
            rsi_score = 2
        elif 30 <= daily_rsi < 40 or 65 < daily_rsi <= 75:
            rsi_score = 1

    if volume_state in {"放量", "正常"}:
        volume_score = 2
    else:
        volume_score = 1

    market_score = _classify_market_score(market_ok)
    total_score = position_score + kline_score + ma_score + rsi_score + volume_score + market_score

    if total_score >= 10 and trend == "多头":
        action = "买入"
    elif total_score >= 7:
        action = "观望"
    else:
        action = "回避"

    review = StockReview(
        name=item["name"],
        code=code.zfill(5) if market == "hk" and code.isdigit() else code,
        market=market,
        industry=item.get("industry", ""),
        price=round(price, 3 if market == "hk" else 2),
        change_pct=round(float(quote.get("f170") or 0) / 100, 2),
        ma20=round(ma20, 3 if market == "hk" else 2) if ma20 is not None else None,
        ma60=round(ma60, 3 if market == "hk" else 2) if ma60 is not None else None,
        dist20=round(dist20, 2) if dist20 is not None else None,
        daily_rsi=round(daily_rsi, 1) if daily_rsi is not None else None,
        weekly_rsi=round(weekly_rsi, 1) if weekly_rsi is not None else None,
        trend=trend,
        volume_state=volume_state,
        higher_lows=higher_lows,
        score_breakdown={
            "股价位置": position_score,
            "K线": kline_score,
            "均线": ma_score,
            "RSI": rsi_score,
            "量能": volume_score,
            "大盘": market_score,
        },
        score_total=total_score,
        action=action,
        reason_lines=[],
    )
    review.reason_lines = _build_reasons(
        review,
        position_score=position_score,
        kline_score=kline_score,
        ma_score=ma_score,
        rsi_score=rsi_score,
        volume_score=volume_score,
        market_score=market_score,
    )
    return review


def _render_market(markets: list[MarketState], a_share_ok: bool, hk_ok: bool) -> list[str]:
    lines = ["### 1. 大盘结论"]
    for item in markets:
        weekly = item.weekly_rsi if item.weekly_rsi is not None else "NA"
        lines.append(
            f"- **{item.name}**：{item.price}，{item.change_pct:+.2f}%，{item.trend}，周 RSI {weekly}"
        )
    lines.append(f"- **A股环境**：{'可操作，但偏向筛强做强' if a_share_ok else '偏弱，先保守'}")
    lines.append(f"- **港股环境**：{'可适度参与' if hk_ok else '恒指偏弱，解释更保守'}")
    return lines


def _render_counts(reviews: list[StockReview]) -> list[str]:
    buy_count = sum(item.action == "买入" for item in reviews)
    watch_count = sum(item.action == "观望" for item in reviews)
    avoid_count = sum(item.action == "回避" for item in reviews)
    return [
        "### 2. 批量结果",
        f"- **买入**：{buy_count}",
        f"- **观望**：{watch_count}",
        f"- **回避**：{avoid_count}",
    ]


def _pick_focus(reviews: list[StockReview], top: int) -> tuple[list[StockReview], list[StockReview], list[StockReview]]:
    sorted_reviews = sorted(
        reviews,
        key=lambda item: (item.score_total, item.change_pct, item.daily_rsi or -1),
        reverse=True,
    )
    buy = [item for item in sorted_reviews if item.action == "买入"][:top]
    watch = [item for item in sorted_reviews if item.action == "观望"][:top]
    avoid = [item for item in reversed(sorted_reviews) if item.action == "回避"][:top]
    return buy, watch, avoid


def _render_focus(reviews: list[StockReview], top: int) -> list[str]:
    buy, watch, avoid = _pick_focus(reviews, top)
    lines = ["### 3. 具体到重点票，为什么这么判"]

    def render_group(title: str, items: list[StockReview]) -> None:
        lines.append(f"#### {title}")
        if not items:
            lines.append("- 本轮没有符合条件的重点标的。")
            return
        for item in items:
            industry = f"，{item.industry}" if item.industry else ""
            lines.append(f"- **{item.name}（{item.code}）**{industry}")
            for reason in item.reason_lines:
                lines.append(f"  - {reason}")

    render_group("买入", buy)
    render_group("强观望", watch)
    render_group("回避", avoid)
    return lines


def build_report(limit: int | None = None, top: int = 6) -> str:
    config = _load_watchlist()
    markets, a_share_ok, hk_ok = _review_markets(config)
    stocks = config["stocks"][:limit] if limit else config["stocks"]

    reviews: list[StockReview] = []
    for item in stocks:
        market_ok = a_share_ok if item["market"] in {"sh", "sz"} else hk_ok
        reviews.append(_review_stock(item, market_ok=market_ok))

    sections = ["## 默认股票列表批量分析"]
    sections.extend(_render_market(markets, a_share_ok, hk_ok))
    sections.append("")
    sections.extend(_render_counts(reviews))
    sections.append("")
    sections.extend(_render_focus(reviews, top=top))
    sections.append("")
    sections.append("> 仅供学习参考，不构成投资建议。")
    return "\n".join(sections)


def send_wechat(title: str, content: str) -> dict[str, Any]:
    sys.path.insert(0, str(PROJECT_ROOT))
    from quant_system.notification import notification_manager

    return notification_manager.send_markdown_message(title, content, channels=["wechat"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="批量复盘默认股票列表")
    parser.add_argument("--limit", type=int, default=None, help="仅分析前 N 只股票，便于调试")
    parser.add_argument("--top", type=int, default=6, help="每个分类展示前 N 只重点票")
    parser.add_argument("--send-wechat", action="store_true", help="将 Markdown 结果发送到微信")
    return parser.parse_args()


def main() -> None:
    _ensure_utf8()
    args = parse_args()
    report = build_report(limit=args.limit, top=args.top)
    print(report)
    if args.send_wechat:
        result = send_wechat("默认股票列表批量分析", report)
        print(f"\n[微信发送结果] {result}")


if __name__ == "__main__":
    main()
