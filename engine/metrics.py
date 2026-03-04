from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple
import math, statistics
from datetime import datetime

def _safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def rsi(prices: List[float], period: int = 14) -> float:
    if len(prices) < period + 1:
        return 50.0
    gains = []
    losses = []
    for i in range(1, period + 1):
        diff = prices[-i] - prices[-i-1]
        if diff >= 0:
            gains.append(diff)
        else:
            losses.append(-diff)
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

def ema(prices: List[float], period: int) -> float:
    if not prices:
        return 0.0
    k = 2.0 / (period + 1.0)
    e = prices[0]
    for p in prices[1:]:
        e = p * k + e * (1.0 - k)
    return e

def macd(prices: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[float, float, float]:
    if len(prices) < slow + signal:
        e12 = ema(prices, fast)
        e26 = ema(prices, slow)
        m = e12 - e26
        return m, 0.0, 0.0
    e12 = ema(prices, fast)
    e26 = ema(prices, slow)
    line = e12 - e26
    # build macd series to compute signal
    macd_series = []
    for i in range(len(prices)):
        sub = prices[:i+1]
        macd_series.append(ema(sub, fast) - ema(sub, slow))
    sig = ema(macd_series[-signal:], signal)
    hist = line - sig
    return line, sig, hist

def atr_pct(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    n = min(len(highs), len(lows), len(closes))
    if n < period + 1:
        return 0.0
    trs = []
    for i in range(n - period, n):
        h = highs[i]
        l = lows[i]
        prev_close = closes[i-1]
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
    atr = sum(trs) / period
    last = closes[-1] if closes else 0.0
    return (atr / last * 100.0) if last else 0.0

def bollinger(prices: List[float], period: int = 20, mult: float = 2.0) -> Tuple[float, float, float]:
    if len(prices) < period:
        p = prices[-1] if prices else 0.0
        return p, p, 0.0
    window = prices[-period:]
    sma = sum(window) / period
    st = statistics.pstdev(window) if period > 1 else 0.0
    upper = sma + mult * st
    lower = sma - mult * st
    width = (upper - lower) / sma if sma else 0.0
    return upper, lower, width

def returns_pct(prices: List[float], back: int) -> float:
    if len(prices) <= back:
        return 0.0
    a = prices[-1]
    b = prices[-1-back]
    return ((a - b) / b * 100.0) if b else 0.0

def slope_pct(prices: List[float], points: int = 30) -> float:
    if len(prices) < points:
        return 0.0
    y = prices[-points:]
    x = list(range(points))
    x_mean = (points - 1) / 2
    y_mean = sum(y) / points
    num = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
    den = sum((xi - x_mean) ** 2 for xi in x) or 1.0
    slope = num / den
    # slope per bar -> pct of last price
    last = y[-1] or 1.0
    return slope / last * 100.0

def vol_sma(volumes: List[float], period: int = 20) -> float:
    if len(volumes) < period:
        return sum(volumes) / max(len(volumes), 1)
    w = volumes[-period:]
    return sum(w) / period

def volatility_1h_pct(prices_1m: List[float]) -> float:
    # std of 1m returns for last 60 points -> approx 1h
    if len(prices_1m) < 61:
        return 0.0
    rets = []
    for i in range(-60, 0):
        p0 = prices_1m[i-1]
        p1 = prices_1m[i]
        if p0:
            rets.append((p1 - p0) / p0)
    if len(rets) < 2:
        return 0.0
    return statistics.pstdev(rets) * 100.0

def book_imbalance(book: Dict[str, Any]) -> float:
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    b = sum(_safe_float(x[1]) for x in bids[:10]) or 0.0
    a = sum(_safe_float(x[1]) for x in asks[:10]) or 0.0
    if (a + b) == 0:
        return 0.0
    return (b - a) / (a + b)

def buy_ratio_from_trades(trades: List[Dict[str, Any]]) -> float:
    # OKX trade has side: buy/sell
    if not trades:
        return 0.5
    buy = 0.0
    sell = 0.0
    for t in trades:
        side = (t.get("side") or "").lower()
        sz = _safe_float(t.get("sz"), 0.0)
        if side == "buy":
            buy += sz
        elif side == "sell":
            sell += sz
    tot = buy + sell
    if tot == 0:
        return 0.5
    return buy / tot
