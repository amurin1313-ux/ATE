"""Быстрый анализ рыночных метрик OKX для калибровки порогов входа/выхода.

Скрипт не меняет конфиг автоматически, а выдаёт рекомендации.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from statistics import median
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx.public_client import OKXPublicClient


def _atr_pct(candles: List[List[str]]) -> float:
    if not candles or len(candles) < 20:
        return 0.0
    # OKX candles: [ts,o,h,l,c,vol,volCcy,volCcyQuote,confirm]
    rows = list(reversed(candles))
    highs = [float(r[2]) for r in rows]
    lows = [float(r[3]) for r in rows]
    closes = [float(r[4]) for r in rows]
    trs = []
    prev_close = closes[0]
    for h, l, c in zip(highs[1:], lows[1:], closes[1:]):
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
        prev_close = c
    if not trs:
        return 0.0
    atr = sum(trs[-14:]) / min(14, len(trs))
    last = closes[-1] if closes[-1] > 0 else 1.0
    return (atr / last) * 100.0


def analyze(symbols: List[str]) -> tuple[Dict[str, Dict[str, float]], List[str]]:
    c = OKXPublicClient(timeout_sec=7.0)
    out: Dict[str, Dict[str, float]] = {}
    errors: List[str] = []
    for s in symbols:
        try:
            t = c.ticker(s)
            bid = float(t.get("bidPx") or 0.0)
            ask = float(t.get("askPx") or 0.0)
            last = float(t.get("last") or 0.0)
            spread_pct = ((ask - bid) / last * 100.0) if bid > 0 and ask > 0 and last > 0 else 0.0
            candles = c.candles(s, bar="1m", limit=120)
            atrp = _atr_pct(candles)
            out[s] = {"last": last, "spread_pct": spread_pct, "atrp_1m_pct": atrp}
        except Exception as e:
            errors.append(f"{s}: {e.__class__.__name__}: {e}")
    return out, errors


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default="BTC-USDT,ETH-USDT,SOL-USDT")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    symbols = [x.strip().upper() for x in str(args.symbols).split(",") if x.strip()]
    data, errors = analyze(symbols)

    spreads = [v["spread_pct"] for v in data.values() if v["spread_pct"] > 0]
    atrps = [v["atrp_1m_pct"] for v in data.values() if v["atrp_1m_pct"] > 0]

    rec = {
        "symbols": data,
        "summary": {
            "median_spread_pct": median(spreads) if spreads else 0.0,
            "median_atrp_1m_pct": median(atrps) if atrps else 0.0,
        },
        "errors": errors,
        "recommendation": {
            "max_spread_buy_pct": round(max(0.05, (median(spreads) * 3.0) if spreads else 0.22), 4),
            "note": "Рекомендация эвристическая; подтверждайте на decision logs/replay.",
        },
    }

    txt = json.dumps(rec, ensure_ascii=False, indent=2)
    print(txt)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(txt + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
