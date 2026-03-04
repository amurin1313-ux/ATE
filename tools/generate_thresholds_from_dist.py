"""Generate per-symbol thresholds for StrategyV3 from decision_logs.

Usage examples (Windows):
  python tools/generate_thresholds_from_dist.py --input "C:\path\to\dist\ATE_6PRO\data\decision_logs" --out data/per_symbol_thresholds.generated.json

Or directly from a zip with dist folder inside:
  python tools/generate_thresholds_from_dist.py --zip "C:\path\to\dist 24.02.2026.zip" --out data/per_symbol_thresholds.generated.json

What it does:
 - Reads *.jsonl.gz (and optionally *.jsonl) decision logs.
 - Extracts key features (RSI/ATR/Spread/Volume).
 - Builds robust per-symbol thresholds using quantiles.
 - Writes JSON {"symbols": {"ETH-USDT": {...}}}.

Notes:
 - This is deterministic and reproducible.
 - It does NOT try to "optimize" for profit (that would overfit).
   It just calibrates thresholds to each symbol's typical volatility/liquidity.
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import zipfile
from collections import defaultdict
from typing import Dict, Any, Iterable, Tuple, Optional


FIELDS = ["rsi14", "atr14_pct", "spread_pct", "volume", "vol_sma20"]


def _iter_lines_from_folder(folder: str) -> Iterable[str]:
    for fn in sorted(os.listdir(folder)):
        p = os.path.join(folder, fn)
        if fn.endswith(".jsonl.gz"):
            with gzip.open(p, "rt", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    yield line
        elif fn.endswith(".jsonl"):
            with open(p, "rt", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    yield line


def _iter_lines_from_zip(zip_path: str) -> Iterable[str]:
    with zipfile.ZipFile(zip_path, "r") as z:
        names = [n for n in z.namelist() if "/data/decision_logs/" in n and (n.endswith(".jsonl.gz") or n.endswith(".jsonl"))]
        for name in sorted(names):
            with z.open(name, "r") as raw:
                if name.endswith(".gz"):
                    data = gzip.decompress(raw.read())
                    stream = io.StringIO(data.decode("utf-8", errors="ignore"))
                else:
                    stream = io.StringIO(raw.read().decode("utf-8", errors="ignore"))
                for line in stream:
                    yield line


def _q(arr, p: float) -> Optional[float]:
    if not arr:
        return None
    arr = sorted(arr)
    idx = int((len(arr) - 1) * p)
    return float(arr[idx])


def build_thresholds(lines: Iterable[str], max_per_symbol: int = 20000) -> Tuple[Dict[str, Any], int]:
    samples = defaultdict(lambda: {k: [] for k in FIELDS})
    seen = 0
    for line in lines:
        try:
            obj = json.loads(line)
        except Exception:
            continue
        feats = obj.get("features") or {}
        sym = (feats.get("symbol") or obj.get("symbol") or "").strip().upper()
        if not sym:
            continue
        d = samples[sym]
        for k in FIELDS:
            v = feats.get(k)
            if v is None:
                continue
            try:
                v = float(v)
            except Exception:
                continue
            arr = d[k]
            if len(arr) < max_per_symbol:
                arr.append(v)
        seen += 1

    thresholds: Dict[str, Any] = {}
    for sym, d in samples.items():
        rsi_buy = _q(d["rsi14"], 0.10) or 30.0
        rsi_sell = _q(d["rsi14"], 0.90) or 70.0
        spread_max = _q(d["spread_pct"], 0.95) or 0.08
        atr_p50 = _q(d["atr14_pct"], 0.50) or 0.20

        rsi_buy = float(max(15.0, min(45.0, rsi_buy)))
        rsi_sell = float(max(55.0, min(85.0, rsi_sell)))
        spread_max = float(max(0.02, min(0.25, spread_max)))

        sl_mult = 1.0 + (min(0.8, max(0.0, (atr_p50 - 0.15))) / 0.35) * 0.5
        tp_mult = 0.9 + (min(0.8, max(0.0, (atr_p50 - 0.15))) / 0.35) * 0.4
        sl_mult = float(max(0.9, min(2.2, sl_mult)))
        tp_mult = float(max(0.8, min(2.0, tp_mult)))

        vol_ratio = []
        for v, sma in zip(d["volume"], d["vol_sma20"]):
            if sma and sma > 0:
                vol_ratio.append(v / sma)
        vol_ratio_min = _q(vol_ratio, 0.60) or 1.05
        vol_ratio_min = float(max(0.9, min(2.5, vol_ratio_min)))

        thresholds[sym] = {
            "rsi_buy": round(rsi_buy, 2),
            "rsi_sell": round(rsi_sell, 2),
            "spread_max": round(spread_max, 4),
            "atr_sl_mult": round(sl_mult, 3),
            "atr_tp_mult": round(tp_mult, 3),
            "vol_ratio_min": round(vol_ratio_min, 3),
        }

    return {"symbols": thresholds}, seen


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", help="Path to decision_logs folder")
    ap.add_argument("--zip", help="Path to a dist zip containing /data/decision_logs")
    ap.add_argument("--out", required=True, help="Output JSON path")
    ap.add_argument("--max-per-symbol", type=int, default=20000)
    args = ap.parse_args()

    if not args.input and not args.zip:
        raise SystemExit("Provide --input or --zip")

    if args.zip:
        lines = _iter_lines_from_zip(args.zip)
    else:
        lines = _iter_lines_from_folder(args.input)

    data, seen = build_thresholds(lines, max_per_symbol=int(args.max_per_symbol))
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump({"generated": True, "seen_lines": int(seen), **data}, f, ensure_ascii=False, indent=2)
    print(f"OK: wrote {args.out} (symbols={len(data['symbols'])}, lines={seen})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
