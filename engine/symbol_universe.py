from __future__ import annotations

import os
from typing import List


def _dedupe_keep_order(items: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for s in items or []:
        s2 = str(s or "").strip().upper()
        if not s2 or s2 in seen:
            continue
        seen.add(s2)
        out.append(s2)
    return out


def _ensure_btc_eth_first(items: List[str]) -> List[str]:
    # Requirement: ETH must be mandatory and placed immediately after BTC.
    items = _dedupe_keep_order(items)
    rest = [s for s in items if s not in ("BTC-USDT", "ETH-USDT")]
    out: List[str] = ["BTC-USDT", "ETH-USDT"]
    out.extend(rest)
    return _dedupe_keep_order(out)


def load_symbol_universe(data_dir: str, *, fallback: List[str]) -> List[str]:
    """Load OKX symbol universe.

    Priority:
    1) data/okx_symbol_universe.txt (one symbol per line, comments with #)
    2) fallback embedded list

    Always:
    - normalize to UPPER
    - keep only '*-USDT'
    - BTC-USDT first, ETH-USDT second
    """
    txt_path = os.path.join(str(data_dir), "data", "okx_symbol_universe.txt")
    items: List[str] = []

    if os.path.exists(txt_path):
        try:
            for raw in open(txt_path, "r", encoding="utf-8").read().splitlines():
                line = str(raw or "").strip()
                if not line or line.startswith("#"):
                    continue
                # allow comma-separated too
                parts = [p.strip() for p in line.replace(";", ",").split(",") if p.strip()]
                for p in parts:
                    s = str(p).strip().upper()
                    if not s:
                        continue
                    if "-" not in s:
                        continue
                    if not s.endswith("-USDT"):
                        continue
                    items.append(s)
        except Exception:
            items = []

    if not items:
        items = [str(x or "").strip().upper() for x in (fallback or []) if str(x or "").strip()]

    items = [s for s in items if s.endswith("-USDT") and "-" in s]
    return _ensure_btc_eth_first(items)
