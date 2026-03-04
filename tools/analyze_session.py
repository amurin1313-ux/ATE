#!/usr/bin/env python3
"""Analyze a local ATE session (portable).

Usage:
  python tools/analyze_session.py --data ./data --decisions ./data/decision_logs

Produces:
  - stdout summary
  - report markdown file in ./data/reports/ (if possible)

No network, safe to run in corporate environment.
"""

from __future__ import annotations

import argparse
import json
import os
import glob
import time
from collections import defaultdict, Counter
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


def _read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _iter_jsonl(paths: List[str]):
    for p in paths:
        try:
            with open(p, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield p, json.loads(line)
                    except Exception:
                        continue
        except Exception:
            continue


def _fmt_pct(x: float) -> str:
    return f"{x*100:.2f}%"


def _fmt_usd(x: float) -> str:
    s = f"{x:.2f}"
    return ("+" if x > 0 else "") + s


def _trade_net_pnl_usd(t: Dict[str, Any]) -> float:
    buy = float(t.get("buy_usd") or 0.0)
    sell = float(t.get("sell_usd") or 0.0)
    buy_fee = float(t.get("buy_fee_usd") or 0.0)
    sell_fee = float(t.get("sell_fee_usd") or 0.0)
    return (sell - buy) - (buy_fee + sell_fee)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="./data", help="Path to data folder")
    ap.add_argument("--decisions", default=None, help="Path to decision_logs folder (default: <data>/decision_logs)")
    ap.add_argument("--top", type=int, default=10, help="How many top rows to print")
    args = ap.parse_args()

    data_dir = os.path.abspath(args.data)
    dec_dir = os.path.abspath(args.decisions or os.path.join(data_dir, "decision_logs"))

    ledger_path = os.path.join(data_dir, "trade_ledger.json")
    if not os.path.exists(ledger_path):
        print(f"[ERROR] trade_ledger.json not found: {ledger_path}")
        return 2

    ledger = _read_json(ledger_path)
    closed = list(ledger.get("closed_trades") or [])
    open_trades = ledger.get("open_trades") or {}

    # Closed trades PnL table
    rows: List[Tuple[str, float, float, float]] = []  # symbol, net_usd, net_pct, duration_sec
    for t in closed:
        try:
            sym = str(t.get("symbol") or "")
            net = _trade_net_pnl_usd(t)
            buy = float(t.get("buy_usd") or 1.0)
            pct = net / buy if buy > 0 else 0.0
            dur = float(t.get("sell_ts") or 0.0) - float(t.get("buy_ts") or 0.0)
            rows.append((sym, net, pct, dur))
        except Exception:
            continue

    rows.sort(key=lambda x: x[1])
    total_net = sum(r[1] for r in rows)

    print("\n=== ATE Session Summary ===")
    print(f"Data folder: {data_dir}")
    print(f"Closed trades: {len(closed)} | Open trades: {len(open_trades)}")
    print(f"Closed net PnL: {_fmt_usd(total_net)} USDT")

    if rows:
        print("\nWorst closed trades:")
        for sym, net, pct, dur in rows[: args.top]:
            print(f"- {sym:<12} net={_fmt_usd(net):>8}  pct={_fmt_pct(pct):>8}  hold={dur/60:>6.1f}m")

        print("\nBest closed trades:")
        for sym, net, pct, dur in sorted(rows, key=lambda x: x[1], reverse=True)[: args.top]:
            print(f"- {sym:<12} net={_fmt_usd(net):>8}  pct={_fmt_pct(pct):>8}  hold={dur/60:>6.1f}m")

    # Decisions / blocked exits analysis
    if not os.path.isdir(dec_dir):
        print(f"\n[WARN] decision_logs folder not found: {dec_dir}")
        dec_paths = []
    else:
        dec_paths = sorted(glob.glob(os.path.join(dec_dir, "decisions_*.jsonl")))
        dec_paths += sorted(glob.glob(os.path.join(dec_dir, "decisions_*.jsonl.gz")))

    # Only parse plain jsonl here (gz parsing intentionally skipped to stay dependency-free)
    dec_paths = [p for p in dec_paths if p.endswith(".jsonl")]

    blocked_sell = Counter()
    blocked_sell_sym = Counter()
    force_exit_hits = Counter()
    raw_stop_blocked = Counter()

    for _, rec in _iter_jsonl(dec_paths):
        if rec.get("type") != "tick":
            continue
        sym = str(rec.get("symbol") or "")
        dec = (rec.get("decision") or {})
        raw = (dec.get("raw") or {})
        ui = (dec.get("ui") or {})
        raw_action = str(raw.get("action") or "").upper()
        ui_action = str(ui.get("action_ui") or "").upper()
        reason_ui = str((rec.get("thresholds") or {}).get("reason_ui") or ui.get("reason_ui") or "")
        force_exit = bool((ui or {}).get("force_exit") or False)

        # raw SELL but UI did not allow
        if raw_action == "SELL" and ui_action != "SELL":
            blocked_sell[reason_ui] += 1
            blocked_sell_sym[sym] += 1
            if "Stop-loss" in str(raw.get("reason") or ""):
                raw_stop_blocked[sym] += 1

        if force_exit and ui_action == "SELL":
            kind = str((ui or {}).get("force_exit_kind") or "")
            force_exit_hits[kind or "unknown"] += 1

    if dec_paths:
        print("\nDecision logs (ticks) analyzed:", len(dec_paths), "files")
        if blocked_sell:
            print("Raw SELL blocked by UI (top reasons):")
            for k, v in blocked_sell.most_common(args.top):
                print(f"- {k or 'UNKNOWN'}: {v}")
            print("Top symbols with blocked exits:")
            for k, v in blocked_sell_sym.most_common(args.top):
                print(f"- {k}: {v}")
        else:
            print("No blocked raw SELL events found in parsed tick logs.")

        if force_exit_hits:
            print("Force-exit SELL hits (by kind):")
            for k, v in force_exit_hits.most_common(args.top):
                print(f"- {k}: {v}")

        if raw_stop_blocked:
            print("\n[IMPORTANT] Raw Stop-loss blocked by UI (symbols):")
            for k, v in raw_stop_blocked.most_common(args.top):
                print(f"- {k}: {v}")

    # Write markdown report
    try:
        rep_dir = os.path.join(data_dir, "reports")
        os.makedirs(rep_dir, exist_ok=True)
        ts = time.strftime("%Y-%m-%d_%H-%M-%S")
        rep_path = os.path.join(rep_dir, f"session_report_{ts}.md")
        with open(rep_path, "w", encoding="utf-8") as f:
            f.write("# ATE Session Report\n\n")
            f.write(f"- Data: `{data_dir}`\n")
            f.write(f"- Closed trades: {len(closed)}\n")
            f.write(f"- Open trades: {len(open_trades)}\n")
            f.write(f"- Closed net PnL: {_fmt_usd(total_net)} USDT\n\n")
            f.write("## Worst closed trades\n\n")
            f.write("|symbol|net_usd|net_pct|hold_min|\n|---|---:|---:|---:|\n")
            for sym, net, pct, dur in rows[: args.top]:
                f.write(f"|{sym}|{net:.4f}|{pct*100:.3f}%|{dur/60:.1f}|\n")
            f.write("\n## Best closed trades\n\n")
            f.write("|symbol|net_usd|net_pct|hold_min|\n|---|---:|---:|---:|\n")
            for sym, net, pct, dur in sorted(rows, key=lambda x: x[1], reverse=True)[: args.top]:
                f.write(f"|{sym}|{net:.4f}|{pct*100:.3f}%|{dur/60:.1f}|\n")
            if dec_paths:
                f.write("\n## Decision logs analysis\n\n")
                if blocked_sell:
                    f.write("### Raw SELL blocked by UI (top reasons)\n\n")
                    for k, v in blocked_sell.most_common(args.top):
                        f.write(f"- {k or 'UNKNOWN'}: {v}\n")
                    f.write("\n### Top symbols with blocked exits\n\n")
                    for k, v in blocked_sell_sym.most_common(args.top):
                        f.write(f"- {k}: {v}\n")
                if force_exit_hits:
                    f.write("\n### Force-exit SELL hits\n\n")
                    for k, v in force_exit_hits.most_common(args.top):
                        f.write(f"- {k}: {v}\n")
        print(f"\nReport written: {rep_path}")
    except Exception as e:
        print(f"\n[WARN] report write failed: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
