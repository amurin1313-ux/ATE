"""Replay / Compare decisions from Decision Log.

Usage:
  python tools/replay_decisions.py --log data/decision_logs/decisions_YYYY-MM-DD.jsonl

This script re-runs the strategy on recorded inputs and compares outputs.
It helps prove that "brain" (strategy decide) did not change between versions.

Everything is offline: no network calls.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import datetime
from typing import Any, Dict, Tuple

# allow running from project root
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from engine.strategy_runtime import StrategyRegistry, SymbolStrategyInstance


def _find_newest_log(data_dir: str) -> str:
    d = os.path.join(data_dir, "decision_logs")
    if not os.path.isdir(d):
        raise FileNotFoundError(f"Decision log folder not found: {d}")
    files = [os.path.join(d, f) for f in os.listdir(d) if f.startswith("decisions_") and f.endswith(".jsonl")]
    if not files:
        raise FileNotFoundError(f"No decision logs in: {d}")
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]


def _float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", default="", help="Path to decisions_*.jsonl. If empty, picks newest from ./data.")
    ap.add_argument("--data", default="data", help="Base data dir (default: ./data)")
    ap.add_argument("--only-signals", action="store_true", help="Replay only type=signal records")
    ap.add_argument("--max", type=int, default=0, help="Max records to process (0 = no limit)")
    ap.add_argument("--out", default="", help="Output report path (json). Default: ./data/replay_report_*.json")
    ap.add_argument("--tol", type=float, default=1e-3, help="Tolerance for score diff")
    args = ap.parse_args()

    log_path = args.log.strip() or _find_newest_log(args.data.strip() or "data")
    if not os.path.exists(log_path):
        raise FileNotFoundError(log_path)

    registry = StrategyRegistry()
    instances: Dict[Tuple[str, str], SymbolStrategyInstance] = {}

    total = 0
    matched_action = 0
    mism_action = 0
    mism_scores = 0
    diffs = []

    t0 = time.time()
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue

            rtype = str(rec.get("type") or "")
            if args.only_signals and rtype != "signal":
                continue
            if rtype not in ("tick", "signal"):
                continue

            symbol = str(rec.get("symbol") or "")
            features = rec.get("features") or {}
            position = rec.get("position") or {}
            portfolio = rec.get("portfolio") or {}
            decision_logged = (rec.get("decision") or {}).get("raw") or {}

            thr = rec.get("thresholds") or {}
            strat_name = str(thr.get("strategy_name") or rec.get("strategy_name") or "") or "StrategyV3"
            strat_params = thr.get("strategy_params") or rec.get("strategy_params") or {}

            # key: symbol + strategy name (params are shared across symbols in this проект)
            inst_key = (symbol, strat_name)
            inst = instances.get(inst_key)
            if inst is None:
                try:
                    inst = SymbolStrategyInstance(registry.create(strat_name), strategy_params=strat_params)
                except Exception:
                    inst = SymbolStrategyInstance(registry.create("StrategyV3"), strategy_params=strat_params)
                instances[inst_key] = inst

            # replay
            try:
                decision_replay = inst.decide(features=features, position=position, portfolio_state=portfolio)
            except Exception as e:
                decision_replay = {"action": "HOLD", "meta": {}, "error": str(e)}

            a0 = str(decision_logged.get("action") or "HOLD").upper()
            a1 = str(decision_replay.get("action") or "HOLD").upper()

            m0 = decision_logged.get("meta") or {}
            m1 = decision_replay.get("meta") or {}
                        # v3: сравниваем score (confidence)
            p0 = _float(m0.get('confidence') or 0.0)
            p1 = _float(m1.get('confidence') or 0.0)

            total += 1
            if a0 == a1:
                matched_action += 1
            else:
                mism_action += 1
                if len(diffs) < 200:
                    diffs.append({
                        "kind": "action",
                        "symbol": symbol,
                        "ts": rec.get("ts"),
                        "a_log": a0,
                        "a_replay": a1,
                        "score_log": p0,
                        
                        "score_replay": p1,
                        
                    })

            if abs(p0b - p1b) > args.tol or abs(p0s - p1s) > args.tol:
                mism_scores += 1
                if len(diffs) < 200 and (not diffs or diffs[-1].get("ts") != rec.get("ts")):
                    diffs.append({
                        "kind": "scores",
                        "symbol": symbol,
                        "ts": rec.get("ts"),
                        "score_log": p0,
                        
                        "score_replay": p1,
                        
                    })

            if args.max and total >= args.max:
                break

    dt = time.time() - t0
    report = {
        "log_path": os.path.abspath(log_path),
        "processed": total,
        "matched_action": matched_action,
        "mismatched_action": mism_action,
        "mismatched_scores": mism_scores,
        "action_match_rate": (matched_action / total) if total else 0.0,
        "seconds": dt,
        "diff_samples": diffs,
        "generated_utc": datetime.datetime.utcnow().isoformat() + "Z",
    }

    out = args.out.strip()
    if not out:
        out = os.path.join(args.data.strip() or "data", f"replay_report_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as wf:
        json.dump(report, wf, ensure_ascii=False, indent=2)

    print("Replay done")
    print(f"Log: {report['log_path']}")
    print(f"Processed: {total} records in {dt:.2f}s")
    print(f"Action match: {matched_action}/{total} = {report['action_match_rate']:.4f}")
    print(f"Mismatched scores: {mism_scores}")
    print(f"Report: {os.path.abspath(out)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
