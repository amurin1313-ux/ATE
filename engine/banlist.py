from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Tuple


@dataclass
class BanRecord:
    symbol: str
    until_ts: float
    reason: str = ""
    source: str = "runtime"  # order / preflight / marketdata / winrate / user
    failures: int = 0

    def active(self, now: Optional[float] = None) -> bool:
        now = float(now or time.time())
        return self.until_ts > now

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "until_ts": float(self.until_ts),
            "reason": str(self.reason or ""),
            "source": str(self.source or "runtime"),
            "failures": int(self.failures or 0),
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "BanRecord":
        return BanRecord(
            symbol=str(d.get("symbol") or ""),
            until_ts=float(d.get("until_ts") or 0.0),
            reason=str(d.get("reason") or ""),
            source=str(d.get("source") or "runtime"),
            failures=int(d.get("failures") or 0),
        )


@dataclass
class BanList:
    """TTL‑ban list for symbols.

    Отличие от permanent untradeable:
    - временно выключаем символ на N минут после повторяющихся сбоев (market data / API reject)
    - по истечении TTL символ автоматически возвращается
    """

    path: str
    bans: Dict[str, BanRecord] = field(default_factory=dict)

    def load(self) -> None:
        try:
            if not os.path.exists(self.path):
                return
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            items = data.get("bans") if isinstance(data, dict) else data
            if isinstance(items, dict):
                # legacy map
                for sym, rec in items.items():
                    if isinstance(rec, dict):
                        r = BanRecord.from_dict({"symbol": sym, **rec})
                        self.bans[str(sym).upper()] = r
            elif isinstance(items, list):
                for rec in items:
                    if isinstance(rec, dict):
                        r = BanRecord.from_dict(rec)
                        if r.symbol:
                            self.bans[str(r.symbol).upper()] = r
        except Exception:
            return

    def save(self) -> None:
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            payload = {"bans": [r.to_dict() for r in self.bans.values()]}
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            return

    def cleanup(self) -> None:
        now = time.time()
        dead = [sym for sym, r in self.bans.items() if not r.active(now)]
        for sym in dead:
            self.bans.pop(sym, None)

    def is_banned(self, symbol: str) -> Tuple[bool, float, str]:
        sym = str(symbol or "").upper()
        if not sym:
            return False, 0.0, ""
        self.cleanup()
        r = self.bans.get(sym)
        if r and r.active():
            return True, float(r.until_ts), str(r.reason or "")
        return False, 0.0, ""

    def ban(self, symbol: str, ttl_sec: float, reason: str, source: str = "runtime", failures: int = 0) -> None:
        sym = str(symbol or "").upper()
        if not sym:
            return
        until = time.time() + max(0.0, float(ttl_sec or 0.0))
        r = BanRecord(symbol=sym, until_ts=until, reason=str(reason or ""), source=str(source or "runtime"), failures=int(failures or 0))
        self.bans[sym] = r
        self.save()

    def bump_failure_and_maybe_ban(self, symbol: str, *, ttl_sec: float, threshold: int, reason: str, source: str) -> Optional[BanRecord]:
        sym = str(symbol or "").upper()
        if not sym:
            return None
        self.cleanup()
        r = self.bans.get(sym)
        if r and r.active():
            # already banned
            return r
        # use failures map in memory (store as failures with very small ttl=0)
        # keep counters inside a dummy record with until_ts=0
        counter = getattr(self, "_failures", None)
        if not isinstance(counter, dict):
            counter = {}
            setattr(self, "_failures", counter)
        counter[sym] = int(counter.get(sym, 0)) + 1
        if counter[sym] >= int(threshold):
            self.ban(sym, ttl_sec=ttl_sec, reason=reason, source=source, failures=counter[sym])
            return self.bans.get(sym)
        return None
