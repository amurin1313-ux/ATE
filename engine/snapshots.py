import os
import json
import time
from typing import Any, Dict


def _safe_name(s: str) -> str:
    s = s or "snapshot"
    out = []
    for c in s:
        if c.isalnum() or c in ("-", "_", "."):
            out.append(c)
        else:
            out.append("_")
    return "".join(out)[:80] or "snapshot"


def write_snapshot(data_dir: str, *, name: str, payload: Dict[str, Any]) -> str:
    """Сохраняет диагностический snapshot в data/snapshots/."""
    snaps_dir = os.path.join(data_dir, "snapshots")
    os.makedirs(snaps_dir, exist_ok=True)

    ts = time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())
    fname = f"{ts}_{_safe_name(name)}.json"
    path = os.path.join(snaps_dir, fname)

    # ensure json-serializable (best-effort)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except TypeError:
        # fallback: stringify non-serializable values
        def _default(obj: Any) -> str:
            try:
                return str(obj)
            except Exception:
                return "<unserializable>"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=_default)

    return path
