"""Миграции данных между версиями.

Требование пользователя: никаких данных в системных/скрытых папках.
Но при установке новой версии в НОВУЮ папку некоторые важные списки
не должны теряться (заблокированные символы).

Решение:
- при первом запуске новой версии пытаемся найти предыдущую папку ATE
  в родительском каталоге (соседние директории)
- переносим/сливаем:
  1) data/untradeable_symbols.json (permanent disable)
  2) data/temp_bans.json (TTL bans)
  3) symbols.symbol_blacklist из data/config.json

Никаких внешних путей не используем: работаем только в пределах папок ATE.
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, Any, List, Optional, Tuple


def _read_json(path: str, default: Any) -> Any:
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            txt = f.read().strip()
        if not txt:
            return default
        return json.loads(txt)
    except Exception:
        return default


def _write_json(path: str, data: Any) -> bool:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False


def _norm_symbol(s: str) -> str:
    return str(s or "").strip().upper()


def _union_keep_order(a: List[str], b: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in (a or []) + (b or []):
        xs = _norm_symbol(x)
        if not xs:
            continue
        if xs in seen:
            continue
        seen.add(xs)
        out.append(xs)
    return out


def _find_candidate_data_dirs(current_base_path: str) -> List[str]:
    """Ищем предыдущие установки ATE в соседних папках."""
    base = os.path.abspath(current_base_path)
    parent = os.path.dirname(base)
    out: List[str] = []
    try:
        for name in os.listdir(parent):
            p = os.path.join(parent, name)
            if not os.path.isdir(p):
                continue
            if os.path.abspath(p) == base:
                continue
            d = os.path.join(p, "data")
            if os.path.isdir(d) and os.path.exists(os.path.join(d, "config.json")):
                out.append(d)
    except Exception:
        return []
    return out


def _pick_best_source(dirs: List[str]) -> Optional[str]:
    """Выбираем наиболее вероятно "последнюю" папку по времени обновления config.json."""
    best: Tuple[float, str] | None = None
    for d in dirs:
        try:
            p = os.path.join(d, "config.json")
            ts = os.path.getmtime(p) if os.path.exists(p) else 0.0
        except Exception:
            ts = 0.0
        if best is None or ts > best[0]:
            best = (ts, d)
    return best[1] if best else None


def migrate_blocked_symbols(*, base_path: str, data_dir: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Мигрирует/сливает блок-листы при первом запуске.

    Возвращает dict с результатом (для логов/UI).
    """

    result = {
        "ok": True,
        "did": False,
        "source": "",
        "migrated": {"untradeable": False, "temp_bans": False, "symbol_blacklist": False},
    }

    # Без скрытых файлов: явный marker в data/
    marker = os.path.join(data_dir, "migrated_blocked_symbols.json")
    try:
        if os.path.exists(marker):
            return result
    except Exception:
        pass

    candidates = _find_candidate_data_dirs(current_base_path=base_path)
    src = _pick_best_source(candidates)
    if not src:
        # нечего переносить
        try:
            _write_json(marker, {"ts": time.time(), "note": "no candidates"})
        except Exception:
            pass
        return result

    result["source"] = src

    # 0) Перенос ключевых торговых настроек (чтобы "мозг" не менялся из-за дефолтов)
    #    Переносим только если:
    #    - в источнике есть значение
    #    - в текущем конфиге ключ отсутствует ИЛИ равен дефолту (значит пользователь ещё не менял)
    try:
        src_cfg = _read_json(os.path.join(src, "config.json"), {})
        src_t = (src_cfg.get("trading", {}) or {})
        dst_t = cfg.setdefault("trading", {})

        defaults = {
            "cooldown_sec": 10.0,
            "default_order_usd": 20.0,
            "order_size_mode": "fixed",
            "order_size_pct": 5.0,
            "max_positions": 1,
            "min_order_usd": 10.0,
            "paper_trade": True,
        }

        moved = False
        for k, dflt in defaults.items():
            if k not in src_t:
                continue
            src_val = src_t.get(k)
            if src_val is None:
                continue
            if k not in dst_t:
                dst_t[k] = src_val
                moved = True
                continue
            # если текущее значение равно дефолту, а в источнике другое — считаем, что это миграция
            try:
                cur = dst_t.get(k)
                if isinstance(dflt, (int, float)):
                    if float(cur) == float(dflt) and float(src_val) != float(dflt):
                        dst_t[k] = src_val
                        moved = True
                else:
                    if str(cur) == str(dflt) and str(src_val) != str(dflt):
                        dst_t[k] = src_val
                        moved = True
            except Exception:
                pass

        if moved:
            result.setdefault("migrated", {})["trading_profile"] = True
            result["did"] = True
    except Exception:
        pass

    # 1) untradeable_symbols.json
    dst_un = os.path.join(data_dir, "untradeable_symbols.json")
    src_un = os.path.join(src, "untradeable_symbols.json")
    try:
        dst_data = _read_json(dst_un, {})
        src_data = _read_json(src_un, {})
        if isinstance(src_data, dict) and src_data:
            if (not isinstance(dst_data, dict)) or (not dst_data):
                if _write_json(dst_un, src_data):
                    result["migrated"]["untradeable"] = True
                    result["did"] = True
            else:
                # merge dict: keep newest per key
                merged = dict(dst_data)
                for k, v in src_data.items():
                    if k not in merged:
                        merged[k] = v
                        continue
                    try:
                        ts0 = float((merged.get(k) or {}).get("ts", 0.0))
                        ts1 = float((v or {}).get("ts", 0.0))
                    except Exception:
                        ts0, ts1 = 0.0, 0.0
                    if ts1 > ts0:
                        merged[k] = v
                if merged != dst_data:
                    if _write_json(dst_un, merged):
                        result["migrated"]["untradeable"] = True
                        result["did"] = True
    except Exception:
        pass

    # 2) temp_bans.json
    dst_b = os.path.join(data_dir, "temp_bans.json")
    src_b = os.path.join(src, "temp_bans.json")
    try:
        dst_data = _read_json(dst_b, {})
        src_data = _read_json(src_b, {})
        if src_data:
            if not dst_data:
                if _write_json(dst_b, src_data):
                    result["migrated"]["temp_bans"] = True
                    result["did"] = True
            else:
                # Бан-лист может быть в формате {bans:[...]}
                # или просто список. Пытаемся аккуратно объединить.
                def _extract(x: Any) -> List[dict]:
                    if isinstance(x, dict) and isinstance(x.get("bans"), list):
                        return [i for i in x.get("bans") if isinstance(i, dict)]
                    if isinstance(x, list):
                        return [i for i in x if isinstance(i, dict)]
                    return []

                a = _extract(dst_data)
                b = _extract(src_data)
                merged: List[dict] = []
                seen = set()
                for r in a + b:
                    sym = _norm_symbol(r.get("symbol", ""))
                    if not sym:
                        continue
                    # ключ = symbol + until (если есть)
                    try:
                        until = float(r.get("until_ts", 0.0) or 0.0)
                    except Exception:
                        until = 0.0
                    key = (sym, int(until))
                    if key in seen:
                        continue
                    seen.add(key)
                    merged.append(r)
                out_payload: Any = {"bans": merged}
                if out_payload != dst_data:
                    if _write_json(dst_b, out_payload):
                        result["migrated"]["temp_bans"] = True
                        result["did"] = True
    except Exception:
        pass

    # 3) symbol_blacklist из config.json
    try:
        src_cfg = _read_json(os.path.join(src, "config.json"), {})
        src_bl = []
        try:
            src_bl = (src_cfg.get("symbols", {}) or {}).get("symbol_blacklist", []) or []
        except Exception:
            src_bl = []

        cur_bl = []
        try:
            cur_bl = (cfg.get("symbols", {}) or {}).get("symbol_blacklist", []) or []
        except Exception:
            cur_bl = []

        merged = _union_keep_order(cur_bl, src_bl)
        if merged != [_norm_symbol(x) for x in (cur_bl or []) if _norm_symbol(x)]:
            cfg.setdefault("symbols", {})
            cfg["symbols"]["symbol_blacklist"] = merged
            result["migrated"]["symbol_blacklist"] = True
            result["did"] = True
    except Exception:
        pass

    # marker
    try:
        os.makedirs(data_dir, exist_ok=True)
        with open(marker, "w", encoding="utf-8") as f:
            json.dump({"ts": time.time(), "source": src, "migrated": result["migrated"]}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    return result
