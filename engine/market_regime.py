from __future__ import annotations

"""Market regime helper.

Цель: дать стратегии простой и устойчивый сигнал о «режиме рынка».
Мы используем BTC-USDT как прокси общего режима рынка.

Принцип:
  - В сильном минусе BTC (быстрое падение / отрицательный уклон) новые BUY блокируются
    на короткое окно (cooldown), чтобы не ловить ножи.
  - В умеренно плохом режиме BUY не блокируется, но порог BUY можно поднять.

Этот модуль НЕ зависит от внешних библиотек и пишет состояние только в shared_state.
"""

from dataclasses import dataclass
import time
from typing import Any, Dict, Optional


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(d)


@dataclass
class MarketRegime:
    # score in [-1..+1], где -1 = опасный (bear), +1 = благоприятный (bull)
    score: float = 0.0
    # если True — блокируем BUY для всех символов (кроме BTC, если разрешено)
    block_buy: bool = False
    # до какого времени блокировать BUY
    block_until: float = 0.0
    # причина (для UI/логов)
    reason: str = ""


def compute_regime_from_btc(features: Dict[str, Any], cfg: Optional[Dict[str, Any]] = None) -> MarketRegime:
    """Вычисляет режим рынка по фичам BTC.

    features ожидаются как минимум:
      - btc_ret_15 (в %)
      - btc_slope30_pct (в %)
      - btc_macd_hist
      - btc_volatility_1h_pct (в %)
    """
    cfg = cfg or {}

    btc_ret_15 = _f(features.get("btc_ret_15"), 0.0)
    btc_slope30 = _f(features.get("btc_slope30_pct"), 0.0)
    btc_macd_hist = _f(features.get("btc_macd_hist"), 0.0)
    btc_vol_1h = _f(features.get("btc_volatility_1h_pct"), 0.0)

    enabled = bool(cfg.get("market_filter_enabled", True))
    if not enabled:
        return MarketRegime(score=0.0, block_buy=False, block_until=0.0, reason="market_filter_disabled")

    # Порог «резкого падения» BTC за 15 минут (в процентах)
    block_ret15 = _f(cfg.get("market_block_btc_ret15", -0.80), -0.80)
    # Порог отрицательного уклона (в процентах)
    block_slope30 = _f(cfg.get("market_block_btc_slope30", -0.25), -0.25)
    # Макс волатильность (в % за час), при которой режим считаем опасным
    danger_vol_1h = _f(cfg.get("market_danger_vol_1h", 3.0), 3.0)
    # сколько секунд держать блокировку (в сек)
    block_seconds = float(_f(cfg.get("market_block_seconds", 180), 180))

    score = 0.0
    reason_parts = []

    # Основной негатив: быстрый минус BTC или отрицательный уклон вместе с отрицательным MACD.
    hard_bear = False
    if btc_ret_15 <= block_ret15:
        hard_bear = True
        reason_parts.append(f"btc_ret15 {btc_ret_15:.2f}%")
    if btc_slope30 <= block_slope30 and btc_macd_hist < 0:
        hard_bear = True
        reason_parts.append(f"btc_slope30 {btc_slope30:.2f}% + macd_hist<0")
    if btc_vol_1h >= danger_vol_1h and btc_ret_15 < 0:
        # высокая волатильность на падении
        hard_bear = True
        reason_parts.append(f"btc_vol1h {btc_vol_1h:.2f}%")

    # score: простая шкала
    # негатив: от -1 до 0
    score += max(-1.0, min(0.0, btc_ret_15 / 2.0))  # -2% -> -1
    score += max(-0.6, min(0.6, btc_slope30 / 1.0))  # -1% slope -> -0.6
    if btc_macd_hist < 0:
        score -= 0.15
    else:
        score += 0.10
    # нормализуем
    score = max(-1.0, min(1.0, score))

    if hard_bear:
        return MarketRegime(score=score, block_buy=True, block_until=time.time() + block_seconds, reason=" / ".join(reason_parts) or "hard_bear")

    return MarketRegime(score=score, block_buy=False, block_until=0.0, reason=" / ".join(reason_parts) or "ok")


def update_shared_market(shared_state: Dict[str, Any], *, symbol: str, features: Dict[str, Any], cfg: Optional[Dict[str, Any]] = None) -> None:
    """Обновляет shared_state['market_ctx'].

    Вызывать из SymbolChannel на каждом тике.
    Только BTC (по умолчанию BTC-USDT) записывает базовые значения режима.
    """
    if not isinstance(shared_state, dict):
        return

    cfg = cfg or {}
    ref_symbol = str(shared_state.get("market_ref_symbol") or cfg.get("market_ref_symbol") or "BTC-USDT")
    if symbol != ref_symbol:
        return

    # Берём метрики BTC из текущих фич канала
    btc_ctx = {
        "btc_symbol": ref_symbol,
        "btc_last_price": _f(features.get("last_price"), 0.0),
        "btc_ret_15": _f(features.get("ret_15"), 0.0),
        "btc_ret_5": _f(features.get("ret_5"), 0.0),
        "btc_slope30_pct": _f(features.get("slope30_pct"), 0.0),
        "btc_macd_hist": _f(features.get("macd_hist"), 0.0),
        "btc_volatility_1h_pct": _f(features.get("volatility_1h_pct"), 0.0),
        "ts": time.time(),
    }

    # вычисляем режим
    regime = compute_regime_from_btc(btc_ctx, cfg)

    # блокировку держим до block_until, даже если следующее значение «чуть улучшилось»
    prev = shared_state.get("market_ctx") or {}
    prev_until = _f(prev.get("block_until"), 0.0)
    now = time.time()
    if prev_until > now and not regime.block_buy:
        # сохраняем блокировку
        regime.block_buy = True
        regime.block_until = prev_until
        regime.reason = str(prev.get("reason") or regime.reason or "")

    shared_state["market_ctx"] = {
        **btc_ctx,
        "score": float(regime.score),
        "block_buy": bool(regime.block_buy),
        "block_until": float(regime.block_until),
        "reason": str(regime.reason or ""),
    }


def inject_market_into_features(shared_state: Dict[str, Any], features: Dict[str, Any]) -> None:
    """Добавляет market_ctx (BTC proxy) в features конкретного символа."""
    if not isinstance(shared_state, dict) or not isinstance(features, dict):
        return
    ctx = shared_state.get("market_ctx")
    if not isinstance(ctx, dict):
        return
    # Добавляем префиксные ключи в фичи (они будут логироваться в Decision Log).
    for k in (
        "btc_symbol",
        "btc_last_price",
        "btc_ret_15",
        "btc_ret_5",
        "btc_slope30_pct",
        "btc_macd_hist",
        "btc_volatility_1h_pct",
        "score",
        "block_buy",
        "block_until",
        "reason",
    ):
        if k in ctx:
            # ключи режима кладём под market_* чтобы не путать с «своими» метриками символа
            if k in ("score", "block_buy", "block_until", "reason"):
                features[f"market_{k}"] = ctx.get(k)
            else:
                features[k] = ctx.get(k)
