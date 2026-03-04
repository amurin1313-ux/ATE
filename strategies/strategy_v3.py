from __future__ import annotations

"""
StrategyV3 — режимная стратегия (1m+5m), с раздельными ОБЯЗАТЕЛЬНЫМИ гейтами и МЯГКИМ скорингом.

Зачем так:
- У тебя раньше VOLR/BUYFLOW фактически стали "жёсткими стопорами" ⇒ 9/10 и BUY нет.
- Простое снижение buy_score_min опасно, если нет разделения обязательных/необязательных условий.

Новая логика:
1) Hard-gates (обязательные, НЕЛЬЗЯ обходить скорингом):
   - Спред (spread_pct <= spread_max)
   - Net edge (ожидаемое движение > комиссия + min_net)
   - Market risk-off (если включён и активен)
   - Экстремальный RSI (RSI > rsi_extreme_block)
   - Минимальные "полы" ликвидности/потока (vol_ratio >= liq_floor, buy_ratio >= buy_floor)

2) Soft-score (мягкий, определяет качество входа):
   - Trend5 / Trend1 / Breakout / MeanReversion (выбор режима автоматически)
   - RSI в тренде НЕ блокирует (только мягко влияет)
   - VOLR/BUYFLOW влияют на score, но не стопорят (кроме safety floors)

Возврат:
- action: BUY/SELL/HOLD
- confidence: это rule_score (0..1), НЕ вероятность
- reason: BLOCK_* / BUY_* / SELL_*
- meta: buy_ok/buy_total + buy_passed/buy_failed (для прямоугольников и диагностики)
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional, List, Tuple
from collections import defaultdict, deque
import math
import statistics
import time


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _sigmoid(x: float) -> float:
    # защита от overflow
    if x > 60:
        return 1.0
    if x < -60:
        return 0.0
    return 1.0 / (1.0 + math.exp(-x))


def _median(xs: List[float], default: float) -> float:
    try:
        xs2 = [float(v) for v in xs if v is not None and not math.isnan(float(v)) and math.isfinite(float(v))]
        if not xs2:
            return default
        return statistics.median(xs2)
    except Exception:
        return default


def _hold_reason_v3(
    mode: str,
    score: float,
    buy_min: float,
    buy_failed: List[str],
    buy_ok: int,
    buy_total: int,
) -> str:
    """ЧИТАЕМАЯ причина HOLD для UI.

    HOLD не является причиной сам по себе.
    Если BUY не произошёл — показываем режим/score/порог и 1–2 главные причины отказа.
    """
    try:
        m = (mode or "").strip().upper() or "NONE"
        sc = float(score or 0.0)
        mn = float(buy_min or 0.0)
        ok = int(buy_ok or 0)
        total = int(buy_total or 0)
        failed = [str(x) for x in (buy_failed or []) if str(x).strip()]
        tail = ",".join(failed[:2]) if failed else ""
        if tail:
            return f"ENTRY_GATE {m} sc={sc:.2f}<{mn:.2f} ok={ok}/{total} fail={tail}"
        return f"ENTRY_GATE {m} sc={sc:.2f}<{mn:.2f} ok={ok}/{total}"
    except Exception:
        return "ENTRY_GATE"


@dataclass
class _DynStats:
    volr: deque
    buy: deque
    ret15: deque
    macdh: deque
    slope: deque


class StrategyV3:
    name = "StrategyV3"

    def __init__(self) -> None:
        self._stats: Dict[str, _DynStats] = {}
        self._last_decision_ts: Dict[str, float] = defaultdict(float)

    def _get_stats(self, sym: str) -> _DynStats:
        st = self._stats.get(sym)
        if st is None:
            st = _DynStats(volr=deque(maxlen=240), buy=deque(maxlen=240), ret15=deque(maxlen=240), macdh=deque(maxlen=240), slope=deque(maxlen=240))
            self._stats[sym] = st
        return st

    def decide(
        self,
        *,
        features: Dict[str, Any],
        position: Dict[str, Any],
        portfolio_state: Dict[str, Any],
        cfg: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        cfg = cfg or {}
        tcfg = (cfg.get("trading") or {}) if isinstance(cfg.get("trading"), dict) else {}
        sym = str((features or {}).get("symbol") or "").strip().upper()

        # --- история: устойчиво читаем оба ключа ---
        need_1m = int(tcfg.get("warmup_candles_1m", 120) or 120)
        need_5m = int(tcfg.get("warmup_candles_5m", 120) or 120)

        have_1m = int((features or {}).get("candles_1m_n") or (features or {}).get("candles_1m") or 0)
        have_5m = int((features or {}).get("candles_5m_n") or (features or {}).get("candles_5m") or 0)

        # 1m прогрев обязателен (иначе индикаторы мусор)
        if have_1m < need_1m:
            # Визуализация прогресса в UI ("кубики"):
            # даже на прогреве показываем, насколько близко к готовности окно данных.
            try:
                prog = max(0.0, min(1.0, float(have_1m) / float(max(1, need_1m))))
            except Exception:
                prog = 0.0
            buy_total = 10
            buy_ok = int(prog * buy_total)
            return {
                "action": "HOLD",
                "confidence": 0.0,
                "reason": "WARMUP_1M",
                "meta": {
                    "warmup": True,
                    "need_1m": need_1m, "have_1m": have_1m,
                    "need_5m": need_5m, "have_5m": have_5m,
                    "buy_ok": buy_ok, "buy_total": buy_total,
                "entry_ok": buy_ok,
                "entry_total": buy_total,
                "entry_checks": entry_checks_sc,
                    "buy_passed": ["WARMUP"], "buy_failed": ["WARMUP"],
                },
            }

        # 5m прогрев НЕ должен блокировать всё — просто отключаем режим TREND5, пока не готов.
        trend5_ready = have_5m >= need_5m

        # --- метрики ---
        px = _safe_float(features.get("last_price"), 0.0)
        rsi = _safe_float(features.get("rsi14"), 50.0)
        macd_h = _safe_float(features.get("macd_hist"), 0.0)
        atr = _safe_float(features.get("atr14_pct"), 0.0)  # в % (0.12 = 0.12%)
        spr = _safe_float(features.get("spread_pct"), 0.0)
        vol = _safe_float(features.get("volume"), 0.0)
        vol_sma = _safe_float(features.get("vol_sma20"), 0.0)
        volr = (vol / vol_sma) if vol_sma > 0 else 0.0
        buy_ratio = _safe_float(features.get("buy_ratio"), 0.5)
        slope30 = _safe_float(features.get("slope30_pct"), 0.0)
        ret5 = _safe_float(features.get("ret_5"), 0.0)
        ret15 = _safe_float(features.get("ret_15"), 0.0)
        bb_upper = _safe_float(features.get("bb_upper"), 0.0)
        ema20 = _safe_float(features.get("ema20"), _safe_float(features.get("ema12"), px))
        ema50 = _safe_float(features.get("ema50"), _safe_float(features.get("ema26"), px))

        # 5m метрики (если готовы)
        rsi5 = _safe_float(features.get("rsi14_5m"), rsi)
        macd5 = _safe_float(features.get("macd_hist_5m"), macd_h)
        ema20_5m = _safe_float(features.get("ema20_5m"), ema20)
        ema50_5m = _safe_float(features.get("ema50_5m"), ema50)
        slope5 = _safe_float(features.get("slope30_5m"), 0.0)
        ret15_5m = _safe_float(features.get("ret_15_5m"), ret15)

        # --- динамические базовые уровни (адаптивные "относительные" пороги) ---
        st = self._get_stats(sym)
        # обновляем статистику
        if math.isfinite(volr):
            st.volr.append(volr)
        if math.isfinite(buy_ratio):
            st.buy.append(buy_ratio)
        if math.isfinite(ret15):
            st.ret15.append(ret15)
        if math.isfinite(macd_h):
            st.macdh.append(macd_h)
        if math.isfinite(slope30):
            st.slope.append(slope30)

        med_volr = _median(list(st.volr), default=1.0)
        med_buy = _median(list(st.buy), default=0.5)
        med_macdh = _median(list(st.macdh), default=0.0)
        med_slope = _median(list(st.slope), default=0.0)

        # --- базовые пороги из конфига / per-symbol ---
        per = {}
        try:
            per = ((tcfg.get("per_symbol_thresholds") or {}).get(sym) or {})
        except Exception:
            per = {}

        # spread_max (в %)
        spread_max = float(per.get("spread_max", tcfg.get("spread_max", 0.06)) or 0.06)
        # min net profit (после комиссий) в долях (0.0015 = 0.15%)
        min_net = float(tcfg.get("micro_profit_take_net_pct", 0.0015) or 0.0015)
        fee_pct = float(tcfg.get("fee_pct_est", 0.0010) or 0.0010)

        # RSI
        rsi_extreme = float(tcfg.get("v3_rsi_extreme_block", 92) or 92)
        rsi_mr_max = float(tcfg.get("v3_rsi_mr_max", 58) or 58)  # для отката
        rsi_trend_max = float(tcfg.get("v3_rsi_buy_trend_max", 86) or 86)

        # safety floors (жёсткие минимумы, чтобы не покупать совсем мёртвое)
        liq_floor = float(tcfg.get("v3_liq_floor", 0.15) or 0.15)
        buy_floor = float(tcfg.get("v3_buy_ratio_floor", 0.45) or 0.45)

        # динамические относительные требования
        # если рынок "тихий", volr может быть 0.2–0.6 — нельзя требовать 1.0+
        volr_req = max(liq_floor, 0.60 * med_volr)
        buy_req = max(buy_floor, med_buy - 0.05)

        # --- Hard-gates ---
        hard_failed: List[str] = []
        hard_passed: List[str] = []

        if spr <= spread_max:
            hard_passed.append("SPREAD")
        else:
            hard_failed.append("SPREAD")

        if rsi < rsi_extreme:
            hard_passed.append("RSI_EXT")
        else:
            hard_failed.append("RSI_EXTREME")

        if volr >= volr_req:
            hard_passed.append("LIQ")
        else:
            hard_failed.append("LIQ_LOW")

        if buy_ratio >= buy_req:
            hard_passed.append("FLOW_FLOOR")
        else:
            hard_failed.append("FLOW_LOW")

        # ожидаемое движение (консервативно) ≈ ATR% * k
        tp_atr_mult = float(tcfg.get("tp_atr_mult", 1.2) or 1.2)
        exp_move = (atr / 100.0) * tp_atr_mult  # atr_pct в процентах
        exp_net = exp_move - fee_pct
        if exp_net >= min_net:
            hard_passed.append("TP_NET")
        else:
            hard_failed.append("TP_NET")

        # market risk-off (если движок выставляет флаг в features)
        market_block = bool(features.get("market_block_buy", False))
        if not market_block:
            hard_passed.append("MKT")
        else:
            hard_failed.append("MKT_RISK_OFF")

        # Anti-chase / exhaustion guard (жёсткий защитный гейт):
        # В сильных пампах стратегия может давать BUY на максимумах (RSI 80+),
        # что часто совпадает со сменой краткосрочного тренда вниз.
        # Мы НЕ блокируем тренд полностью, но требуем подтверждения потоком и импульсом.
        anti_chase = False
        try:
            rsi_hi = rsi >= float(tcfg.get("v3_rsi_chase_block", 80) or 80)
            ret15_hi = ret15 >= float(tcfg.get("v3_ret15_chase_block", 0.70) or 0.70)
            flow_need = max(buy_req + 0.03, float(tcfg.get("v3_buy_ratio_chase_min", 0.60) or 0.60))
            flow_weak = buy_ratio < flow_need
            mom_weak = (macd_h < (med_macdh + 0.02)) or (ret5 < 0.0) or (slope30 < (med_slope - 0.01))
            if rsi_hi and ret15_hi and flow_weak and mom_weak:
                anti_chase = True
        except Exception:
            anti_chase = False

        if not anti_chase:
            hard_passed.append("ANTI_CHASE")
        else:
            hard_failed.append("CHASE")

        # если есть hard-fails — вход запрещён (НО прогресс условий всё равно считаем для UI)
        has_position = bool(position and position.get("status") == "OPEN")
        hard_block = (not has_position and bool(hard_failed))
        hard_block_reason = (hard_failed[0] if hard_failed else "")

        # --- Выбор режима (автоматически) ---
        # TREND5 — только если 5m готовы
        trend5 = False
        if trend5_ready:
            trend5 = (ema20_5m > ema50_5m) and (slope5 > 0.0) and (macd5 > 0.0) and (ret15_5m > 0.0)

        breakout = (bb_upper > 0.0) and (px > bb_upper) and (ret5 > 0.0) and (volr >= volr_req)

        trend1 = (ema20 > ema50) and (slope30 > 0.0) and (macd_h > 0.0) and (ret15 > 0.0)

        mean_rev = (rsi <= rsi_mr_max) and (ret5 < 0.0)

        # приоритет выбора: trend5 > breakout > trend1 > mean_rev
        if trend5:
            mode = "TREND5"
        elif breakout:
            mode = "BREAKOUT"
        elif trend1:
            mode = "TREND1"
        elif mean_rev:
            mode = "MR"
        else:
            mode = "NONE"

        # если ни один режим не совпал — НЕ блокируем "MODE" навсегда, а даём понятный HOLD
        if not has_position and mode == "NONE":
            # всё равно считаем soft прогресс для визуала
            # (часто "желтость" будет, но без режима входа — BUY не нужен)
            return {
                "action": "HOLD",
                "confidence": 0.0,
                "reason": "NO_ENTRY_MODE",
                "meta": {"mode": "NONE", "hard_failed": hard_failed, "hard_passed": hard_passed, "buy_ok": 0, "buy_total": 10, "buy_passed": [], "buy_failed": ["MODE_NONE"]},
            }

        # --- Soft-score (качество момента) ---
        buy_passed: List[str] = []
        buy_failed: List[str] = []

        # нормализованные признаки
        # momentum relative to volatility (ATR)
        denom = max(1e-6, (atr / 100.0))
        mom = (ret15 / 100.0) / denom  # ret15 в %
        mom5 = (ret15_5m / 100.0) / denom
        flow = (buy_ratio - 0.5) * 4.0
        liq = math.log(max(1e-6, volr / max(1e-6, volr_req)))
        spr_pen = -min(3.0, spr / max(1e-6, spread_max))
        overheat = max(0.0, (rsi - rsi_trend_max) / 10.0)

        # веса режима
        if mode == "TREND5":
            x = 1.1 * mom5 + 0.8 * (macd5 * 50.0) + 0.6 * liq + 0.7 * flow + 0.4 * spr_pen - 0.4 * overheat
        elif mode == "BREAKOUT":
            x = 1.0 * mom + 0.9 * (macd_h * 80.0) + 0.7 * liq + 0.6 * flow + 0.3 * spr_pen - 0.5 * overheat
        elif mode == "TREND1":
            x = 0.9 * mom + 0.8 * (macd_h * 80.0) + 0.6 * liq + 0.6 * flow + 0.3 * spr_pen - 0.4 * overheat
        else:  # MR
            # на откате RSI должен быть ниже, и flow может быть слабее
            mr_boost = max(0.0, (rsi_mr_max - rsi) / 10.0)
            x = 0.8 * mom + 0.6 * (macd_h * 80.0) + 0.5 * liq + 0.4 * flow + 0.2 * spr_pen + 0.6 * mr_boost

        score = float(_sigmoid(x))

        # soft checks (10 факторов)
        # 1) MODE
        buy_passed.append(f"MODE_{mode}")

        # 2) MOM
        if (ret15 > 0.0) or (mode == "MR"):
            buy_passed.append("MOM")
        else:
            buy_failed.append("MOM")

        # 3) MACD
        if (macd_h > 0.0) or (mode == "MR"):
            buy_passed.append("MACD")
        else:
            buy_failed.append("MACD")

        # 4) TREND FILTER
        if (mode in ("TREND5", "TREND1", "BREAKOUT")) or mean_rev:
            buy_passed.append("TREND")
        else:
            buy_failed.append("TREND")

        # 5) RSI (мягко в тренде)
        if mode == "MR":
            if rsi <= rsi_mr_max:
                buy_passed.append("RSI_MR")
            else:
                buy_failed.append("RSI_MR")
        else:
            if rsi <= rsi_trend_max:
                buy_passed.append("RSI_TREND_OK")
            else:
                # не блокируем, но отмечаем
                buy_failed.append("RSI_TREND_HIGH")

        # 6) VOLR (мягко)
        if volr >= volr_req:
            buy_passed.append("VOLR")
        else:
            buy_failed.append("VOLR")

        # 7) BUYFLOW (мягко)
        if buy_ratio >= buy_req:
            buy_passed.append("BUYFLOW")
        else:
            buy_failed.append("BUYFLOW")

        
        # 8) SPREAD (порог)
        # ВАЖНО: это именно прогресс условий, а не "гарантия действия".
        spread_ok = (spr <= spread_max)

        # 9) TP_NET (ожидаемая чистая прибыль после комиссий)
        tp_net_ok = (exp_net >= min_net)

        # 10) MKT (рыночный режим не risk-off)
        mkt_ok = (not market_block)

        # --- Прогресс условий входа (для "кубиков" в UI) ---
        # Считаем близость к покупке по фиксированному набору проверок.
        # 4-й кубик будет загораться ТОЛЬКО в момент action=BUY (это ограничивает UI).
        # Здесь мы даём честный процент выполненных условий.
        soft_ok = {
            "MODE": ("MODE" in buy_passed),
            "TREND": ("TREND" in buy_passed),
            "RSI": ("RSI" in buy_passed),
            "MOM": ("MOM" in buy_passed),
            "VOLR": ("VOLR" in buy_passed),
            "BUYFLOW": ("BUYFLOW" in buy_passed),
        }
        hard_ok = {
            "SPREAD": bool(spread_ok),
            "RSI_EXT": bool(rsi < rsi_extreme),
            "LIQ": bool(volr >= volr_req),
            "FLOW_FLOOR": bool(buy_ratio >= buy_req),
            "TP_NET": bool(tp_net_ok),
            "MKT": bool(mkt_ok),
        }

        # score_ok добавим позже, когда вычислим buy_min_eff (он зависит от anti-idle и market adjustments)
        entry_checks = {}
        entry_checks.update(hard_ok)
        entry_checks.update(soft_ok)

        entry_ok = sum(1 for _k, _v in entry_checks.items() if _v)
        entry_total = max(1, len(entry_checks))

        # Эти поля читает UI для кубиков (entry_ok/entry_total).
        # Для совместимости также дублируем в buy_ok/buy_total.
        buy_ok = int(entry_ok)
        buy_total = int(entry_total)


        # --- BUY decision ---
        buy_min = float(tcfg.get("v3_buy_score_min", 0.72) or 0.72)
        # лёгкий "anti-idle" — если давно не было BUY сигнала, чуть смягчаем порог, но не ниже 0.66
        last_ts = float(self._last_decision_ts.get(sym, 0.0) or 0.0)
        now = time.time()
        if last_ts > 0 and (now - last_ts) > float(tcfg.get("anti_idle_after_sec", 900) or 900):
            buy_min = max(0.66, buy_min - 0.04)
        # --- Добавляем SCORE как часть прогресса входа (для UI "кубиков") ---
        try:
            entry_checks_sc = dict(entry_checks)
        except Exception:
            entry_checks_sc = {}
        entry_checks_sc["SCORE"] = bool(score >= buy_min)
        entry_ok_sc = sum(1 for _k, _v in entry_checks_sc.items() if _v)
        entry_total_sc = max(1, len(entry_checks_sc))
        # обновляем для UI
        buy_ok = int(entry_ok_sc)
        buy_total = int(entry_total_sc)


        decision = {
            "action": "HOLD",
            "confidence": score,
            "reason": (f"BLOCK_{hard_block_reason}" if hard_block else _hold_reason_v3(mode, score, buy_min, buy_failed, buy_ok, buy_total)),
            "meta": {
                "mode": mode,
                "signal": str(mode),
                "block_reason": (f"HARD:{hard_block_reason}" if hard_block else ""),
                "hard_failed": hard_failed,
                "hard_passed": hard_passed,
                "buy_ok": buy_ok,
                "buy_total": buy_total,
                "entry_ok": buy_ok,
                "entry_total": buy_total,
                "entry_checks": entry_checks_sc,
                "buy_passed": buy_passed,
                "buy_failed": buy_failed,
                "need_1m": need_1m, "have_1m": have_1m, "need_5m": need_5m, "have_5m": have_5m,
                "score_min": buy_min,
            },
        }

        if (not has_position) and (not hard_block) and (score >= buy_min):
            decision["action"] = "BUY"
            decision["reason"] = f"BUY:{mode}"
            self._last_decision_ts[sym] = now
            decision["meta"]["buy_signal"] = True

        # --- SELL decision (упрощённо и безопасно) ---
        if has_position:
            # если позиция открыта, оцениваем выход
            entry_px = _safe_float(position.get("entry_px") or position.get("entry_price") or 0.0, 0.0)
            peak_px = _safe_float(position.get("peak_price") or position.get("peak_px") or 0.0, 0.0)
            if peak_px <= 0:
                peak_px = max(entry_px, px)

            pnl_pct = 0.0
            if entry_px > 0:
                pnl_pct = (px / entry_px - 1.0) * 100.0

            dd_pct = 0.0
            if peak_px > 0:
                dd_pct = (px / peak_px - 1.0) * 100.0

            # выход по ухудшению импульса
            sell_passed: List[str] = []
            sell_failed: List[str] = []

            if dd_pct <= -float(tcfg.get("trail_dd_pct", 0.35) or 0.35):
                sell_passed.append("TRAIL_DD")
            else:
                sell_failed.append("TRAIL_DD")

            if macd_h < 0.0 and ret5 < 0.0:
                sell_passed.append("MOM_DOWN")
            else:
                sell_failed.append("MOM_DOWN")

            if rsi > float(tcfg.get("v3_rsi_sell", 74) or 74):
                sell_passed.append("RSI_SELL")
            else:
                sell_failed.append("RSI_SELL")

            sell_score = _sigmoid(0.8 * (1 if "TRAIL_DD" in sell_passed else -1) + 0.6 * (1 if "MOM_DOWN" in sell_passed else -1) + 0.4 * (1 if "RSI_SELL" in sell_passed else -1))
            sell_min = float(tcfg.get("v3_sell_score_min", 0.65) or 0.65)

            if sell_score >= sell_min:
                decision = {
                    "action": "SELL",
                    "confidence": float(sell_score),
                    "reason": "SELL_SIGNAL",
                    "meta": {
                        "mode": mode,
                "signal": str(mode),
                "block_reason": (f"HARD:{hard_block_reason}" if hard_block else ""),
                        "sell_ok": len(sell_passed),
                        "sell_total": 3,
                        "sell_passed": sell_passed,
                        "sell_failed": sell_failed,
                        "pnl_pct": pnl_pct,
                        "dd_pct": dd_pct,
                    },
                }
            else:
                decision["meta"]["sell_ok"] = len(sell_passed)
                decision["meta"]["sell_total"] = 3
                decision["meta"]["sell_passed"] = sell_passed
                decision["meta"]["sell_failed"] = sell_failed

        return decision
