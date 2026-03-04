from __future__ import annotations

import time
import threading
from typing import Any, Dict, Optional, Set


def request_smooth_stop_impl(self: Any, *, minutes: Optional[int] = 15, max_time: bool = False) -> Dict[str, Any]:
    """Плавный STOP.
    
    Требования (Stage06/FIX24):
    - перестаём менять список мониторинга (Auto-TOP)
    - запрещаем новые BUY
    - распродаём монеты, купленные в этой сессии
    - после полного выхода делаем stop()
    
    minutes: лимит времени до принудительного закрытия (None/<=0 не принимаем)
    max_time: если True — ждём продаж только по решению стратегии, без тайм-аута
    """
    try:
        if self.stop_event.is_set():
            return {"ok": False, "error": "engine is stopped"}
    except Exception:
        pass
    
    # уже запущено
    try:
        if bool(self.shared_state.get("smooth_stop", False)):
            return {"ok": True, "msg": "already active"}
    except Exception:
        pass
    
    now = time.time()
    deadline_ts = 0.0
    if not bool(max_time):
        try:
            m = int(minutes or 0)
        except Exception:
            m = 15
        if m <= 0:
            m = 15
        deadline_ts = now + float(m) * 60.0
    
    self.shared_state["smooth_stop"] = True
    self.shared_state["smooth_stop_deadline_ts"] = float(deadline_ts)
    self.shared_state["smooth_stop_max_time"] = bool(max_time)
    
    try:
        self.ui_queue.put({"type": "smooth_stop_started", "msg": "smooth stop requested", "deadline_ts": deadline_ts, "max_time": bool(max_time)})
    except Exception:
        pass
    
    # запускаем отдельный поток ликвидации
    if getattr(self, "_smooth_stop_thread", None) and self._smooth_stop_thread.is_alive():
        return {"ok": True, "msg": "thread already running"}
    
    def run():
        completed = False
        try:
            session_start = float(getattr(self, "_run_started_at", 0.0) or 0.0)
            # DUST threshold: берем максимум из настроек и min_order_usd (иначе "хвосты" не продать на OKX).
            try:
                dust_th_eff = float(self.shared_state.get("dust_usd_threshold", 1.0) or 1.0)
            except Exception:
                dust_th_eff = 1.0
            try:
                min_order = float((self.config.get("trading", {}) or {}).get("min_order_usd", 0.0) or 0.0)
                if min_order > dust_th_eff:
                    dust_th_eff = float(min_order)
            except Exception:
                pass
            # Если пользователь включил max_time=True, мы всё равно делаем fallback-форс через N секунд,
            # чтобы плавный стоп гарантированно завершался.
            max_time_fallback_sec = 30 * 60
            smooth_stop_started_ts = time.time()

            # целевой набор символов для плавного стопа — все реально открытые позиции,
            # а не только сделки "текущей сессии". Иначе при перезапуске/импорте позиции остаются,
            # а smooth-stop видит пустой target и останавливает движок, НЕ продав позиции.
            def open_target_symbols() -> Set[str]:
                syms: Set[str] = set()
                try:
                    prot_ccy = set([str(x).upper() for x in (getattr(self, "_protect_ccy", set()) or set())])
                except Exception:
                    prot_ccy = set()

                # 1) позиции из портфеля (источник истины для фактического qty)
                try:
                    for p in (self.portfolio.positions or {}).values():
                        try:
                            sym = str(getattr(p, "symbol", "") or "").strip()
                            q = float(getattr(p, "qty", 0.0) or 0.0)
                            if not sym or q <= 0:
                                continue
                            # защита базовой валюты (например BTC)
                            try:
                                base = sym.split("-")[0].upper()
                                if base in prot_ccy:
                                    continue
                            except Exception:
                                pass

                            # референс цена
                            px_ref = 0.0
                            try:
                                lp = (self.shared_state.get("last_prices") or {}).get(sym) or {}
                                px_ref = float(lp.get("bid") or lp.get("last") or lp.get("price") or 0.0)
                            except Exception:
                                px_ref = 0.0
                            if px_ref <= 0:
                                try:
                                    t = self.public.ticker(sym)
                                    px_ref = float((t or {}).get("last") or (t or {}).get("lastPx") or 0.0)
                                except Exception:
                                    px_ref = 0.0

                            # DUST не блокирует завершение плавного стопа
                            try:
                                if self.portfolio.is_dust_qty(qty=float(q), px=float(px_ref or 0.0), threshold_usd=float(dust_th_eff)):
                                    continue
                            except Exception:
                                pass

                            syms.add(sym)
                        except Exception:
                            continue
                except Exception:
                    pass

                # 2) страховка: open_trades (если positions ещё не обновились)
                try:
                    ot = getattr(self.portfolio, "open_trades", {}) or {}
                    if isinstance(ot, dict):
                        it = ot.values()
                    else:
                        it = ot
                    for tr in it:
                        try:
                            sym = str(getattr(tr, "symbol", "") or "").strip()
                            if not sym:
                                continue
                            pos = self.portfolio.position(sym)
                            q = float(getattr(pos, "qty", 0.0) or 0.0)
                            if q <= 0:
                                continue
                            try:
                                base = sym.split("-")[0].upper()
                                if base in prot_ccy:
                                    continue
                            except Exception:
                                pass
                            px_ref = 0.0
                            try:
                                lp = (self.shared_state.get("last_prices") or {}).get(sym) or {}
                                px_ref = float(lp.get("bid") or lp.get("last") or lp.get("price") or 0.0)
                            except Exception:
                                px_ref = 0.0
                            if px_ref <= 0:
                                try:
                                    t = self.public.ticker(sym)
                                    px_ref = float((t or {}).get("last") or (t or {}).get("lastPx") or 0.0)
                                except Exception:
                                    px_ref = 0.0
                            try:
                                if self.portfolio.is_dust_qty(qty=float(q), px=float(px_ref or 0.0), threshold_usd=float(dust_th_eff)):
                                    continue
                            except Exception:
                                pass
                            syms.add(sym)
                        except Exception:
                            continue
                except Exception:
                    pass

                return syms
            target = open_target_symbols()
            if not target:
                # нечего продавать — просто стоп
                try:
                    self.ui_queue.put({"type": "smooth_stop_done", "msg": "no positions"})
                except Exception:
                    pass
                completed = True
                self.stop()
                return
    
            while not self.stop_event.is_set():
                target = open_target_symbols()
                if not target:
                    completed = True
                    break
    
                dts = float(self.shared_state.get("smooth_stop_deadline_ts", 0.0) or 0.0)
                mx = bool(self.shared_state.get("smooth_stop_max_time", False))
                now2 = time.time()
    
                # если таймер истёк — форсируем продажу всех оставшихся
                # + fallback: даже при max_time=True форсим через max_time_fallback_sec
                if ((not mx) and dts and now2 >= dts) or (mx and (now2 - smooth_stop_started_ts) >= max_time_fallback_sec):
                    for sym in list(target):
                        try:
                            last = 0.0
                            try:
                                lp = (self.shared_state.get("last_prices") or {}).get(sym) or {}
                                last = float(lp.get("last") or lp.get("price") or 0.0)
                            except Exception:
                                last = 0.0
                            if last <= 0:
                                try:
                                    t = self.public.ticker(sym)
                                    last = float((t or {}).get("last") or 0.0)
                                except Exception:
                                    last = 0.0
                            self.manual_trade(symbol=sym, side="sell", last_price=last, source="smooth_stop_force", force=True)
                        except Exception:
                            continue
    
                # прогресс в UI (не спамим, раз в 2 сек)
                try:
                    if int(now2) % 2 == 0:
                        self.ui_queue.put({"type": "warn", "symbol": "ENGINE", "warn": f"Плавный стоп: осталось позиций {len(target)}"})
                except Exception:
                    pass
    
                time.sleep(1)
    
        except Exception as e:
            try:
                self.ui_queue.put({"type": "error", "symbol": "ENGINE", "error": f"smooth_stop_exception: {e}"})
            except Exception:
                pass
        finally:
            try:
                self.ui_queue.put({"type": "smooth_stop_done", "msg": "done"})
            except Exception:
                pass
            # STOP движка делаем ТОЛЬКО если мы реально закрыли позиции (completed=True) или позиций не было.
            # Если случилась ошибка — не глушим мониторинг/обновления, иначе пользователь теряет контроль.
            if completed:
                try:
                    self.stop()
                except Exception:
                    pass
            else:
                try:
                    self.ui_queue.put({"type": "error", "symbol": "ENGINE", "error": "smooth_stop_incomplete: liquidation thread exited without closing all positions (engine keeps running)."})
                except Exception:
                    pass
    
    self._smooth_stop_thread = threading.Thread(target=run, daemon=True)
    self._smooth_stop_thread.start()
    return {"ok": True, "msg": "started", "deadline_ts": deadline_ts, "max_time": bool(max_time)}
    
