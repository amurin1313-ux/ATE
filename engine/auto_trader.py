from __future__ import annotations

import queue
import threading
import time
from collections import deque
from typing import Dict, Any, Optional

from engine.logging_utils import log_event


class AutoTrader(threading.Thread):
    """
    Поток автоторговли.

    Получает сигналы от каналов (SymbolChannel) и исполняет ордера через EngineController.manual_trade().
    Делает "плавный старт" — некоторое время после START только собирает метрики, без сделок.
    """

    def __init__(self, *, data_dir: str, controller, signal_q: "queue.Queue[dict]", shared_state: dict):
        super().__init__(daemon=True)
        self.data_dir = data_dir
        self.controller = controller
        self.q = signal_q
        self.shared = shared_state
        self._stop_event = threading.Event()
        self._buy_times = deque()  # timestamps последних BUY (успешных отправок)
        self._sell_block_until: Dict[str, float] = {}  # symbol -> ts
        self._post_buy_grace_until: Dict[str, float] = {}  # symbol -> ts


    def stop(self):
        self._stop_event.set()

    def run(self):
        log_event(self.data_dir, {"level": "INFO", "msg": "AutoTrader started"})
        while not self._stop_event.is_set():
            try:
                msg = self.q.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self._process_msg(msg)
            except Exception as e:
                log_event(self.data_dir, {"level": "ERROR", "msg": "AutoTrader handle failed", "extra": {"err": str(e), "msg": msg}})

    def _process_msg(self, msg: Dict[str, Any]):
        if not self.shared.get("auto_trade", False):
            return

        def _mark_exec_block(_symbol: str, _code: str, _detail: str = "") -> None:
            """Записать причину, почему BUY-сигнал не был исполнен.

            UI показывает намерение стратегии (зелёная строка), но исполнение может быть
            заблокировано защитами движка (pending/max_pos/PRV stale/...).
            Сохраняем последний блок, чтобы UI мог отрисовать "КУПИТЬ⛔" и причину.
            """
            try:
                if not _symbol:
                    return
                d = self.shared.setdefault('exec_block', {})
                if not isinstance(d, dict):
                    d = {}
                    self.shared['exec_block'] = d
                now_ts = float(time.time())
                d[_symbol] = {
                    "ts": now_ts,
                    "code": str(_code or "EXEC_BLOCK"),
                    "detail": str(_detail or "")[:160],
                }
                # prune old
                for k in list(d.keys()):
                    try:
                        if (now_ts - float((d.get(k) or {}).get('ts') or 0.0)) > 180.0:
                            d.pop(k, None)
                    except Exception:
                        continue
            except Exception:
                pass

        now = time.time()
        warmup_until = float(self.shared.get("warmup_until", 0.0) or 0.0)
        # warmup блокирует только BUY. SELL (особенно force_exit) должен проходить.
        if warmup_until and now < warmup_until:
            # msg ещё не распарсен — проверим после чтения action.
            pass

        if msg.get("type") != "signal":
            return

        symbol = str(msg.get("symbol") or "")
        action = str(msg.get("action") or "").upper()
        conf = float(msg.get("confidence") or 0.0)
        last = float(msg.get("last") or 0.0)
        spread_pct = float(msg.get("spread_pct") or 0.0)
        lag_sec = float(msg.get("lag_sec") or 9999.0)
        force_exit = bool(msg.get("force_exit") or False)

        if not symbol or action not in ("BUY", "SELL"):
            return

        try:
            cfg0 = (self.shared.get('cfg') or {})
            tcfg = (cfg0.get('trading') or {}) if isinstance(cfg0.get('trading'), dict) else {}
        except Exception:
            tcfg = {}
        min_hold_seconds = float(tcfg.get('min_hold_seconds', 45) or 45)

        sell_cooldown = float(tcfg.get('sell_cooldown_sec', 10) or 10)
        post_buy_grace = float(tcfg.get('post_buy_exit_grace_sec', 12) or 12)
        now_ts = float(time.time())
        if action == 'SELL':
            # 1) если недавно уже отправляли SELL по символу — ждём подтверждения fill от OKX
            if now_ts < float(self._sell_block_until.get(symbol, 0.0) or 0.0):
                return
            # 2) короткая защита сразу после BUY (комиссия/спред дают мгновенный минус, но это не повод резать)
            if now_ts < float(self._post_buy_grace_until.get(symbol, 0.0) or 0.0):
                # исключение: если это действительно жёсткий стоп (большой минус) — force_exit всё равно пройдёт
                try:
                    meta0 = msg.get('meta') or {}
                    kind = str(meta0.get('force_exit_kind') or '')
                except Exception:
                    kind = ''
                if not force_exit or kind not in ('STOP_LOSS',):
                    return
        elif action == 'BUY':
            # помечаем grace: после BUY не разрешаем SELL несколько секунд
            self._post_buy_grace_until[symbol] = now_ts + post_buy_grace


        def _ret_block(code: str, detail: str = ""):
            if action == "BUY":
                _mark_exec_block(symbol, code, detail)
            return

        if action == "BUY":
            try:
                lim = float(self.shared.get('max_daily_loss_usdt', 0.0) or 0.0)
            except Exception:
                lim = 0.0
            if lim and lim > 0:
                try:
                    pnl = float(self.shared.get('session_realized_pnl', 0.0) or 0.0)
                except Exception:
                    pnl = 0.0
                if pnl <= -abs(lim):
                    log_event(self.data_dir, {"level": "WARN", "msg": "AutoTrader daily loss limit reached: BUY blocked", "extra": {"pnl_usdt": round(pnl, 4), "limit_usdt": float(lim), "symbol": symbol}})
                    return _ret_block("DAILY_LOSS", f"pnl={pnl:.2f}<=-{abs(lim):.2f}")

        # SELL оставляем разрешённым, чтобы бот мог выходить из позиций даже при проблемах PRV.
        if action == "BUY":
            try:
                freeze_sec = float(self.shared.get('prv_buy_freeze_sec', 25.0) or 25.0)
            except Exception:
                freeze_sec = 25.0
            if freeze_sec > 0:
                try:
                    last_ok = float(getattr(self.controller, '_prv_last_ok_ts', 0.0) or 0.0)
                except Exception:
                    last_ok = 0.0
                if last_ok > 0 and (now - last_ok) >= freeze_sec:
                    # подталкиваем health-check (внутри него есть rate-limit на reconnect)
                    try:
                        if hasattr(self.controller, '_prv_health_check'):
                            self.controller._prv_health_check()
                    except Exception:
                        pass
                    log_event(self.data_dir, {
                        "level": "WARN",
                        "msg": "AutoTrader PRV stale: BUY blocked",
                        "extra": {"symbol": symbol, "stale_sec": round(now - last_ok, 2), "freeze_sec": freeze_sec}
                    })
                    return _ret_block("PRV_STALE", f"stale={now-last_ok:.1f}s freeze={freeze_sec:.1f}s")
        # Глобальный warmup при START блокирует только BUY.
        if warmup_until and now < warmup_until and action == "BUY":
            return _ret_block("WARMUP", f"until={int(warmup_until-now)}s")

        # Во время разгона запрещаем новые BUY, но НЕ блокируем SELL (сопровождение открытых позиций).
        try:
            pause_until = float(self.shared.get("symbols_change_pause_until", 0.0) or 0.0)
        except Exception:
            pause_until = 0.0
        if action == "BUY" and pause_until and now < pause_until:
            return _ret_block("SYMBOLS_WARMUP", f"pause={int(pause_until-now)}s")

        if action == "BUY":
            try:
                per = self.shared.get("symbol_warmup_until", {}) or {}
                if isinstance(per, dict):
                    su = float(per.get(symbol, 0.0) or 0.0)
                    if su and now < su:
                        return _ret_block("SYMBOL_WARMUP", f"until={int(su-now)}s")
            except Exception:
                pass


        try:
            if self.shared.get("smooth_stop", False) and action == "BUY":
                return _ret_block("SMOOTH_STOP", "")
        except Exception:
            pass

        # автотрейдер должен полностью игнорировать сигналы по нему.
        try:
            ds = self.shared.get('disabled_symbols')
            if isinstance(ds, set) and symbol in ds:
                return _ret_block("DISABLED", "")
        except Exception:
            pass

        try:
            ttl = float(self.shared.get("signal_ttl_sec", 3.0) or 3.0)
        except Exception:
            ttl = 3.0
        try:
            msg_ts = float(msg.get("ts") or 0.0)
        except Exception:
            msg_ts = 0.0
        if action == "BUY" and msg_ts > 0 and ttl > 0 and (now - msg_ts) > ttl:
            log_event(self.data_dir, {"level": "INFO", "msg": "AutoTrader skip stale signal", "extra": {"symbol": symbol, "action": action, "age_sec": round(now - msg_ts, 3), "ttl_sec": ttl}})
            return _ret_block("SIGNAL_TTL", f"age={now-msg_ts:.2f}s ttl={ttl:.2f}s")

        if action == "BUY" and not force_exit:
            try:
                gthr = float(self.shared.get("global_buy_throttle_sec", 4.0) or 4.0)
            except Exception:
                gthr = 4.0
            try:
                glast = float(self.shared.get("global_last_buy_ts", 0.0) or 0.0)
            except Exception:
                glast = 0.0
            if gthr > 0 and glast > 0 and (now - glast) < gthr:
                log_event(self.data_dir, {"level": "INFO", "msg": "AutoTrader skip global throttle", "extra": {"symbol": symbol, "action": action, "since_last_buy": round(now - glast, 3), "throttle_sec": gthr}})
                return _ret_block("GLOBAL_THROTTLE", f"gap={now-glast:.2f}<thr={gthr:.2f}")

        if action == "BUY":
            try:
                max_spread = float(self.shared.get("max_spread_buy_pct", 0.25) or 0.25)
            except Exception:
                max_spread = 0.25
            try:
                max_lag = float(self.shared.get("max_lag_buy_sec", 10.0) or 10.0)
            except Exception:
                max_lag = 10.0
            if max_lag > 0 and lag_sec > max_lag:
                log_event(self.data_dir, {"level": "INFO", "msg": "AutoTrader skip lag", "extra": {"symbol": symbol, "lag_sec": round(lag_sec, 3), "max_lag": max_lag}})
                return _ret_block("LAG", f"lag={lag_sec:.2f}>max={max_lag:.2f}")
            if max_spread > 0 and spread_pct > max_spread:
                log_event(self.data_dir, {"level": "INFO", "msg": "AutoTrader skip spread", "extra": {"symbol": symbol, "spread_pct": round(spread_pct, 4), "max_spread_pct": max_spread}})
                return _ret_block("SPREAD", f"spread={spread_pct:.4f}>max={max_spread:.4f}")
        # v3: Score-порог (rule_score). Поле msg['confidence'] используется как score для совместимости.
        # thr нужен и для логирования, даже если force_exit=True (тогда сравнение не делаем).
        if action == "BUY":
            try:
                thr = float(self.shared.get("v3_buy_score_min", 0.70) or 0.70)
            except Exception:
                thr = 0.70
        else:
            try:
                thr = float(self.shared.get("v3_sell_score_min", 0.65) or 0.65)
            except Exception:
                thr = 0.65
        if not force_exit:
            if conf < thr:
                return _ret_block("LOW_SCORE", f"score={conf:.3f}<thr={thr:.3f}")

        # SAFETY: blacklist символов (например stable-stable), настраивается в data/config.json -> symbols.symbol_blacklist
        bl = self.shared.get('symbol_blacklist', []) or []
        try:
            if symbol in set(bl):
                return _ret_block("BLACKLIST", "")
        except Exception:
            pass

        # SAFETY: не торгуем стабильными монетами против USDT (USDC/USDT и т.п.)
        try:
            base = symbol.split('-')[0].upper() if '-' in symbol else symbol[:4].upper()
            stable = {'USDC','USDT','DAI','TUSD','USDP','BUSD','FDUSD','EUR','EURT','PYUSD'}
            if base in stable:
                return _ret_block("STABLE", base)
        except Exception:
            pass


        try:
            legacy_cd = float(self.shared.get('cooldown_sec', 10.0) or 10.0)
        except Exception:
            legacy_cd = 10.0
        try:
            buy_cd = float(self.shared.get('buy_cooldown_sec', legacy_cd) or legacy_cd)
        except Exception:
            buy_cd = legacy_cd
        try:
            sell_cd = float(self.shared.get('sell_cooldown_sec', legacy_cd) or legacy_cd)
        except Exception:
            sell_cd = legacy_cd
        # SELL должен иметь приоритет и не блокироваться cooldown, иначе бот
        # отдаёт прибыль или не успевает выйти. BUY остаётся с cooldown.
        cd = buy_cd if action == 'BUY' else 0.0
        try:
            last_ts = float((self.controller.portfolio.last_signal_ts or {}).get(symbol) or 0.0)
        except Exception:
            last_ts = 0.0
        if action == 'BUY' and (not force_exit) and cd > 0 and (now - last_ts) < cd:
            return _ret_block("COOLDOWN", f"left={cd-(now-last_ts):.1f}s")

        
        if action == "BUY" and (not force_exit):
            try:
                win_sec = float(self.shared.get("buy_rate_window_sec", 600) or 600)
                max_buys = int(self.shared.get("buy_rate_max", 4) or 4)
                min_gap = float(self.shared.get("buy_min_gap_sec", 45) or 45)
            except Exception:
                win_sec, max_buys, min_gap = 600.0, 4, 45.0

            # prune old BUY timestamps
            try:
                while self._buy_times and (now - float(self._buy_times[0])) > win_sec:
                    self._buy_times.popleft()
            except Exception:
                pass

            try:
                last_buy_ts = float(self._buy_times[-1]) if self._buy_times else 0.0
            except Exception:
                last_buy_ts = 0.0

            if last_buy_ts and (now - last_buy_ts) < min_gap:
                return _ret_block("BUY_RATE_GAP", f"gap={now-last_buy_ts:.1f}<min_gap={min_gap:.1f}")

            if max_buys > 0 and len(self._buy_times) >= max_buys:
                return _ret_block("BUY_RATE_MAX", f"cnt={len(self._buy_times)}>=max={max_buys}")
        # Защита: pending ордера.
        # Раньше force_exit мог пропускать pending-check и отправлять несколько SELL подряд,
        # что приводило к распродаже лишних активов (в демо — демо-набор).
        if getattr(self.controller.portfolio, "has_pending", None) is not None:
            try:
                if self.controller.portfolio.has_pending(symbol):
                    return _ret_block("PENDING", "")
            except Exception:
                pass
        pos = self.controller.portfolio.position(symbol)
        # v3 safety: минимальное время удержания, чтобы не было BUY→SELL от шума
        if action == "SELL":
            try:
                hs = int(getattr(pos, 'holding_sec', 0) or 0)
            except Exception:
                hs = 0
            try:
                kind0 = str(msg.get('force_exit_kind') or '')
            except Exception:
                kind0 = ''
            if min_hold_seconds and hs < int(min_hold_seconds) and not (force_exit and kind0 in ("STOP_LOSS", "HARD_STOP")):
                return
        try:
            dust_th = float(self.shared.get('dust_usd_threshold', 1.0) or 1.0)
        except Exception:
            dust_th = 1.0
        if action == "BUY" and pos.qty > 0:
            try:
                if not self.controller.portfolio.is_dust_qty(qty=float(pos.qty or 0.0), px=float(last or pos.last_price or 0.0), threshold_usd=float(dust_th)):
                    return _ret_block("HAVE_POS", "")
            except Exception:
                return _ret_block("HAVE_POS", "")
        if action == "SELL":
            if pos.qty <= 0:
                return
            try:
                if self.controller.portfolio.is_dust_qty(qty=float(pos.qty or 0.0), px=float(last or pos.last_price or 0.0), threshold_usd=float(dust_th)):
                    return
            except Exception:
                return

        if action == 'BUY' and not force_exit:
            try:
                # Важно: основной конфиг называется 'trading'. В прошлых ревизиях
                # часть кода ошибочно читала 'trade' и тем самым получала дефолт
                # max_positions=1 => бот открывал только ОДНУ позицию.
                cfg_root = (self.controller.config or {})
                tcfg = (cfg_root.get('trading') or cfg_root.get('trade') or {})
                max_pos = int(float(tcfg.get('max_positions', 1) or 1))
            except Exception:
                max_pos = 1
            if max_pos > 0:
                try:
                    # считаем открытые позиции без 'пыли'
                    open_cnt = 0
                    for _s, _p in (getattr(self.controller.portfolio, 'positions', {}) or {}).items():
                        try:
                            q = float(getattr(_p, 'qty', 0.0) or 0.0)
                            px = float(getattr(_p, 'last_price', 0.0) or 0.0)
                            if q <= 0:
                                continue
                            if getattr(self.controller.portfolio, 'is_dust_qty', None) is not None:
                                if self.controller.portfolio.is_dust_qty(qty=q, px=px, threshold_usd=float(dust_th)):
                                    continue
                            open_cnt += 1
                        except Exception:
                            continue
                    pending_cnt = 0
                    try:
                        pending_cnt = len(getattr(self.controller.portfolio, 'pending_orders', {}) or {})
                    except Exception:
                        pending_cnt = 0
                    if (open_cnt + pending_cnt) >= max_pos:
                        return _ret_block("MAX_POS", f"open={open_cnt} pending={pending_cnt} max={max_pos}")
                except Exception:
                    pass

        # Исполнение
        # ВАЖНО: логируем цену, по которой стратегия приняла решение (decision_price),
        # чтобы потом сравнивать с фактической ценой исполнения ордера.
        meta_in = {}
        try:
            if isinstance(msg.get('meta'), dict):
                meta_in = dict(msg.get('meta') or {})
        except Exception:
            meta_in = {}
        res = self.controller.manual_trade(
            side=action.lower(),
            symbol=symbol,
            last_price=last,
            source="strategy",
            force=(bool(force_exit) and action=="SELL"),
            meta={
                "confidence": float(conf or 0.0),
                "decision_price": float(last or 0.0),
                "decision_ts": float(msg_ts or now),
                "reason": str(msg.get('reason') or meta_in.get('reason') or ''),
                **meta_in,
            },
        )
        if action == 'SELL' and res.get('ok'):
            try:
                self._sell_block_until[symbol] = float(time.time()) + float(sell_cooldown or 10.0)
            except Exception:
                pass
        if not res.get("ok"):
            # Если OKX запрещает торговлю парой (локальные ограничения) — баним символ, чтобы не тратить API и не забивать логи.
            try:
                resp = res.get("response") or {}
                s_code = ""
                if isinstance(resp, dict):
                    data = resp.get("data") or []
                    if data and isinstance(data, list) and isinstance(data[0], dict):
                        s_code = str(data[0].get("sCode") or "")
                if s_code == "51155":
                    try:
                        # бан минимум на сутки
                        self.dead_supervisor.ban_symbol(symbol, minutes=24*60, reason="OKX_COMPLIANCE_51155")
                    except Exception:
                        pass
            except Exception:
                pass
            # логируем причину
            log_event(self.data_dir, {"level": "WARN", "msg": "AutoTrader order rejected", "extra": {"symbol": symbol, "action": action, "conf": conf, "thr": thr, "force_exit": bool(force_exit), "decision_price": float(last or 0.0), "error": res.get("error"), "response": res.get("response")}})
        else:
            log_event(self.data_dir, {"level": "INFO", "msg": "AutoTrader order sent", "extra": {"symbol": symbol, "action": action, "conf": conf, "thr": thr, "force_exit": bool(force_exit), "decision_price": float(last or 0.0), "ord_id": str(res.get("ord_id") or ""), "status": str(res.get("status") or "")}})

        if action == "BUY" and bool(res.get("ok")):
            try:
                self.shared["global_last_buy_ts"] = float(time.time())
            except Exception:
                pass
            try:
                self._buy_times.append(float(time.time()))
            except Exception:
                pass
            try:
                grace = float(self.shared.get('post_buy_grace_sec', 12) or 12)
                self._post_buy_grace_until[str(symbol)] = float(time.time()) + max(0.0, grace)
            except Exception:
                pass
