from __future__ import annotations

import json
import threading
import time
from typing import Any, Dict, Optional, Set
import math


try:
    import websocket  # websocket-client
except Exception:  # pragma: no cover
    websocket = None


class OKXPublicWS:
    """Public WebSocket stream for near-real-time tickers.

    - 1 поток на всё приложение
    - хранит last price + timestamp по instId
    - умеет подписываться на множество instId

    IMPORTANT:
    При отсутствии зависимости websocket-client приложение продолжит работать
    (будет использовать REST), но цены могут запаздывать.
    """

    WS_URL = "wss://ws.okx.com:8443/ws/v5/public"

    def __init__(self):
        self._lock = threading.Lock()
        # instId -> {'last': float, 'bid': float, 'ask': float, 'ts': float}
        # OKX channel=tickers returns last, bidPx, askPx (and more) per instId.
        self._prices: Dict[str, Dict[str, Any]] = {}
        self._wanted: Set[str] = set()

        self._ws_app = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._connected = threading.Event()
        self._last_error: str = ""
        self._last_msg_ts: float = 0.0
        self._msg_count: int = 0
        self._msg_count_ts: float = time.time()
        self._msg_per_sec: float = 0.0

        # OKX публичный WS НЕ гарантирует ответ на WebSocket ping frames
        # (а websocket-client может закрывать соединение по ping/pong timeout).
        # Поэтому используем application-level ping: отправляем текст "ping" и ждём "pong".
        self._ping_thread: Optional[threading.Thread] = None
        self._ping_stop = threading.Event()
        self._last_pong_ts: float = 0.0


    def set_symbols(self, symbols: Set[str]):
        with self._lock:
            self._wanted = set(symbols)
        # if already connected, subscribe to new ones
        self._try_send_subscribe()

    def start(self):
        if websocket is None:
            return
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        try:
            if self._ws_app:
                self._ws_app.close()
        except Exception:
            pass

    def get_quote(self, inst_id: str) -> Dict[str, float]:
        """Return {'last','bid','ask','ts'} (floats). Missing keys -> 0."""
        with self._lock:
            d = self._prices.get(inst_id) or {}
            return {
                "last": float(d.get("last") or 0.0),
                "bid": float(d.get("bid") or 0.0),
                "ask": float(d.get("ask") or 0.0),
                "ts": float(d.get("ts") or 0.0),
            }

    # --- compatibility layer (older SymbolChannel expects get_last) ---
    def get_last(self, inst_id: str) -> tuple[float, float]:
        """Return (last, ts) for SymbolChannel compatibility.

        Older engine versions injected a WS helper with get_last().
        REV10 introduced OKXPublicWS with get_quote(); this adapter keeps
        the rest of the engine unchanged.
        """
        q = self.get_quote(inst_id)
        try:
            last = float(q.get("last") or 0.0)
        except Exception:
            last = 0.0
        try:
            ts = float(q.get("ts") or 0.0)
        except Exception:
            ts = 0.0
        return last, ts

    def status(self) -> Dict[str, str]:
        now = time.time()
        age = 0.0
        try:
            if self._last_msg_ts:
                age = max(0.0, now - float(self._last_msg_ts))
        except Exception:
            age = 0.0
        return {
            "connected": "1" if self._connected.is_set() else "0",
            "last_error": self._last_error[:200],
            "prices": str(len(self._prices)),
            "msg_per_sec": f"{self._msg_per_sec:.2f}",
            "last_msg_age_sec": f"{age:.1f}",
        }

    # ---------------- internal ----------------

    def _run(self):
        # reconnect loop
        while not self._stop.is_set():
            try:
                self._connected.clear()
                self._ws_app = websocket.WebSocketApp(
                    self.WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                # ВАЖНО: отключаем ping_interval/ping_timeout websocket-client,
                # иначе получаем "ping/pong timed out" даже при живом соединении.
                # Держим соединение живым собственным ping (см. _ping_loop).
                self._ws_app.run_forever(ping_interval=0, ping_timeout=None)
            except Exception as e:
                self._last_error = f"ws run_forever: {e}"
            # backoff
            for _ in range(5):
                if self._stop.is_set():
                    return
                time.sleep(1)

    def _on_open(self, _ws):
        self._connected.set()
        self._last_pong_ts = time.time()
        self._start_ping_loop()
        self._try_send_subscribe(force=True)

    def _on_close(self, _ws, _code, _msg):
        self._connected.clear()
        self._stop_ping_loop()

    def _on_error(self, _ws, err):
        msg = str(err)
        self._last_error = msg
        # Если библиотека всё равно ругается на ping/pong timeout — форсируем закрытие,
        # чтобы включился reconnect loop.
        try:
            if "ping/pong" in msg and "timed out" in msg:
                if self._ws_app:
                    self._ws_app.close()
        except Exception:
            pass

    def _on_message(self, _ws, message: str):
        # OKX application-level ping/pong
        if message == "pong":
            ts = time.time()
            self._last_pong_ts = ts
            # считаем pong как живое сообщение (для health/UI)
            try:
                self._last_msg_ts = ts
            except Exception:
                pass
            return
        try:
            j = json.loads(message)
        except Exception:
            return
        if "event" in j:
            # subscribe/unsubscribe ack
            return
        arg = j.get("arg") or {}
        ch = arg.get("channel")
        if ch not in ("tickers", "bbo-tbt"):
            return
        inst_id = arg.get("instId")
        data = (j.get("data") or [])
        if not inst_id or not data:
            return
        d0 = data[0] or {}
        ts = time.time()

        # counters for health/UI
        try:
            self._last_msg_ts = ts
            self._msg_count += 1
            if (ts - float(self._msg_count_ts)) >= 1.0:
                dt = max(1e-6, ts - float(self._msg_count_ts))
                self._msg_per_sec = float(self._msg_count) / dt
                self._msg_count = 0
                self._msg_count_ts = ts
        except Exception:
            pass

        def _f(x):
            try:
                return float(x)
            except Exception:
                return 0.0

        # tickers: last, bidPx, askPx; bbo-tbt: bidPx, askPx (last может отсутствовать)
        last = _f(d0.get("last") or d0.get("lastPx"))
        bid = _f(d0.get("bidPx") or d0.get("bid") or d0.get("bidPrice"))
        ask = _f(d0.get("askPx") or d0.get("ask") or d0.get("askPrice"))
        if last <= 0 and bid > 0 and ask > 0:
            last = (bid + ask) / 2.0
        with self._lock:
            cur = self._prices.get(inst_id) or {}
            # merge: bbo-tbt может обновить bid/ask без last
            if last > 0:
                cur["last"] = last
            if bid > 0:
                cur["bid"] = bid
            if ask > 0:
                cur["ask"] = ask
            cur["ts"] = ts
            self._prices[inst_id] = cur

    def _try_send_subscribe(self, force: bool = False):
        if websocket is None:
            return
        if not self._connected.is_set() and not force:
            return
        try:
            with self._lock:
                wanted = set(self._wanted)
            if not wanted:
                return
            args = []
            for s in sorted(wanted):
                args.append({"channel": "tickers", "instId": s})
                args.append({"channel": "bbo-tbt", "instId": s})
            # OKX WS может ограничивать количество args в одном subscribe (часто ~30).
            # Поэтому дробим подписки батчами — иначе часть символов остаётся без WS-цен.
            if self._ws_app and self._ws_app.sock and self._ws_app.sock.connected:
                max_batch = 30
                for i0 in range(0, len(args), max_batch):
                    payload = {"op": "subscribe", "args": args[i0:i0+max_batch]}
                    self._ws_app.send(json.dumps(payload))
                    # небольшой пауза, чтобы избежать flood при 30+ символах
                    time.sleep(0.05)

        except Exception as e:
            self._last_error = f"subscribe: {e}"

    # ---------------- ping loop ----------------

    def _start_ping_loop(self):
        if self._ping_thread and self._ping_thread.is_alive():
            return
        self._ping_stop.clear()
        self._ping_thread = threading.Thread(target=self._ping_loop, daemon=True)
        self._ping_thread.start()

    def _stop_ping_loop(self):
        self._ping_stop.set()

    def _ping_loop(self):
        # OKX рекомендует периодически отправлять "ping" и ожидать "pong".
        # Делаем каждые 15 секунд + watchdog: если pong не приходил 60 секунд — закрываем сокет.
        while not self._stop.is_set() and not self._ping_stop.is_set():
            time.sleep(15)
            if self._stop.is_set() or self._ping_stop.is_set():
                return
            try:
                if self._ws_app and self._ws_app.sock and self._ws_app.sock.connected:
                    self._ws_app.send("ping")
            except Exception as e:
                self._last_error = f"ping send: {e}"

            # watchdog
            try:
                now = time.time()
                if (now - float(self._last_pong_ts or 0.0)) > 60.0:
                    self._last_error = "pong timeout (app-level)"
                    if self._ws_app:
                        self._ws_app.close()
                    return
            except Exception:
                pass
