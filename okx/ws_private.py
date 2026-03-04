from __future__ import annotations

import base64
import hashlib
import hmac
import json
import threading
import time
from typing import Any, Callable, Dict, Optional

try:
    import websocket  # websocket-client
except Exception:  # pragma: no cover
    websocket = None


def _ws_sign(timestamp: str, secret: str) -> str:
    """OKX WS login signature.

    OKX requires:
        sign = Base64( HMAC_SHA256( secret, timestamp + 'GET' + '/users/self/verify'))
    """
    msg = f"{timestamp}GET/users/self/verify".encode("utf-8")
    mac = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).digest()
    return base64.b64encode(mac).decode("utf-8")


class OKXPrivateWS:
    """Private WebSocket stream.

    Goal for ATE 2.0:
    - receive fills/order status/balance updates in near real time
    - dramatically reduce REST polling pressure and state desync

    This implementation is safe by default:
    - if websocket-client is missing, it stays disabled (engine uses REST)
    - any error keeps trading running (WS is an accelerator, not a hard dependency)

    NOTE: In this stage we only *collect* events and expose them via callbacks.
    Execution still uses REST for placing/canceling orders.
    """

    WS_URL_LIVE = "wss://ws.okx.com:8443/ws/v5/private"
    # OKX demo environment uses a different websocket host.
    WS_URL_DEMO = "wss://wspap.okx.com:8443/ws/v5/private"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str,
        *,
        simulated_trading: bool = False,
        on_event: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        self.api_key = api_key or ""
        self.api_secret = api_secret or ""
        self.passphrase = passphrase or ""
        self.simulated_trading = bool(simulated_trading)
        self.on_event = on_event

        self._ws_app = None
        self._thread: Optional[threading.Thread] = None
        self._watchdog_thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._connected = threading.Event()
        self._authed = threading.Event()
        self._last_error: str = ""
        self._lock = threading.Lock()

        # perf/status counters (operator-visible)
        self._last_msg_ts: float = 0.0
        self._msg_count: int = 0
        self._rate_last_ts: float = time.time()
        self._rate_last_count: int = 0
        self._msg_per_sec: float = 0.0

        # 24/7 stability: если PRV "залип" (есть connected, но долго нет сообщений),
        # принудительно перезапускаем соединение.
        # Это безопасно: торговля продолжается через REST, WS — ускоритель.
        self._watchdog_stale_sec: float = 60.0
        self._watchdog_check_every_sec: float = 10.0
        self._last_connect_attempt_ts: float = 0.0

        # subscriptions we want to keep alive
        self._subs = [
            {"channel": "orders", "instType": "SPOT"},
            {"channel": "fills", "instType": "SPOT"},
            {"channel": "account"},
        ]

        # pick correct endpoint for live vs demo keys
        self._ws_url = self.WS_URL_DEMO if self.simulated_trading else self.WS_URL_LIVE

    def start(self):
        if websocket is None:
            return
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

        # watchdog запускаем один раз вместе с основным потоком
        if self._watchdog_thread is None or (not self._watchdog_thread.is_alive()):
            self._watchdog_thread = threading.Thread(target=self._watchdog, daemon=True)
            self._watchdog_thread.start()

    def stop(self):
        self._stop.set()
        try:
            # websocket-client may raise if close() is called before sock is created.
            # Guard heavily: WS is an accelerator and must never break trading.
            if self._ws_app is not None:
                try:
                    sock = getattr(self._ws_app, 'sock', None)
                    if sock is not None and getattr(sock, 'connected', False):
                        self._ws_app.close()
                except Exception:
                    # Fall back: attempt close but swallow any errors
                    try:
                        self._ws_app.close()
                    except Exception:
                        pass
        except Exception:
            pass

    def set_watchdog(self, *, stale_sec: float = 60.0, check_every_sec: float = 10.0) -> None:
        try:
            self._watchdog_stale_sec = float(stale_sec or 60.0)
        except Exception:
            self._watchdog_stale_sec = 60.0
        try:
            self._watchdog_check_every_sec = float(check_every_sec or 10.0)
        except Exception:
            self._watchdog_check_every_sec = 10.0

    def status(self) -> Dict[str, str]:
        try:
            age = max(0.0, time.time() - float(self._last_msg_ts or 0.0)) if float(self._last_msg_ts or 0.0) > 0 else 0.0
        except Exception:
            age = 0.0
        return {
            "connected": "1" if self._connected.is_set() else "0",
            "authed": "1" if self._authed.is_set() else "0",
            "last_error": self._last_error[:200],
            "msg_per_sec": f"{float(self._msg_per_sec or 0.0):.2f}",
            "last_msg_age_sec": f"{float(age or 0.0):.1f}",
        }

    # ---------------- internal ----------------

    def _run(self):
        backoff = 2
        while not self._stop.is_set():
            try:
                self._connected.clear()
                self._authed.clear()
                self._last_connect_attempt_ts = time.time()
                self._ws_app = websocket.WebSocketApp(
                    self._ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                # NOTE: OKX expects ping; websocket-client can manage it
                self._ws_app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                self._last_error = f"ws run_forever: {e}"

            # exponential backoff with cap (24/7 safe)
            try:
                b = int(backoff)
            except Exception:
                b = 5
            b = max(2, min(60, b))
            for _ in range(b):
                if self._stop.is_set():
                    return
                time.sleep(1)
            backoff = min(60, backoff * 2)

    def _watchdog(self):
        """Watchdog: если соединение "залипло" (нет сообщений слишком долго) — перезапускаем."""
        while not self._stop.is_set():
            try:
                if self._connected.is_set():
                    # если connected, но давно нет сообщений — форсим reconnect
                    age = 0.0
                    try:
                        if float(self._last_msg_ts or 0.0) > 0:
                            age = max(0.0, time.time() - float(self._last_msg_ts or 0.0))
                    except Exception:
                        age = 0.0
                    if age and float(self._watchdog_stale_sec or 0.0) > 0 and age >= float(self._watchdog_stale_sec):
                        try:
                            self._last_error = f"watchdog_stale:{age:.1f}s"
                        except Exception:
                            pass
                        try:
                            if self._ws_app is not None:
                                self._ws_app.close()
                        except Exception:
                            pass
                        try:
                            self._connected.clear()
                            self._authed.clear()
                        except Exception:
                            pass
            except Exception:
                pass

            # sleep in chunks
            try:
                step = float(self._watchdog_check_every_sec or 10.0)
            except Exception:
                step = 10.0
            step = max(2.0, min(30.0, step))
            for _ in range(int(step)):
                if self._stop.is_set():
                    break
                time.sleep(1)

    def _on_open(self, _ws):
        self._connected.set()
        try:
            self._send_login()
        except Exception as e:
            self._last_error = f"login_send: {e}"

    def _on_close(self, _ws, _code, _msg):
        self._connected.clear()
        self._authed.clear()
        try:
            if _code is not None:
                self._last_error = f"closed:{_code}:{_msg or ''}"[:200]
        except Exception:
            pass

    def _on_error(self, _ws, err):
        msg = str(err)
        # websocket-client sometimes reports a noisy internal AttributeError when
        # the transport closes before sock is fully initialized.
        if "object has no attribute 'sock'" in msg:
            msg = "transport_closed_before_socket"
        # Частая ошибка на Windows: WinError 10054 (удалённый хост разорвал соединение)
        # В этом случае принудительно закрываем сокет, чтобы цикл _run быстрее перешёл
        # к переподключению.
        try:
            if "10054" in msg or "Connection reset by peer" in msg:
                try:
                    if self._ws_app is not None:
                        self._ws_app.close()
                except Exception:
                    pass
        except Exception:
            pass
        # Ping/pong timeout: force close to trigger reconnect loop
        try:
            if "ping/pong timed out" in msg or "pong" in msg and "timed out" in msg:
                try:
                    if self._ws_app is not None:
                        self._ws_app.close()
                except Exception:
                    pass
                # make status reflect outage immediately
                try:
                    self._connected.clear()
                    self._authed.clear()
                except Exception:
                    pass
        except Exception:
            pass
        self._last_error = msg

    def _emit(self, payload: Dict[str, Any]):
        cb = self.on_event
        if not cb:
            return
        try:
            cb(payload)
        except Exception:
            # never break WS thread by callback errors
            return

    def _send_login(self):
        if not self._ws_app or not self._ws_app.sock or not self._ws_app.sock.connected:
            return
        # OKX uses seconds as string for WS timestamp
        ts = str(int(time.time()))
        sign = _ws_sign(ts, self.api_secret)
        args = {
            "apiKey": self.api_key,
            "passphrase": self.passphrase,
            "timestamp": ts,
            "sign": sign,
        }
        payload = {"op": "login", "args": [args]}
        self._ws_app.send(json.dumps(payload))

    def _send_subscribe(self):
        if not self._ws_app or not self._ws_app.sock or not self._ws_app.sock.connected:
            return
        payload = {"op": "subscribe", "args": list(self._subs)}
        self._ws_app.send(json.dumps(payload))

    def _on_message(self, _ws, message: str):
        # status counters (do this before json parsing to count raw traffic)
        try:
            now = time.time()
            self._last_msg_ts = now
            self._msg_count += 1
            # rate every ~1s
            if (now - float(self._rate_last_ts)) >= 1.0:
                dt = max(1e-6, now - float(self._rate_last_ts))
                dc = int(self._msg_count) - int(self._rate_last_count)
                self._msg_per_sec = float(dc) / float(dt)
                self._rate_last_ts = now
                self._rate_last_count = int(self._msg_count)
        except Exception:
            pass
        try:
            j = json.loads(message)
        except Exception:
            return

        # Events: login / subscribe ack
        ev = j.get("event")
        if ev:
            if ev == "login":
                # OKX returns code="0" on success
                try:
                    if str(j.get("code")) == "0":
                        self._authed.set()
                        self._send_subscribe()
                    else:
                        self._last_error = f"login_failed:{j.get('code')}:{j.get('msg')}"
                except Exception as e:
                    self._last_error = f"login_parse:{e}"
                return
            # OKX may send event=notice "Please reconnect".
            if ev == "notice":
                try:
                    self._last_error = f"notice:{str(j.get('msg') or '')[:80]}"
                except Exception:
                    self._last_error = "notice"
                # force reconnect
                try:
                    if self._ws_app is not None:
                        self._ws_app.close()
                except Exception:
                    pass
                return
            return

        # Data messages
        arg = j.get("arg") or {}
        ch = arg.get("channel")
        data = j.get("data") or []
        if not ch or not data:
            return

        payload = {
            "ts": time.time(),
            "channel": ch,
            "arg": arg,
            "data": data,
        }
        self._emit(payload)
