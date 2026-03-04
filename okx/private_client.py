import base64, hmac, hashlib, json, time
from typing import Dict, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://www.okx.com"

def _iso_timestamp() -> str:
    # OKX требует ISO8601 UTC, например 2020-12-08T09:08:57.715Z
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + ".000Z"

def sign_okx(timestamp: str, method: str, request_path: str, body: str, secret: str) -> str:
    msg = f"{timestamp}{method.upper()}{request_path}{body}".encode("utf-8")
    mac = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).digest()
    return base64.b64encode(mac).decode("utf-8")

class OKXPrivateClient:
    """Минимальный OKX Private API клиент.

    Ошибка OKX code=50101: "APIKey does not match current environment" обычно означает,
    что ключ создан для ДЕМО (simulated trading) или наоборот.

    Для демо-режима OKX использует специальный HTTP-заголовок:
        x-simulated-trading: 1

    Поэтому мы поддерживаем флаг simulated_trading.
    """

    def __init__(self, api_key: str, api_secret: str, passphrase: str, *, simulated_trading: bool = False, timeout_sec: float = 6.0):
        self.api_key = api_key or ""
        self.api_secret = api_secret or ""
        self.passphrase = passphrase or ""
        self.simulated_trading = bool(simulated_trading)
        self.timeout = timeout_sec
        self.sess = requests.Session()
        # Важно: торговые методы (POST) НЕ ретраим, иначе при таймауте/502 можно
        # случайно создать несколько одинаковых ордеров. Ретраим только GET.
        retries = Retry(total=2, connect=2, read=2, backoff_factor=0.35,
                        status_forcelist=(429, 500, 502, 503, 504),
                        allowed_methods=("GET",))
        adapter = HTTPAdapter(max_retries=retries)
        self.sess.mount("https://", adapter)
        self.sess.mount("http://", adapter)

    def _headers(self, method: str, path: str, body: str) -> Dict[str, str]:
        ts = _iso_timestamp()
        sig = sign_okx(ts, method, path, body, self.api_secret)
        h = {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": sig,
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json"
        }
        if self.simulated_trading:
            # OKX demo/simulated trading flag
            h["x-simulated-trading"] = "1"
        return h

    def _request(self, method: str, path: str, payload: Optional[dict] = None) -> Dict[str, Any]:
        url = BASE_URL + path
        body = "" if payload is None else json.dumps(payload, separators=(",", ":"))
        headers = self._headers(method, path, body)
        r = self.sess.request(method=method.upper(), url=url, data=body if body else None, headers=headers, timeout=self.timeout)
        try:
            r.raise_for_status()
        except Exception:
            # Попробуем вытащить полезное сообщение OKX
            txt = ''
            try:
                txt = r.text
            except Exception:
                txt = ''
            raise Exception(f"OKX HTTP {getattr(r,'status_code', '???')}: {txt[:300]}")
        return r.json()

    def place_order_spot(self, inst_id: str, side: str, sz: str, ord_type: str = "market", px: Optional[str] = None,
                         td_mode: str = "cash", tgt_ccy: Optional[str] = None, cl_ord_id: Optional[str] = None) -> Dict[str, Any]:
        payload = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side.lower(),
            "ordType": ord_type,
            "sz": str(sz),
        }
        if cl_ord_id:
            # Client Order ID (<=32 символа). Нужен для трассировки и защиты от дублей.
            payload["clOrdId"] = str(cl_ord_id)[:32]
        if tgt_ccy:
            payload["tgtCcy"] = str(tgt_ccy)
        if px is not None and ord_type.lower() in ("limit","post_only"):
            payload["px"] = str(px)
        return self._request("POST", "/api/v5/trade/order", payload)

    def cancel_order(self, inst_id: str, ord_id: str) -> Dict[str, Any]:
        payload = {"instId": inst_id, "ordId": ord_id}
        return self._request("POST", "/api/v5/trade/cancel-order", payload)

    def balances(self) -> Dict[str, Any]:
        return self._request("GET", "/api/v5/account/balance")

    def asset_balances(self, ccy: Optional[str] = None) -> Dict[str, Any]:
        """Funding account balances.

        В демо OKX история ордеров не чистится, а активы можно «сбросить».
        Поэтому нам нужно уметь читать и Trading balance (account/balance), и Funding balance (asset/balances)
        чтобы корректно объяснять пользователю, где лежит валюта.

        Docs: OKX API v5 → Funding Account → Get Balance.
        """
        p = "/api/v5/asset/balances"
        if ccy:
            p += f"?ccy={ccy}"
        return self._request("GET", p)

    def open_orders(self, inst_id: Optional[str] = None) -> Dict[str, Any]:
        p = "/api/v5/trade/orders-pending"
        if inst_id:
            p += f"?instId={inst_id}"
        # GET with query is part of path; body empty
        return self._request("GET", p)


    def trade_fee(self, inst_type: str = "SPOT", inst_id: Optional[str] = None) -> Dict[str, Any]:
        """
        GET /api/v5/account/trade-fee

        Используется для получения taker/maker комиссии прямо с OKX,
        чтобы не "угадывать" комиссии локально.

        Docs: OKX API v5 → Account → trade-fee. 
        """
        p = f"/api/v5/account/trade-fee?instType={inst_type}"
        if inst_id:
            p += f"&instId={inst_id}"
        return self._request("GET", p)

    def order_details(self, inst_id: str, ord_id: str) -> Dict[str, Any]:
        """
        GET /api/v5/trade/order?instId=...&ordId=...
        """
        p = f"/api/v5/trade/order?instId={inst_id}&ordId={ord_id}"
        return self._request("GET", p)

    def fills(self, inst_id: Optional[str] = None, ord_id: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        """
        GET /api/v5/trade/fills

        Можно фильтровать по instId и ordId.
        Docs: 
        """
        p = f"/api/v5/trade/fills?limit={int(limit)}"
        if inst_id:
            p += f"&instId={inst_id}"
        if ord_id:
            p += f"&ordId={ord_id}"
        return self._request("GET", p)

    def fills_history(self, inst_type: str = "SPOT", inst_id: Optional[str] = None, ord_id: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        """
        GET /api/v5/trade/fills-history

        Иногда /trade/fills на demo/ограниченных ключах возвращает code!=0.
        В таком случае этот эндпоинт часто работает стабильнее.

        Docs: OKX API v5 → Trade → fills-history.
        """
        p = f"/api/v5/trade/fills-history?instType={inst_type}&limit={int(limit)}"
        if inst_id:
            p += f"&instId={inst_id}"
        if ord_id:
            p += f"&ordId={ord_id}"
        return self._request("GET", p)

    def orders_history(
        self,
        inst_type: str = "SPOT",
        inst_id: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """Trade order history.

        OKX API v5: GET /api/v5/trade/orders-history
        Обычно возвращает историю за последние 7 дней.

        В демо OKX историю ордеров нельзя очистить, поэтому мы используем этот метод
        для синхронизации сделок, которые были открыты/закрыты ВРУЧНУЮ вне приложения.
        """
        p = f"/api/v5/trade/orders-history?instType={inst_type}&limit={int(limit)}"
        if inst_id:
            p += f"&instId={inst_id}"
        if state:
            p += f"&state={state}"
        return self._request("GET", p)


    # ---- Availability / validation helpers ----
    def max_size(self, inst_id: str, td_mode: str = "cash") -> Dict[str, Any]:
        """GET /api/v5/account/max-size

        Используем как *проверку доступности* инструмента для аккаунта/окружения.
        В demo/simulated trading и при local compliance restrictions этот эндпоинт
        обычно возвращает code!=0 / sCode!=0.

        Параметры минимальные: instId + tdMode.
        """
        p = f"/api/v5/account/max-size?instId={inst_id}&tdMode={td_mode}"
        return self._request("GET", p)

    def max_avail_size(
        self,
        inst_id: str,
        td_mode: str = "cash",
        ccy: str = "USDT",
        px: str = "1",
    ) -> Dict[str, Any]:
        """GET /api/v5/account/max-avail-size

        Этот эндпоинт иногда отражает ограничения окружения (local compliance / not tradable)
        точнее, чем /account/max-size.

        Для SPOT нам достаточно задать instId, tdMode и базовые параметры ccy/px.
        px можно ставить "1" — это проверка доступности, а не реальный расчёт размера.
        """
        inst_id = str(inst_id)
        td_mode = str(td_mode)
        ccy = str(ccy)
        px = str(px)
        p = f"/api/v5/account/max-avail-size?instId={inst_id}&tdMode={td_mode}&ccy={ccy}&px={px}"
        return self._request("GET", p)

