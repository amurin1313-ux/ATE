from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, Any, List, Optional

BASE_URL = "https://www.okx.com"


class OKXPublicClient:
    """Minimal OKX Public REST client.

    Важно: в Stage02 цену стараемся брать из WebSocket (см. okx/ws_public.py),
    а REST используем как резерв и для свечей.
    """

    def __init__(self, timeout_sec: float = 5.0):
        self.sess = requests.Session()
        retries = Retry(total=2, connect=2, read=2, backoff_factor=0.35,
                        status_forcelist=(429, 500, 502, 503, 504),
                        allowed_methods=("GET",))
        adapter = HTTPAdapter(max_retries=retries)
        self.sess.mount("https://", adapter)
        self.sess.mount("http://", adapter)
        self.timeout = timeout_sec

    def _get(self, path: str, params: Optional[dict] = None) -> Dict[str, Any]:
        url = BASE_URL + path
        r = self.sess.get(url, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def ticker(self, inst_id: str) -> Dict[str, Any]:
        j = self._get("/api/v5/market/ticker", {"instId": inst_id})
        return (j.get("data") or [{}])[0]

    def spot_tickers(self) -> List[Dict[str, Any]]:
        j = self._get("/api/v5/market/tickers", {"instType": "SPOT"})
        return j.get("data") or []

    def instruments_spot(self, inst_id: str | None = None) -> List[Dict[str, Any]]:
        params = {"instType": "SPOT"}
        if inst_id:
            params["instId"] = inst_id
        j = self._get("/api/v5/public/instruments", params)
        return j.get("data") or []

    def candles(self, inst_id: str, bar: str = "1m", limit: int = 200) -> List[List[str]]:
        j = self._get("/api/v5/market/candles", {"instId": inst_id, "bar": bar, "limit": str(limit)})
        return j.get("data") or []

    def trades(self, inst_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        j = self._get("/api/v5/market/trades", {"instId": inst_id, "limit": str(limit)})
        return j.get("data") or []

    def books(self, inst_id: str, sz: int = 20) -> Dict[str, Any]:
        j = self._get("/api/v5/market/books", {"instId": inst_id, "sz": str(sz)})
        return (j.get("data") or [{}])[0]
