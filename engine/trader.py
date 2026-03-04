from __future__ import annotations
import os, json, time
from typing import Dict, Any, Optional, List, Tuple

from engine.logging_utils import log_event, MSK_TZ
from okx.private_client import OKXPrivateClient


def _now_date():
    import datetime
    # Единый стандарт времени: Москва (MSK)
    return datetime.datetime.now(MSK_TZ).date().isoformat()


def _okx_is_success(resp: dict) -> tuple[bool, str]:
    """OKX может вернуть HTTP 200, но code != 0 или sCode != 0."""
    if not isinstance(resp, dict):
        return False, "OKX response is not a dict"
    code = str(resp.get("code", ""))
    msg = str(resp.get("msg", "") or resp.get("message", "") or "")
    if code != "0":
        return False, f"OKX code={code} {msg}".strip()
    data = resp.get("data")
    if isinstance(data, list) and data:
        row = data[0] or {}
        s_code = row.get("sCode")
        s_msg = str(row.get("sMsg") or "")
        if s_code is not None and str(s_code) != "0":
            return False, f"OKX sCode={s_code} {s_msg}".strip()
    return True, ""


def _fee_to_usd(*, fee: float, fee_ccy: str, px: float, last_px: float) -> float:
    """OKX fee может быть в USDT или в base-ccy (BTC/ETH). Переводим в USDT."""
    try:
        f = abs(float(fee or 0.0))
    except Exception:
        f = 0.0
    c = str(fee_ccy or "").upper()
    if f <= 0:
        return 0.0
    if c in ("USDT", "USD"):
        return f
    # если комиссия в базовой валюте — конвертируем по цене сделки/last
    ref = float(last_px or 0.0) or float(px or 0.0)
    if ref <= 0:
        return 0.0
    return f * ref


def _split_inst(inst_id: str) -> Tuple[str, str]:
    """BTC-USDT -> (BTC, USDT). Возвращает (base, quote)."""
    s = str(inst_id or '')
    if '-' in s:
        a, b = s.split('-', 1)
        return a.upper().strip(), b.upper().strip()
    return s.upper().strip(), ''


class Trader:
    def __init__(self, data_dir: str, private_client: Optional[OKXPrivateClient]):
        self.data_dir = data_dir
        self.private = private_client

    def _dryrun_path(self):
        return os.path.join(self.data_dir, "orders_dryrun", f"orders_{_now_date()}.jsonl")

    def place_order(self, *, dry_run: bool, inst_id: str, side: str, sz: str, ord_type: str = "market",
                    px: Optional[str] = None, tgt_ccy: Optional[str] = None) -> Dict[str, Any]:
        # Client Order ID для трассировки и защиты от случайных дублей.
        # OKX ограничивает clOrdId до 32 символов.
        cl_ord_id = None
        try:
            inst_compact = (str(inst_id or '').upper().replace('-', '') or 'NA')
            s1 = (str(side or '').upper()[:1] or 'X')
            ts_ms = int(time.time() * 1000)
            cl_ord_id = f"ATE6P{ts_ms}{s1}{inst_compact[:6]}"[:32]
        except Exception:
            cl_ord_id = None
        # Dry-run удалён: ордера отправляются только при наличии OKX private клиента.
        if self.private is None:
            return {"ok": False, "dry_run": False, "error": "NO_PRIVATE_CLIENT"}

        try:
            j = self.private.place_order_spot(inst_id=inst_id, side=side, sz=sz, ord_type=ord_type, px=px, tgt_ccy=tgt_ccy, cl_ord_id=cl_ord_id)
            ok, err = _okx_is_success(j)
            if not ok:
                log_event(self.data_dir, {"level": "ERROR", "msg": "place_order rejected",
                                          "extra": {"instId": inst_id, "side": side, "err": err, "resp": j}})
                return {"ok": False, "dry_run": False, "error": err, "response": j}
            return {"ok": True, "dry_run": False, "response": j}
        except Exception as e:
            log_event(self.data_dir, {"level": "ERROR", "msg": "place_order failed", "extra": {"instId": inst_id, "err": str(e)}})
            return {"ok": False, "error": str(e)}

    def get_trade_fee_rate(self, *, inst_id: Optional[str] = None) -> float:
        """Возвращает taker fee rate (абсолютное значение), например 0.001."""
        if self.private is None:
            return 0.001
        try:
            j = self.private.trade_fee(inst_type="SPOT", inst_id=inst_id)
            data = (j or {}).get("data") or []
            if not data:
                return 0.001
            taker = data[0].get("taker") or data[0].get("takerFeeRate") or data[0].get("takerFee") or ""
            r = float(taker)
            return abs(r)
        except Exception:
            return 0.001

    def fetch_recent_fills(self, *, limit: int = 100) -> Dict[str, Any]:
        """Скан последних fills (для синхронизации ручных сделок и страховки)."""
        if self.private is None:
            return {"ok": False, "error": "no private client"}

        try:
            j = self.private.fills(inst_id=None, ord_id=None, limit=int(limit))
            ok, err = _okx_is_success(j)
            if not ok:
                # Пытаемся через /trade/fills-history.
                try:
                    j2 = self.private.fills_history(inst_type="SPOT", inst_id=None, ord_id=None, limit=int(limit))
                    ok2, err2 = _okx_is_success(j2)
                    if not ok2:
                        return {"ok": False, "error": f"fills: {err}; fills_history: {err2}", "response": {"fills": j, "fills_history": j2}}
                    rows = (j2 or {}).get("data") or []
                    return {"ok": True, "rows": rows, "via": "fills_history"}
                except Exception as e2:
                    return {"ok": False, "error": f"fills: {err}; fills_history_exc: {e2}", "response": j}

            rows = (j or {}).get("data") or []
            return {"ok": True, "rows": rows, "via": "fills"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def fetch_fills_history_for_symbol(self, *, inst_id: str, limit: int = 100) -> Dict[str, Any]:
        """Возвращает fills-history для конкретного символа.

        REV5: используется для восстановления себестоимости (cost basis), когда:
        - trade_ledger был очищен/повреждён,
        - позиция была открыта/закрыта вручную,
        - /trade/fills по ordId временно пуст.

        Мы используем именно fills-history, потому что на demo/ограниченных ключах
        обычный /trade/fills может быть недоступен.
        """
        if self.private is None:
            return {"ok": False, "error": "no private client"}
        try:
            j = self.private.fills_history(inst_type="SPOT", inst_id=str(inst_id), ord_id=None, limit=int(limit))
            ok, err = _okx_is_success(j)
            if not ok:
                return {"ok": False, "error": err, "response": j}
            rows = (j or {}).get("data") or []
            return {"ok": True, "rows": rows}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _fallback_from_order_details(self, *, inst_id: str, ord_id: str, last_px: float, side: Optional[str] = None) -> Tuple[float, float, float, float, float, str]:
        """Если /fills пустой — берём данные из /order_details.

        Возвращает:
            filled_qty_gross (base), notional_quote, fee_quote_amt, fee_base_amt, avg_px, fee_ccy
        """
        if self.private is None:
            return 0.0, 0.0, 0.0, 0.0, 0.0, ""
        try:
            od = self.private.order_details(inst_id=inst_id, ord_id=ord_id)
            d = (od or {}).get("data") or []
            if not (isinstance(d, list) and d):
                return 0.0, 0.0, 0.0, 0.0, 0.0, ""
            r = d[0] or {}
            base_ccy, quote_ccy = _split_inst(inst_id)
            side_l = str(side or '').lower().strip()

            filled_qty_gross = float(r.get("accFillSz") or r.get("fillSz") or 0.0)
            avg_px = float(r.get("avgPx") or r.get("fillPx") or 0.0)

            # не всегда есть notional, поэтому считаем
            notional = 0.0
            for k in ("accFillNotional", "fillNotional", "fillNotionalUsd", "accFillNotionalUsd"):
                if k in r and r.get(k) not in (None, ""):
                    try:
                        notional = float(r.get(k) or 0.0)
                        break
                    except Exception:
                        notional = 0.0
            if (not notional) and filled_qty_gross > 0 and avg_px > 0:
                notional = filled_qty_gross * avg_px

            # комиссия
            fee = float(r.get("fee") or r.get("fillFee") or 0.0)
            fee_ccy = str(r.get("feeCcy") or r.get("fillFeeCcy") or "").upper().strip()
            fee_quote_amt = 0.0
            fee_base_amt = 0.0
            if fee and fee_ccy:
                f_abs = abs(float(fee))
                if quote_ccy and fee_ccy == quote_ccy:
                    fee_quote_amt = f_abs
                elif base_ccy and fee_ccy == base_ccy:
                    fee_base_amt = f_abs
                else:
                    # неизвестная валюта — конвертируем в quote
                    fee_quote_amt = _fee_to_usd(fee=fee, fee_ccy=fee_ccy, px=avg_px, last_px=last_px)

            # avg_px, если не дали
            if avg_px <= 0 and filled_qty_gross > 0:
                avg_px = abs(float(notional)) / filled_qty_gross

            return float(filled_qty_gross), abs(float(notional)), float(fee_quote_amt), float(fee_base_amt), float(avg_px), str(fee_ccy or '')
        except Exception:
            return 0.0, 0.0, 0.0, 0.0, 0.0, ""

    def fetch_fills_for_order(self, *, inst_id: str, ord_id: str, last_px: float, side: Optional[str] = None) -> Dict[str, Any]:
        """
        Получаем фактические комиссии/объем по ордеру через /api/v5/trade/fills.
        если /fills пустой — используем fallback через /api/v5/trade/order.
        """
        if self.private is None:
            return {"ok": False, "error": "no private client"}

        rows = []
        via = "fills"
        try:
            j = self.private.fills(inst_id=inst_id, ord_id=ord_id, limit=100)
            ok, err = _okx_is_success(j)
            if not ok:
                via = "fills_history"
                try:
                    j2 = self.private.fills_history(inst_type="SPOT", inst_id=inst_id, ord_id=ord_id, limit=100)
                    ok2, err2 = _okx_is_success(j2)
                    if ok2:
                        rows = (j2 or {}).get("data") or []
                    else:
                        rows = []
                except Exception:
                    rows = []
            else:
                rows = (j or {}).get("data") or []
        except Exception as e:
            return {"ok": False, "error": str(e)}

        base_ccy, quote_ccy = _split_inst(inst_id)
        side_l = str(side or '').lower().strip()

        # Важно:
        #  - один и тот же fill может прийти из WS и REST;
        #  - внутри одного ордера может быть много fills в ОДНУ миллисекунду.
        # Поэтому предпочитаем per-fill id (tradeId/fillId/billId), а если его нет —
        # используем нормализованный timestamp (ms) + px + sz.
        def _uid(r: dict) -> str:
            """Стабильный uid для одного fill (REST)."""
            tid = r.get('tradeId') or r.get('fillId') or r.get('billId')
            if tid:
                side_s0 = str(r.get('side') or '').lower().strip()
                return f"{r.get('ordId','')}|{str(tid).strip()}|{side_s0}"

            t_raw = r.get("fillTime") or r.get("ts") or r.get("uTime") or r.get("cTime")
            try:
                t = float(t_raw) if t_raw is not None else 0.0
                if t > 0 and t < 1e12:
                    t = t * 1000.0
                t = str(int(round(t)))
            except Exception:
                t = "0"
            try:
                px = float(r.get("fillPx") or r.get("px") or 0.0)
            except Exception:
                px = 0.0
            try:
                sz = float(r.get("fillSz") or r.get("sz") or 0.0)
            except Exception:
                sz = 0.0
            px_s = f"{px:.12f}".rstrip('0').rstrip('.')
            sz_s = f"{sz:.12f}".rstrip('0').rstrip('.')
            side_s = str(r.get('side') or '').lower().strip()
            return f"{r.get('ordId','')}|{t}|{px_s}|{sz_s}|{side_s}"

        seen_uids = set()
        uniq_rows = []
        for r in (rows or []):
            try:
                u = _uid(r or {})
                if not u or u in seen_uids:
                    continue
                seen_uids.add(u)
                uniq_rows.append(r)
            except Exception:
                continue
        rows = uniq_rows

        # Доп. источник правды — order_details (особенно для BUY market с tgtCcy=quote_ccy).
        od0 = {}
        try:
            od = self.private.order_details(inst_id=inst_id, ord_id=ord_id)
            dd = (od or {}).get('data') or []
            if isinstance(dd, list) and dd:
                od0 = dd[0] or {}
        except Exception:
            od0 = {}
        od_state = str((od0 or {}).get('state') or '').lower().strip()
        od_tgt = str((od0 or {}).get('tgtCcy') or '').lower().strip()
        try:
            od_req_sz = float((od0 or {}).get('sz') or 0.0)
        except Exception:
            od_req_sz = 0.0
        try:
            od_acc_fill_sz = float((od0 or {}).get('accFillSz') or 0.0)
        except Exception:
            od_acc_fill_sz = 0.0
        od_acc_notional = 0.0
        try:
            for k in ("accFillNotional", "accFillNotionalUsd", "fillNotional", "fillNotionalUsd"):
                if (od0 or {}).get(k) not in (None, ""):
                    od_acc_notional = abs(float((od0 or {}).get(k) or 0.0))
                    if od_acc_notional > 0:
                        break
        except Exception:
            od_acc_notional = 0.0

        filled_qty_gross = 0.0
        notional_quote = 0.0

        fee_quote_amt = 0.0  # в quote (USDT)
        fee_base_amt = 0.0   # в base (BTC/ETH/...)
        fee_ccy_last = ""

        for r in rows:
            try:
                px = float(r.get("fillPx") or r.get("px") or 0.0)
                sz = float(r.get("fillSz") or r.get("sz") or 0.0)
                fee = float(r.get("fee") or 0.0)
                ccy = str(r.get("feeCcy") or "")
                # notional: если OKX дал готовое поле — используем его (точнее, чем px*sz)
                n = 0.0
                for k in ("fillNotional", "fillNotionalUsd", "notional", "notionalUsd", "fillNotionalUSDT"):
                    if k in r and r.get(k) not in (None, ""):
                        try:
                            n = float(r.get(k) or 0.0)
                            break
                        except Exception:
                            n = 0.0
                if (not n) and px and sz:
                    n = px * sz

                if sz:
                    filled_qty_gross += sz
                if n:
                    notional_quote += abs(n)

                if fee:
                    c = str(ccy or '').upper().strip()
                    f_abs = abs(float(fee))
                    if c and quote_ccy and c == quote_ccy:
                        fee_quote_amt += f_abs
                        fee_ccy_last = c
                    elif c and base_ccy and c == base_ccy:
                        fee_base_amt += f_abs
                        fee_ccy_last = c
                    else:
                        # неизвестная валюта комиссии — конвертируем в USDT как раньше
                        fee_quote_amt += _fee_to_usd(fee=fee, fee_ccy=c, px=px, last_px=last_px)
                        fee_ccy_last = c or fee_ccy_last
            except Exception:
                continue

        # Особенно важно для BUY market (tgtCcy=quote_ccy): там "sz" — это запрошенная сумма в quote.
        need_fb = (filled_qty_gross <= 0 and notional_quote <= 0)
        if side_l == 'buy' and od_tgt == 'quote_ccy' and od_req_sz > 0:
            # fills могут быть неполными/задублированными → notional выходит 0.71 или 200 вместо 100
            if (notional_quote <= 0) or (notional_quote < od_req_sz * 0.7) or (notional_quote > od_req_sz * 1.3):
                need_fb = True
        if need_fb:
            # IMPORTANT: если мы решили, что /fills подозрителен (пустой/неполный/задублированный),
            # то fallback из order_details должен ПЕРЕЗАТЕРЕТЬ totals, а не "max()".
            # Иначе баг "реально купил на 100$, а UI показывает 200$" останется навсегда.
            fb_qty, fb_notional, fb_fee_quote, fb_fee_base, fb_avg_px, fb_fee_ccy = self._fallback_from_order_details(
                inst_id=inst_id, ord_id=ord_id, last_px=last_px, side=side
            )
            if fb_qty > 0:
                filled_qty_gross = fb_qty
            if fb_notional > 0:
                notional_quote = fb_notional
            if fb_fee_quote > 0:
                fee_quote_amt = fb_fee_quote
            if fb_fee_base > 0:
                fee_base_amt = fb_fee_base
            if fb_fee_ccy:
                fee_ccy_last = fb_fee_ccy

        # не должна внезапно удваиваться. Даже если order ещё не "filled",
        # notional не может быть > запрошенной суммы с большим запасом.
        if side_l == 'buy' and od_tgt == 'quote_ccy' and od_req_sz > 0:
            try:
                if float(notional_quote or 0.0) > float(od_req_sz) * 1.05:
                    # в приоритете accFillNotional (если есть), иначе жёстко режем до запроса
                    notional_quote = float(od_acc_notional or od_req_sz)
            except Exception:
                pass

        # Если ордер уже filled, то фактическая сумма исполнения в quote почти всегда равна запрошенной.
        if side_l == 'buy' and od_tgt == 'quote_ccy' and od_req_sz > 0:
            if od_state == 'filled':
                # В приоритете: accFillNotional (если OKX дал), иначе запрошенный sz.
                ref = float(od_acc_notional or od_req_sz)
                if ref > 0:
                    # Если notional очень далёк — считаем что /fills был неполный или дублированный.
                    if (notional_quote <= 0) or (notional_quote < ref * 0.7) or (notional_quote > ref * 1.3):
                        notional_quote = ref
            # Если OKX дал accFillSz и fills меньше — подтянем.
            if od_acc_fill_sz > 0 and filled_qty_gross > 0 and od_acc_fill_sz > filled_qty_gross * 1.05:
                filled_qty_gross = od_acc_fill_sz

        avg_px = (notional_quote / filled_qty_gross) if filled_qty_gross > 0 else 0.0

        # BUY: комиссия в base уменьшает полученное количество.
        # SELL: комиссия в base увеличивает списание base из баланса.
        filled_qty_net = float(filled_qty_gross)
        if fee_base_amt > 0 and base_ccy:
            if side_l == 'sell':
                filled_qty_net = float(filled_qty_gross) + float(fee_base_amt)
            else:
                filled_qty_net = max(0.0, float(filled_qty_gross) - float(fee_base_amt))

        fee_mode = ''
        if fee_quote_amt > 0:
            fee_mode = 'quote'
        elif fee_base_amt > 0:
            fee_mode = 'base'

        # notional_quote (в quote-валюте, обычно USDT) = сумма исполнения БЕЗ учёта комиссии.
        # - BUY: комиссия может быть списана в BASE (уменьшает qty), но не уменьшает quote-notional.
        # - SELL: комиссия часто списывается в QUOTE и уменьшает сумму "получено".
        notional_usd_gross = float(notional_quote or 0.0)
        # notional_usd_ui: для UI можно показать "после комиссии" ТОЛЬКО если комиссия в quote и это SELL.
        # Для BUY с комиссией в base UI не должен уменьшать сумму исполнения.
        notional_usd_ui = float(notional_usd_gross)
        if side_l == 'sell' and fee_quote_amt > 0:
            notional_usd_ui = max(0.0, notional_usd_gross - float(fee_quote_amt))
# USD-эквивалент комиссии для UI/логов
        fee_usd_total = float(fee_quote_amt or 0.0)
        if fee_base_amt > 0:
            fee_usd_total += float(fee_base_amt) * (float(avg_px) or float(last_px) or 0.0)

        # Важно: возвращаем сырые rows (fills) чтобы контроллер мог:
        # 1) пометить их как импортированные (анти-дубликаты)
        # 2) не дать сканеру recent_fills повторно импортировать те же fills,
        #    из-за чего сумма покупки могла удваиваться (200$ -> 400$).
        return {
            "ok": True,
            "filled_qty": filled_qty_net,
            "filled_qty_gross": filled_qty_gross,
            "avg_px": avg_px,
            "notional_usd": notional_usd_gross,
            "notional_usd_ui": notional_usd_ui,
            "notional_usd_gross": notional_usd_gross,
            "fee_usd": fee_usd_total,
            "fee_mode": fee_mode,
            "fee_ccy": (fee_ccy_last or quote_ccy or base_ccy),
            "fee_quote_amt": float(fee_quote_amt or 0.0),
            "fee_base_amt": float(fee_base_amt or 0.0),
            "base_ccy": base_ccy,
            "quote_ccy": quote_ccy,
            "fills_count": len(rows),
            "rows": rows,
            "via": via,
        }

    def fetch_recent_orders(self, *, inst_id: str, limit: int = 50, state: str = "filled") -> Dict[str, Any]:
        """Получить историю ордеров OKX по инструменту.

        Используется как дополнительный источник синхронизации, когда пользователь
        совершает сделки вне приложения (в мобильном OKX), а /trade/fills может
        возвращать неполный список.
        """
        if self.private is None:
            return {"ok": False, "error": "no private client", "data": []}
        try:
            j = self.private.orders_history(inst_type="SPOT", inst_id=inst_id, state=state, limit=int(limit))
            ok, err = _okx_is_success(j)
            if not ok:
                return {"ok": False, "error": err or "orders_history failed", "data": []}
            return {"ok": True, "data": (j or {}).get("data") or []}
        except Exception as e:
            return {"ok": False, "error": str(e), "data": []}
