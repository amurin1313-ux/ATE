from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Dict, Any, Optional, List, Tuple
import time
import os
import json
import uuid

from engine.logging_utils import log_trade_event


@dataclass
class Trade:
    """Одна сделка (BUY -> SELL) для UI.

    - у каждой сделки есть уникальный trade_id
    - повторная покупка после закрытия создаёт НОВУЮ строку истории
    - открытая позиция по символу может быть только одна (режим 1 символ = 1 позиция)
    """

    trade_id: str
    symbol: str

    buy_ts: float = 0.0
    buy_usd: float = 0.0
    buy_qty: float = 0.0
    buy_qty_gross: float = 0.0  # до комиссии в BASE (fillSz)
    buy_px: float = 0.0
    buy_fee_usd: float = 0.0
    buy_fee_ccy: str = ""
    buy_fee_amt: float = 0.0
    buy_fee_mode: str = ""  # "quote"|"base"|""  (влияет на расчёт сумм)
    buy_ord_id: str = ""

    buy_score: float = 0.0  # entry score (v3)

    sell_ts: float = 0.0
    sell_usd: float = 0.0
    sell_qty: float = 0.0
    sell_qty_gross: float = 0.0  # до комиссии в BASE (fillSz)
    sell_px: float = 0.0
    sell_fee_usd: float = 0.0
    sell_fee_ccy: str = ""
    sell_fee_amt: float = 0.0
    sell_fee_mode: str = ""
    sell_ord_id: str = ""

    # Накопители фактически проданного (пока сделка ещё открыта).
    sold_ts_last: float = 0.0
    sold_usd: float = 0.0
    sold_qty: float = 0.0
    sold_qty_gross: float = 0.0
    sold_fee_usd: float = 0.0
    sold_fee_ccy: str = ""
    sold_fee_amt: float = 0.0
    sold_fee_mode: str = ""
    # несколько ордеров продаж (TP1/TP2/финал)
    sell_ord_ids: List[str] = field(default_factory=list)
    # лёгкий список "ног" продаж для UI/разбора
    sell_legs: List[Dict[str, Any]] = field(default_factory=list)

    # флаги — чтобы TP1/TP2 не повторялись
    tp1_done: bool = False
    tp2_done: bool = False

    sell_reason: str = ""  # reason for exit (from decision/pending.meta)

    source: str = "bot"  # manual/strategy

    # max favorable excursion (по *net* pnl в USD) за время жизни сделки
    max_net_pnl_usd: float = 0.0
    max_net_pnl_pct: float = 0.0
    max_net_pnl_ts: float = 0.0
    # max adverse excursion (по *net* pnl в USD)
    min_net_pnl_usd: float = 0.0
    min_net_pnl_pct: float = 0.0
    min_net_pnl_ts: float = 0.0
    # первый момент, когда pnl стал приемлемым
    first_accept_ts: float = 0.0
    first_accept_pnl_usd: float = 0.0
    first_accept_pnl_pct: float = 0.0
    first_accept_kind: str = ""  # POSITIVE|NEGATIVE

    @property
    def is_open(self) -> bool:
        return self.buy_ts > 0 and self.sell_ts <= 0

    @property
    def holding_sec(self) -> int:
        if not self.buy_ts:
            return 0
        end = self.sell_ts if self.sell_ts else time.time()
        return int(end - self.buy_ts)

    def est_pnl_now(self, *, last_px: float, fee_rate: float) -> Tuple[float, float]:
        """Оценка *мгновенного* PnL для открытой сделки (как можно ближе к OKX).

        Важно про комиссии OKX (SPOT):
        - notional (buy_usd/sell_usd) всегда в QUOTE (обычно USDT) и **без комиссии**.
        - комиссия может списываться в QUOTE или в BASE.
          * BUY fee в BASE → вы получаете меньше BASE (это уже отражено в buy_qty).
          * SELL fee в BASE → вы отдаёте чуть больше BASE, чем исполнилось (sell_qty учитывает),
            а notional в QUOTE не уменьшается → это надо учитывать отдельно.

        Для *оценки сейчас* у нас ещё нет фактической SELL-комиссии, поэтому:
        - учитываем фактическую BUY-комиссию в QUOTE (если она была),
        - прогнозируем SELL-комиссию как taker fee в QUOTE: value * fee_rate.

        Это используется стратегией для micro-profit/timeout и должно быть консервативным.
        """
        if (not self.is_open) or float(self.buy_qty or 0.0) <= 0.0 or float(last_px or 0.0) <= 0.0:
            return 0.0, 0.0

        fee_rate = float(fee_rate or 0.0)
        if fee_rate < 0:
            fee_rate = 0.0

        quote_ccy = str((self.symbol or '').split('-')[-1] if '-' in (self.symbol or '') else 'USDT').upper()

        def _fee_mode_is_quote(mode: str, fee_ccy: str) -> bool:
            m = str(mode or '').lower().strip()
            if m == 'quote':
                return True
            if m:
                return False
            # fallback by currency
            return str(fee_ccy or '').upper().strip() == quote_ccy

        # фактическая BUY fee в quote (если была)
        buy_fee_quote = 0.0
        if _fee_mode_is_quote(self.buy_fee_mode, self.buy_fee_ccy):
            fee_amt = abs(float(self.buy_fee_amt or 0.0))
            fee_usd = abs(float(self.buy_fee_usd or 0.0))
            buy_fee_quote = fee_amt if fee_amt > 0 else fee_usd

        spent_total = float(self.buy_usd or 0.0) + float(buy_fee_quote or 0.0)

        sold_qty = float(getattr(self, 'sold_qty', 0.0) or 0.0)
        sold_usd = float(getattr(self, 'sold_usd', 0.0) or 0.0)
        sold_fee_mode = str(getattr(self, 'sold_fee_mode', '') or '')
        sold_fee_ccy = str(getattr(self, 'sold_fee_ccy', '') or '')
        sold_fee_amt = float(getattr(self, 'sold_fee_amt', 0.0) or 0.0)
        sold_fee_usd = float(getattr(self, 'sold_fee_usd', 0.0) or 0.0)

        # реализованная часть
        realized_got = 0.0
        if sold_usd > 0:
            realized_got = float(sold_usd)
            # fee in QUOTE
            if _fee_mode_is_quote(sold_fee_mode, sold_fee_ccy):
                fee_amt = abs(float(sold_fee_amt or 0.0))
                fee_usd = abs(float(sold_fee_usd or 0.0))
                realized_got -= fee_amt if fee_amt > 0 else fee_usd
            else:
                # fee in BASE: оцениваем через среднюю цену продажи (если есть)
                try:
                    realized_got -= abs(float(sold_fee_amt or 0.0)) * float(getattr(self, 'sell_px', 0.0) or last_px or 0.0)
                except Exception:
                    pass

        # оставшаяся позиция (qty уже NET, BUY fee в BASE учтён)
        buy_qty = float(self.buy_qty or 0.0)
        remain_qty = max(0.0, buy_qty - sold_qty)
        remain_value = remain_qty * float(last_px or 0.0)
        est_sell_fee_quote = remain_value * fee_rate
        remain_got = remain_value - est_sell_fee_quote

        got_total = realized_got + remain_got
        pnl = got_total - spent_total
        pct = pnl / max(1e-9, spent_total)
        return float(pnl), float(pct)

    def realized_pnl(self) -> Tuple[float, float]:
        """Итоговый PnL по закрытой сделке (максимально близко к OKX).

        Ключевой фикс REV42:
        - SELL fee в BASE *не отражается* в sell_usd (notional), поэтому её нужно вычесть как
          fee_base_amt * sell_px (оценка в QUOTE на момент продажи).

        BUY fee в BASE НЕ вычитаем отдельно, потому что buy_qty уже NET и это влияние проявляется
        через меньший объём продажи.
        """
        if float(self.sell_ts or 0.0) <= 0.0:
            return 0.0, 0.0

        quote_ccy = str((self.symbol or '').split('-')[-1] if '-' in (self.symbol or '') else 'USDT').upper()

        def _fee_mode_is_quote(mode: str, fee_ccy: str) -> bool:
            m = str(mode or '').lower().strip()
            if m == 'quote':
                return True
            if m:
                return False
            return str(fee_ccy or '').upper().strip() == quote_ccy

        def _fee_mode_is_base(mode: str, fee_ccy: str) -> bool:
            m = str(mode or '').lower().strip()
            if m == 'base':
                return True
            if m:
                return False
            base_ccy = str((self.symbol or '').split('-')[0] if '-' in (self.symbol or '') else '').upper()
            return base_ccy and str(fee_ccy or '').upper().strip() == base_ccy

        # BUY cost
        buy_fee_quote = 0.0
        if _fee_mode_is_quote(self.buy_fee_mode, self.buy_fee_ccy):
            fee_amt = abs(float(self.buy_fee_amt or 0.0))
            fee_usd = abs(float(self.buy_fee_usd or 0.0))
            buy_fee_quote = fee_amt if fee_amt > 0 else fee_usd
        spent_total = float(self.buy_usd or 0.0) + float(buy_fee_quote or 0.0)

        # SELL proceeds
        got_total = float(self.sell_usd or 0.0)
        # fee in QUOTE
        if _fee_mode_is_quote(self.sell_fee_mode, self.sell_fee_ccy):
            fee_amt = abs(float(self.sell_fee_amt or 0.0))
            fee_usd = abs(float(self.sell_fee_usd or 0.0))
            got_total -= float(fee_amt if fee_amt > 0 else fee_usd)
        # fee in BASE (IMPORTANT)
        elif _fee_mode_is_base(self.sell_fee_mode, self.sell_fee_ccy):
            try:
                got_total -= abs(float(self.sell_fee_amt or 0.0)) * float(self.sell_px or 0.0)
            except Exception:
                pass

        pnl = got_total - spent_total
        pct = pnl / max(1e-9, spent_total)
        return float(pnl), float(pct)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "buy_ts": self.buy_ts,
            "buy_usd": self.buy_usd,
            "buy_qty": self.buy_qty,
            "buy_qty_gross": self.buy_qty_gross,
            "buy_px": self.buy_px,
            "buy_fee_usd": self.buy_fee_usd,
            "buy_fee_ccy": self.buy_fee_ccy,
            "buy_fee_amt": self.buy_fee_amt,
            "buy_fee_mode": self.buy_fee_mode,
            "buy_ord_id": self.buy_ord_id,
            "buy_score": float(getattr(self, "buy_score", 0.0) or 0.0),
            "buy_confidence": float(getattr(self, "buy_score", 0.0) or 0.0),
            "sell_ts": self.sell_ts,
            "sell_usd": self.sell_usd,
            "sell_qty": self.sell_qty,
            "sell_qty_gross": self.sell_qty_gross,
            "sell_px": self.sell_px,
            "sell_fee_usd": self.sell_fee_usd,
            "sell_fee_ccy": self.sell_fee_ccy,
            "sell_fee_amt": self.sell_fee_amt,
            "sell_fee_mode": self.sell_fee_mode,
            "sell_ord_id": self.sell_ord_id,

            # partial exits
            "sold_ts_last": float(getattr(self, "sold_ts_last", 0.0) or 0.0),
            "sold_usd": float(getattr(self, "sold_usd", 0.0) or 0.0),
            "sold_qty": float(getattr(self, "sold_qty", 0.0) or 0.0),
            "sold_qty_gross": float(getattr(self, "sold_qty_gross", 0.0) or 0.0),
            "sold_fee_usd": float(getattr(self, "sold_fee_usd", 0.0) or 0.0),
            "sold_fee_ccy": str(getattr(self, "sold_fee_ccy", "") or ""),
            "sold_fee_amt": float(getattr(self, "sold_fee_amt", 0.0) or 0.0),
            "sold_fee_mode": str(getattr(self, "sold_fee_mode", "") or ""),
            "sell_ord_ids": list(getattr(self, "sell_ord_ids", []) or []),
            "sell_legs": list(getattr(self, "sell_legs", []) or []),
            "tp1_done": bool(getattr(self, "tp1_done", False)),
            "tp2_done": bool(getattr(self, "tp2_done", False)),
            "source": self.source,

            # analytics
            "max_net_pnl_usd": float(getattr(self, "max_net_pnl_usd", 0.0) or 0.0),
            "max_net_pnl_pct": float(getattr(self, "max_net_pnl_pct", 0.0) or 0.0),
            "max_net_pnl_ts": float(getattr(self, "max_net_pnl_ts", 0.0) or 0.0),
            "min_net_pnl_usd": float(getattr(self, "min_net_pnl_usd", 0.0) or 0.0),
            "min_net_pnl_pct": float(getattr(self, "min_net_pnl_pct", 0.0) or 0.0),
            "min_net_pnl_ts": float(getattr(self, "min_net_pnl_ts", 0.0) or 0.0),
            "first_accept_ts": float(getattr(self, "first_accept_ts", 0.0) or 0.0),
            "first_accept_pnl_usd": float(getattr(self, "first_accept_pnl_usd", 0.0) or 0.0),
            "first_accept_pnl_pct": float(getattr(self, "first_accept_pnl_pct", 0.0) or 0.0),
            "first_accept_kind": str(getattr(self, "first_accept_kind", "") or ""),
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Trade":
        return Trade(
            trade_id=str(d.get("trade_id") or ""),
            symbol=str(d.get("symbol") or ""),
            buy_ts=float(d.get("buy_ts") or 0.0),
            buy_usd=float(d.get("buy_usd") or 0.0),
            buy_qty=float(d.get("buy_qty") or 0.0),
            buy_qty_gross=float(d.get("buy_qty_gross") or 0.0),
            buy_px=float(d.get("buy_px") or 0.0),
            buy_fee_usd=float(d.get("buy_fee_usd") or 0.0),
            buy_fee_ccy=str(d.get("buy_fee_ccy") or ""),
            buy_fee_amt=float(d.get("buy_fee_amt") or 0.0),
            buy_fee_mode=str(d.get("buy_fee_mode") or ""),
            buy_ord_id=str(d.get("buy_ord_id") or ""),
            buy_score=float((d.get("buy_score") if d.get("buy_score") is not None else d.get("buy_score")) or 0.0),
            sell_ts=float(d.get("sell_ts") or 0.0),
            sell_usd=float(d.get("sell_usd") or 0.0),
            sell_qty=float(d.get("sell_qty") or 0.0),
            sell_qty_gross=float(d.get("sell_qty_gross") or 0.0),
            sell_px=float(d.get("sell_px") or 0.0),
            sell_fee_usd=float(d.get("sell_fee_usd") or 0.0),
            sell_fee_ccy=str(d.get("sell_fee_ccy") or ""),
            sell_fee_amt=float(d.get("sell_fee_amt") or 0.0),
            sell_fee_mode=str(d.get("sell_fee_mode") or ""),
            sell_ord_id=str(d.get("sell_ord_id") or ""),

            sold_ts_last=float(d.get("sold_ts_last") or 0.0),
            sold_usd=float(d.get("sold_usd") or 0.0),
            sold_qty=float(d.get("sold_qty") or 0.0),
            sold_qty_gross=float(d.get("sold_qty_gross") or 0.0),
            sold_fee_usd=float(d.get("sold_fee_usd") or 0.0),
            sold_fee_ccy=str(d.get("sold_fee_ccy") or ""),
            sold_fee_amt=float(d.get("sold_fee_amt") or 0.0),
            sold_fee_mode=str(d.get("sold_fee_mode") or ""),
            sell_ord_ids=list(d.get("sell_ord_ids") or []),
            sell_legs=list(d.get("sell_legs") or []),
            tp1_done=bool(d.get("tp1_done") or False),
            tp2_done=bool(d.get("tp2_done") or False),
            source=str(d.get("source") or "bot"),

            max_net_pnl_usd=float(d.get("max_net_pnl_usd") or 0.0),
            max_net_pnl_pct=float(d.get("max_net_pnl_pct") or 0.0),
            max_net_pnl_ts=float(d.get("max_net_pnl_ts") or 0.0),
            min_net_pnl_usd=float(d.get("min_net_pnl_usd") or 0.0),
            min_net_pnl_pct=float(d.get("min_net_pnl_pct") or 0.0),
            min_net_pnl_ts=float(d.get("min_net_pnl_ts") or 0.0),
            first_accept_ts=float(d.get("first_accept_ts") or 0.0),
            first_accept_pnl_usd=float(d.get("first_accept_pnl_usd") or 0.0),
            first_accept_pnl_pct=float(d.get("first_accept_pnl_pct") or 0.0),
            first_accept_kind=str(d.get("first_accept_kind") or ""),
        )


    def analytics_drop_from_peak_realized(self) -> Tuple[float, float]:
        """Сколько «отдали» от пика на закрытии (в USD и в процентных пунктах)."""
        try:
            peak_usd = float(getattr(self, "max_net_pnl_usd", 0.0) or 0.0)
            peak_pct = float(getattr(self, "max_net_pnl_pct", 0.0) or 0.0)
        except Exception:
            peak_usd, peak_pct = 0.0, 0.0
        try:
            rp_usd, rp_pct = self.realized_pnl()
            rp_pct_pp = float(rp_pct) * 100.0
        except Exception:
            rp_usd, rp_pct_pp = 0.0, 0.0
        return float(peak_usd - rp_usd), float(peak_pct - rp_pct_pp)


@dataclass
class Position:
    symbol: str
    qty: float = 0.0
    avg_price: float = 0.0
    opened_ts: float = 0.0
    last_price: float = 0.0
    # локальный максимум цены с момента открытия позиции (для profit-lock)
    peak_price: float = 0.0
    # время (epoch) обновления peak_price — нужно для умного выхода при "застое" в плюсе
    peak_ts: float = 0.0
    realized_pnl: float = 0.0
    fee_paid: float = 0.0

    @property
    def holding_sec(self) -> int:
        if not self.opened_ts:
            return 0
        return int(time.time() - self.opened_ts)

    @property
    def unrealized_pnl(self) -> float:
        if self.qty <= 0 or self.avg_price <= 0:
            return 0.0
        return (self.last_price - self.avg_price) * self.qty

    @property
    def profit_pct(self) -> float:
        if self.qty <= 0 or self.avg_price <= 0:
            return 0.0
        return (self.last_price - self.avg_price) / self.avg_price


@dataclass
class Portfolio:
    """Минимальное состояние портфеля для стратегии и UI.

    - pending_orders: запрещаем двойные ордера и не рисуем сделку в UI, пока OKX не вернул fills
    - история сделок не затирается (closed_trades список)
    """

    data_dir: str

    cash_usd: float = 0.0
    equity_usd: float = 0.0
    positions: Dict[str, Position] = field(default_factory=dict)

    consecutive_losses: Dict[str, int] = field(default_factory=dict)
    last_signal_ts: Dict[str, float] = field(default_factory=dict)

    # timestamp последнего закрытия сделки по символу (для защиты от немедленного re-entry)
    last_exit_ts: Dict[str, float] = field(default_factory=dict)

    last_okx_sync_ts: float = 0.0

    assets_usd: float = 0.0
    assets_count: int = 0

    # Сделки
    open_trades: Dict[str, List[Trade]] = field(default_factory=dict)  # symbol -> [Trade, ...] (может быть несколько открытых лотов)
    closed_trades: List[Trade] = field(default_factory=list)     # история

    # pending ордера (symbol -> {ord_id, side, created_ts, ...})
    pending_orders: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    # ordId -> trade_id мапа для защиты от дублей при multi-fill и reconcile.
    ordid_trade_map: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        self._load_ledger_safe()

    # --- совместимость имён ---
    @property
    def cash_usdt(self) -> float:
        return float(self.cash_usd or 0.0)

    @cash_usdt.setter
    def cash_usdt(self, v: float) -> None:
        try:
            self.cash_usd = float(v)
        except Exception:
            pass

    def position(self, symbol: str) -> Position:
        return self.positions.setdefault(symbol, Position(symbol=symbol))

    def position_dict(self, symbol: str) -> Dict[str, Any]:
        p = self.positions.get(symbol)
        if not p or p.qty <= 0:
            return {"status": "IDLE"}
        return {
            "status": "HOLDING",
            "entry_price": p.avg_price,
            "entry_ts": int(p.opened_ts) if p.opened_ts else 0,
            "base_qty": p.qty,
            "profit_pct": p.profit_pct,
            "holding_sec": p.holding_sec,
            "fee_paid": p.fee_paid,
            "realized_pnl": p.realized_pnl,
            "peak_price": float(getattr(p, 'peak_price', 0.0) or 0.0),
            "peak_ts": float(getattr(p, 'peak_ts', 0.0) or 0.0),
        }

    def portfolio_state(self) -> Dict[str, Any]:
        open_positions = [p for p in self.positions.values() if p.qty > 0]
        positions_notional = sum((p.last_price * p.qty) for p in open_positions)
        total_eq = float(self.equity_usd or 0.0)
        cash = float(self.cash_usd or 0.0)
        if total_eq <= 0:
            total_eq = cash + positions_notional

        return {
            "total_equity": total_eq,
            "cash": cash,
            "positions_notional": positions_notional,
            "positions_count": len(open_positions),
            "consecutive_losses": self.consecutive_losses,
            "last_signal_ts": self.last_signal_ts,
            "okx_sync_ts": self.last_okx_sync_ts,
            "assets_usd": float(self.assets_usd or 0.0),
            "assets_count": int(self.assets_count or 0),
            "pending_orders": int(len(self.pending_orders)),
        }

    def on_price(self, symbol: str, last_price: float) -> None:
        p = self.positions.get(symbol)
        if p:
            px = float(last_price or 0.0)
            p.last_price = px
            # peak tracking (только если позиция открыта)
            try:
                if p.qty > 0 and px > 0:
                    if float(p.peak_price or 0.0) <= 0 or px > float(p.peak_price):
                        p.peak_price = px
                        p.peak_ts = time.time()
            except Exception:
                pass

    def update_from_okx_balance(self, *, total_equity: Optional[float], cash_usdt: Optional[float], assets_usd: float = 0.0, assets_count: int = 0) -> None:
        if total_equity is not None:
            try:
                self.equity_usd = float(total_equity)
            except Exception:
                pass
        if cash_usdt is not None:
            try:
                self.cash_usd = float(cash_usdt)
            except Exception:
                pass
        try:
            self.assets_usd = float(assets_usd or 0.0)
        except Exception:
            pass
        try:
            self.assets_count = int(assets_count or 0)
        except Exception:
            pass
        self.last_okx_sync_ts = time.time()

    def apply_local_fill(self, *, symbol: str, side: str, qty: float, price: float, fee: float = 0.0) -> None:
        """Упрощённое обновление позиции от fills."""
        if qty <= 0 or price <= 0:
            return
        p = self.position(symbol)
        if side.lower() == "buy":
            # если в позиции уже висит "пыль" (qty>0), но avg_price обнулён
            # (например после сброса активов/истории), то для корректного среднего
            # считаем, что пыль приобретена примерно по текущей цене.
            if p.qty > 0 and (p.avg_price or 0.0) <= 0:
                p.avg_price = float(price)
            new_notional = p.qty * p.avg_price + qty * price
            new_qty = p.qty + qty
            p.avg_price = (new_notional / new_qty) if new_qty else price
            p.qty = new_qty
            if not p.opened_ts:
                p.opened_ts = time.time()
            p.fee_paid += float(fee or 0.0)
            p.last_price = price
            # инициализируем peak с момента входа
            if float(getattr(p, 'peak_price', 0.0) or 0.0) <= 0:
                p.peak_price = float(price)
                p.peak_ts = time.time()
            else:
                if float(price) > float(p.peak_price):
                    p.peak_price = float(price)
                    p.peak_ts = time.time()
        elif side.lower() == "sell":
            sell_qty = min(p.qty, qty)
            if sell_qty <= 0:
                return
            pnl = (price - p.avg_price) * sell_qty
            p.realized_pnl += pnl
            p.qty -= sell_qty
            p.fee_paid += float(fee or 0.0)
            p.last_price = price
            if p.qty <= 0:
                p.qty = 0.0
                p.avg_price = 0.0
                p.opened_ts = 0.0
                p.peak_price = 0.0
                p.peak_ts = 0.0

    # ---------------- dust helpers ----------------

    @staticmethod
    def is_dust_qty(*, qty: float, px: float, threshold_usd: float) -> bool:
        """Возвращает True, если остаток можно считать "пылью" (слишком мал для продажи/ограничений).

        threshold_usd задаётся в конфиге (по умолчанию 1 USDT).
        """
        try:
            q = float(qty or 0.0)
            p = float(px or 0.0)
            th = float(threshold_usd or 0.0)
        except Exception:
            return False
        if q <= 0 or p <= 0 or th <= 0:
            return False
        return (q * p) <= th

    def is_dust_position(self, *, symbol: str, last_px: float, threshold_usd: float) -> bool:
        p = self.positions.get(str(symbol))
        if not p:
            return False
        return self.is_dust_qty(qty=float(p.qty or 0.0), px=float(last_px or p.last_price or 0.0), threshold_usd=threshold_usd)

    # ---------------- pending orders ----------------

    def has_pending(self, symbol: str) -> bool:
        return bool(self.pending_orders.get(symbol))

    def add_pending(self, *, symbol: str, order: Dict[str, Any]) -> None:
        self.pending_orders[str(symbol)] = dict(order or {})
        self._save_ledger_safe()

    def set_pending(self, symbol: str, order: Dict[str, Any]) -> None:
        self.add_pending(symbol=symbol, order=order)

    def to_ui_dict(self) -> Dict[str, Any]:
        """Безопасный снимок состояния для UI/снапшотов."""
        try:
            return {
                "portfolio": self.portfolio_state(),
                "positions": {s: self.position_dict(s) for s in (self.positions or {}).keys()},
                "open_trades": [t.to_dict() for lst in (self.open_trades or {}).values() for t in (lst or [])],
                "closed_trades": [t.to_dict() for t in (self.closed_trades or [])[:200]],
                "pending_orders": self.pending_orders or {},
            }
        except Exception:
            return {"portfolio": self.portfolio_state(), "pending_orders": self.pending_orders or {}}

    def clear_pending(self, symbol: str) -> None:
        if symbol in self.pending_orders:
            del self.pending_orders[symbol]
            self._save_ledger_safe()

    # ---------------- trade ledger ----------------

    def _ledger_path(self) -> str:
        return os.path.join(self.data_dir, "trade_ledger.json")

    def _load_ledger_safe(self) -> None:
        try:
            p = self._ledger_path()
            if not os.path.exists(p):
                return
            data = json.loads(open(p, "r", encoding="utf-8").read() or "{}")
            # closed
            closed = data.get("closed_trades") or []
            if isinstance(closed, list):
                self.closed_trades = [Trade.from_dict(x) for x in closed if isinstance(x, dict)]
            # open
            open_map = data.get("open_trades") or {}
            if isinstance(open_map, dict):
                self.open_trades = {}
                for sym, td in open_map.items():
                    s = str(sym)
                    # backward compat:
                    # - old format: {symbol: {Trade}}
                    # - new format: {symbol: [{Trade}, ...]}
                    if isinstance(td, dict):
                        self.open_trades[s] = [Trade.from_dict(td)]
                    elif isinstance(td, list):
                        lst = []
                        for it in td:
                            if isinstance(it, dict):
                                try:
                                    lst.append(Trade.from_dict(it))
                                except Exception:
                                    continue
                        if lst:
                            self.open_trades[s] = lst
            # pending
            pending = data.get("pending_orders") or {}
            if isinstance(pending, dict):
                self.pending_orders = {str(k): (v if isinstance(v, dict) else {}) for k, v in pending.items()}
        except Exception:
            # не критично
            return

    def _save_ledger_safe(self) -> None:
        try:
            os.makedirs(self.data_dir, exist_ok=True)
            payload = {
                "open_trades": {k: [t.to_dict() for t in (v or [])] for k, v in (self.open_trades or {}).items()},
                "closed_trades": [t.to_dict() for t in (self.closed_trades or [])],
                "pending_orders": self.pending_orders or {},
            }
            with open(self._ledger_path(), "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            return

    def _new_trade_id(self) -> str:
        return uuid.uuid4().hex

    def open_trade_list(self, symbol: str) -> List[Trade]:
        return list((self.open_trades or {}).get(str(symbol), []) or [])

    def open_trade_count(self, symbol: str) -> int:
        try:
            return len((self.open_trades or {}).get(str(symbol), []) or [])
        except Exception:
            return 0

    def open_trade_total_count(self) -> int:
        try:
            return int(sum(len(v or []) for v in (self.open_trades or {}).values()))
        except Exception:
            return 0

    def last_open_trade(self, symbol: str) -> Optional[Trade]:
        lst = (self.open_trades or {}).get(str(symbol), []) or []
        if not lst:
            return None
        # LIFO
        return lst[-1]

    def update_open_trade_analytics(
        self,
        *,
        symbol: str,
        trade_id: str,
        pnl_net_usd: float,
        pnl_net_pct: float,
        now_ts: float,
        accept_pos_pct: float = 0.0,
        accept_neg_pct: float = -0.35,
    ) -> None:
        """Обновляет MFE/MAE/тайминги по открытой сделке.

        pnl_net_pct ожидается в долях (например 0.0023 = +0.23%).
        accept_*_pct — в процентах (например -0.35 = -0.35%).
        """
        tr: Optional[Trade] = None
        try:
            tr = self.find_open_trade_by_id(str(trade_id))
        except Exception:
            tr = None
        if tr is None:
            try:
                tr = self.last_open_trade(str(symbol))
            except Exception:
                tr = None
        if tr is None:
            return

        try:
            pnl_usd = float(pnl_net_usd or 0.0)
        except Exception:
            pnl_usd = 0.0
        try:
            pnl_pp = float(pnl_net_pct or 0.0) * 100.0  # percent points
        except Exception:
            pnl_pp = 0.0

        # init min to first observed sample (so MAE works)
        # init max to first observed sample (so MFE works even if always negative due to fees)
        if float(getattr(tr, "min_net_pnl_ts", 0.0) or 0.0) <= 0.0:
            tr.min_net_pnl_usd = pnl_usd
            tr.min_net_pnl_pct = pnl_pp
            tr.min_net_pnl_ts = float(now_ts or 0.0)


        if float(getattr(tr, "max_net_pnl_ts", 0.0) or 0.0) <= 0.0:
            tr.max_net_pnl_usd = pnl_usd
            tr.max_net_pnl_pct = pnl_pp
            tr.max_net_pnl_ts = float(now_ts or 0.0)

        # max
        if pnl_usd >= float(getattr(tr, "max_net_pnl_usd", 0.0) or 0.0):
            tr.max_net_pnl_usd = pnl_usd
            tr.max_net_pnl_pct = pnl_pp
            tr.max_net_pnl_ts = float(now_ts or 0.0)

        # min
        if pnl_usd <= float(getattr(tr, "min_net_pnl_usd", 0.0) or 0.0):
            tr.min_net_pnl_usd = pnl_usd
            tr.min_net_pnl_pct = pnl_pp
            tr.min_net_pnl_ts = float(now_ts or 0.0)


        if float(getattr(tr, "max_net_pnl_ts", 0.0) or 0.0) <= 0.0:
            tr.max_net_pnl_usd = pnl_usd
            tr.max_net_pnl_pct = pnl_pp
            tr.max_net_pnl_ts = float(now_ts or 0.0)

        # first acceptable
        if float(getattr(tr, "first_accept_ts", 0.0) or 0.0) <= 0.0 and float(getattr(tr, "buy_ts", 0.0) or 0.0) > 0.0:
            pos_ok = pnl_pp >= float(accept_pos_pct or 0.0)
            neg_ok = pnl_pp <= float(accept_neg_pct or -0.35)
            if pos_ok:
                tr.first_accept_ts = float(now_ts or 0.0)
                tr.first_accept_pnl_usd = pnl_usd
                tr.first_accept_pnl_pct = pnl_pp
                tr.first_accept_kind = "POSITIVE"
            elif neg_ok:
                tr.first_accept_ts = float(now_ts or 0.0)
                tr.first_accept_pnl_usd = pnl_usd
                tr.first_accept_pnl_pct = pnl_pp
                tr.first_accept_kind = "NEGATIVE"

        # persist to ledger (best effort, cheap file)
        try:
            self._save_ledger()
        except Exception:
            pass

    def find_open_trade_by_id(self, trade_id: str) -> Optional[Trade]:
        tid = str(trade_id or "")
        if not tid:
            return None
        for lst in (self.open_trades or {}).values():
            for tr in (lst or []):
                if getattr(tr, "trade_id", "") == tid:
                    return tr
        return None

    def _remove_open_trade_by_id(self, trade_id: str) -> Optional[Trade]:
        tid = str(trade_id or "")
        if not tid:
            return None
        for sym, lst in list((self.open_trades or {}).items()):
            if not isinstance(lst, list):
                continue
            for i, tr in enumerate(list(lst)):
                if getattr(tr, "trade_id", "") == tid:
                    got = lst.pop(i)
                    if not lst:
                        try:
                            del self.open_trades[str(sym)]
                        except Exception:
                            pass
                    return got
        return None



    def on_bot_buy(self, *, symbol: str, usd_amount: float, qty: float, price: float, fee_usd: float, ts: Optional[float] = None, source: str = "manual", ord_id: str = "", trade_id: str = "") -> Trade:
        ts = float(ts or time.time())
        tr = Trade(
            trade_id=(str(trade_id) if str(trade_id or '').strip() else self._new_trade_id()),
            symbol=str(symbol),
            buy_ts=ts,
            buy_usd=float(usd_amount or 0.0),
            buy_qty=float(qty or 0.0),
            buy_qty_gross=float(qty or 0.0),
            buy_px=float(price or 0.0),
            buy_fee_usd=float(fee_usd or 0.0),
            buy_ord_id=str(ord_id or ""),
            source=str(source or "manual"),
        )
        sym = str(symbol)
        lst = (self.open_trades or {}).get(sym)
        if lst is None or not isinstance(lst, list):
            lst = []
            self.open_trades[sym] = lst
        lst.append(tr)
        self._save_ledger_safe()

        self.record_trade({
            "type": "BUY",
            "trade_id": tr.trade_id,
            "symbol": symbol,
            "ts": ts,
            "usd": float(usd_amount or 0.0),
            "qty": float(qty or 0.0),
            "px": float(price or 0.0),
            "fee_usd": float(fee_usd or 0.0),
            "ord_id": str(ord_id or ""),
            "source": str(source or "manual"),
            "buy_score": float(getattr(tr, 'buy_score', 0.0) or 0.0),
        })
        return tr

    def ensure_recovered_open_trade(self, *, symbol: str, qty: float, px_ref: float, source: str = 'recovered') -> Trade:
        """Гарантирует наличие open_trade, даже если ledger потерялся после рестарта.

        По исследованию: SELL должен быть возможен по факту позиции, а не по наличию OPEN_TRADE.
        Этот метод создаёт "восстановленную" сделку, чтобы UI и сопровождение работали.
        """
        sym = str(symbol)
        tr = self.last_open_trade(sym)
        if tr is not None and float(getattr(tr, 'buy_qty', 0.0) or 0.0) > 0:
            return tr
        ts = time.time()
        tr = Trade(
            trade_id=f"recovered_{sym}_{int(ts)}",
            symbol=sym,
            buy_ts=float(ts),
            buy_qty=float(qty or 0.0),
            buy_qty_gross=float(qty or 0.0),
            buy_px=float(px_ref or 0.0),
            buy_usd=float(qty or 0.0) * float(px_ref or 0.0),
            source=str(source or 'recovered'),
        )
        lst = (self.open_trades or {}).get(sym)
        if lst is None or not isinstance(lst, list):
            lst = []
            self.open_trades[sym] = lst
        lst.append(tr)
        self._save_ledger_safe()
        # только WARN-событие, чтобы не путать статистику BUY
        self.record_trade({
            'type': 'RECOVER_OPEN',
            'trade_id': tr.trade_id,
            'symbol': sym,
            'ts': float(ts),
            'qty': float(qty or 0.0),
            'px': float(px_ref or 0.0),
            'source': str(source or 'recovered'),
        })
        return tr

    def on_bot_sell(
            self,
            *,
            symbol: str,
            qty: float,
            price: float,
            fee_usd: float,
            fee_mode: str = "",
            fee_ccy: str = "",
            fee_amt: float = 0.0,
            usd_amount: Optional[float] = None,
            ts: Optional[float] = None,
            source: str = "manual",
            ord_id: str = "",
            trade_id: str = "",
        ) -> Optional[Trade]:
            """Закрыть ОДИН лот сделки (Trade) по symbol.

            поддержка нескольких открытых лотов по одному symbol.
            Если trade_id передан — закрываем именно его, иначе закрываем последний (LIFO).
            """
            ts = float(ts or time.time())
            sym = str(symbol)

            tr: Optional[Trade] = None
            if str(trade_id or '').strip():
                tr = self.find_open_trade_by_id(str(trade_id))
            if tr is None:
                tr = self.last_open_trade(sym)
            if tr is None:
                return None
            # только по финальному закрытию позиции (см. ingest_okx_fill).

            sell_usd = float(usd_amount) if usd_amount is not None else float(qty or 0.0) * float(price or 0.0)
            tr.sell_ts = ts
            tr.sell_qty = float(qty or 0.0)
            tr.sell_qty_gross = float(qty or 0.0)
            tr.sell_px = float(price or 0.0)
            tr.sell_usd = float(sell_usd or 0.0)
            tr.sell_fee_usd = float(fee_usd or 0.0)
            tr.sell_fee_mode = str(fee_mode or tr.sell_fee_mode or '')
            tr.sell_fee_ccy = str(fee_ccy or tr.sell_fee_ccy or '')
            tr.sell_fee_amt = float(fee_amt or tr.sell_fee_amt or 0.0)
            if ord_id and not tr.sell_ord_id:
                tr.sell_ord_id = str(ord_id)
            tr.source = str(source or tr.source or "manual")

            # attach sell reason from pending meta (if available)
            try:
                po = (self.pending_orders or {}).get(sym) or {}
                meta = (po.get("meta") or {}) if isinstance(po, dict) else {}
                rr = str(meta.get("reason") or meta.get("reason_ui") or "")
                if rr and not getattr(tr, "sell_reason", ""):
                    tr.sell_reason = rr
            except Exception:
                pass

            # закрытая сделка -> в историю
            self.closed_trades.append(tr)

            # remove from open list (в режиме 1:1 закрываем весь лот)
            try:
                self._remove_open_trade_by_id(tr.trade_id)
            except Exception:
                # fallback: remove last by symbol
                try:
                    lst = (self.open_trades or {}).get(sym) or []
                    if isinstance(lst, list) and lst:
                        lst.pop()
                        if not lst and sym in self.open_trades:
                            del self.open_trades[sym]
                except Exception:
                    pass

            self._save_ledger_safe()

            try:
                self.last_exit_ts[sym] = float(ts)
            except Exception:
                pass

            self.record_trade({
                "type": "SELL",
                "trade_id": tr.trade_id,
                "symbol": sym,
                "ts": float(ts),
                "usd": float(sell_usd or 0.0),
                "qty": float(qty or 0.0),
                "px": float(price or 0.0),
                "fee_usd": float(fee_usd or 0.0),
                "ord_id": str(ord_id or ""),
                "source": str(source or "manual"),
                "buy_score": float(getattr(tr, 'buy_score', 0.0) or 0.0),
                "holding_sec": int(getattr(tr, 'holding_sec', 0) or 0),
                "pnl_usd": float(tr.realized_pnl()[0] if hasattr(tr, 'realized_pnl') else 0.0),
                "pnl_pct": float(tr.realized_pnl()[1] if hasattr(tr, 'realized_pnl') else 0.0),
                "reason": str(getattr(tr, "sell_reason", "") or ""),
            })
            return tr

    def set_open_trade_buy_totals(
        self,
        *,
        symbol: str,
        trade_id: str = "",
        filled_qty: float,
        filled_qty_gross: float = 0.0,
        notional_usd: float,
        fee_usd: float,
        fee_mode: str = "",
        fee_ccy: str = "",
        fee_amt: float = 0.0,
        avg_px: float,
        ts: float,
        source: str = "manual",
        ord_id: str = "",
        buy_score: float = 0.0  # entry score (v3)
    ) -> Trade:
        """Обновить/создать открытую сделку по фактическим fills.

        Используется трекером pending_orders: пока ордер не подтверждён fills — сделку не рисуем.
        Когда fills появляются — создаём/обновляем открытую строку (ОДНА на symbol).

        Важно: позиция обновляется отдельно через apply_local_fill().
        """
        sym = str(symbol)

        tr: Optional[Trade] = None
        if str(trade_id or '').strip():
            tr = self.find_open_trade_by_id(str(trade_id))
        if tr is None:
            # создаём новый лот, если не найден
            tr = Trade(trade_id=(str(trade_id) if str(trade_id or '').strip() else self._new_trade_id()), symbol=sym, buy_ts=float(ts or time.time()), source=str(source or "manual"))
            lst = (self.open_trades or {}).get(sym)
            if lst is None or not isinstance(lst, list):
                lst = []
                self.open_trades[sym] = lst
            lst.append(tr)
        # buy_ts — момент первого подтверждённого fill
        if not tr.buy_ts:
            tr.buy_ts = float(ts or time.time())
        tr.buy_qty = float(filled_qty or 0.0)
        # qty_gross (до комиссии в BASE). Если не передали — считаем равным qty.
        qg = float(filled_qty_gross or 0.0) if float(filled_qty_gross or 0.0) > 0 else float(filled_qty or 0.0)
        tr.buy_qty_gross = qg
        tr.buy_usd = float(notional_usd or 0.0)
        tr.buy_fee_usd = float(fee_usd or 0.0)
        tr.buy_fee_mode = str(fee_mode or tr.buy_fee_mode or '')
        tr.buy_fee_ccy = str(fee_ccy or tr.buy_fee_ccy or '')
        tr.buy_fee_amt = float(fee_amt or tr.buy_fee_amt or 0.0)
        # avg_px — фактическая цена исполнения (OKX). Если 0 — считаем по gross qty.
        apx = float(avg_px or 0.0)
        if apx > 0:
            tr.buy_px = apx
        else:
            qg = float(getattr(tr, 'buy_qty_gross', 0.0) or 0.0)
            tr.buy_px = (float(notional_usd or 0.0) / qg) if qg > 0 else 0.0
        if ord_id and not tr.buy_ord_id:
            tr.buy_ord_id = str(ord_id)
        try:
            tr.buy_score = float(buy_score or 0.0)
        except Exception:
            tr.buy_score = float(getattr(tr, "buy_score", 0.0) or 0.0)
        tr.source = str(source or tr.source or "manual")
        self._save_ledger_safe()
        return tr

    def record_trade(self, row: Dict[str, Any]) -> None:
        try:
            log_trade_event(self.data_dir, row)
        except Exception:
            pass

    def ingest_okx_fill(self, r: Dict[str, Any], source: str = 'okx') -> None:
        """импорт одной строки fill от OKX в локальное состояние.

        Нужен для синхронизации:
        - если ордер выполнен, но /fills по ordId вернулся пустым/с задержкой;
        - если сделку сделали вручную в приложении OKX;
        - если приложение перезапустили и pending потерялся.

        Мы обновляем позицию, ledger и пишем событие в trade_history.
        """
        try:
            symbol = str(r.get('instId') or r.get('inst_id') or r.get('symbol') or '')
            if not symbol:
                return
            side = str(r.get('side') or '').lower().strip()
            if side not in ('buy', 'sell'):
                return
            # OKX: комиссия часто списывается в BASE (BUY BTC-USDT -> fee BTC).
            # Для корректного qty в портфеле:
            # - BUY: qty_net = fillSz - abs(fee) (если feeCcy==BASE)
            # - SELL: qty_out = fillSz + abs(fee) (если feeCcy==BASE)
            px = float(r.get('fillPx') or r.get('px') or 0.0)
            sz_gross = float(r.get('fillSz') or r.get('sz') or 0.0)
            if px <= 0 or sz_gross <= 0:
                return
            ord_id = str(r.get('ordId') or '')

            # Ордер OKX (ordId) может состоять из 10–20 fills.
            # Дедуп по fill_uid делается в controller._reconcile_recent_fills().
            # Здесь мы должны блокировать только:
            #   - ordId, который уже обрабатывается pending_orders трекером,
            #   - ordId, который уже финализирован и лежит в closed_trades.
            # ВАЖНО: ordId, который уже есть в open_trades, НЕ является дублем —
            # это как раз и есть продолжение одного и того же ордера (multi-fill).
            allow_multifill = False
            if ord_id:
                try:
                    # 1) pending => ранее трекер pending_orders сам всё обрабатывал.
                    # именно они должны попадать в ledger. Поэтому:
                    #   - для ws_private / ws_orders НЕ блокируем импорт fills даже если pending есть;
                    #   - для остальных источников (rest_reconcile и т.п.) сохраняем защиту от дублей.
                    src = str(source or '').lower().strip()
                    if src not in ('ws_private', 'ws_orders'):
                        if isinstance(self.pending_orders, dict):
                            for _sym, po in (self.pending_orders or {}).items():
                                if str((po or {}).get('ord_id') or '') == ord_id:
                                    return

                    # 2) если ordId уже присутствует в open_trades этого символа —
                    # разрешаем (это multi-fill), но НЕ создаём новую сделку.
                    lst = (self.open_trades or {}).get(str(symbol), []) or []
                    for t in lst:
                        if side == 'buy' and str(getattr(t, 'buy_ord_id', '') or '') == ord_id:
                            allow_multifill = True
                            break
                        if side == 'sell' and str(getattr(t, 'sell_ord_id', '') or '') == ord_id:
                            allow_multifill = True
                            break

                    # 3) closed_trades: если ordId уже финализирован — это дубль
                    for ct in (self.closed_trades or [])[-400:]:
                        if str(getattr(ct, 'buy_ord_id', '') or '') == ord_id or str(getattr(ct, 'sell_ord_id', '') or '') == ord_id:
                            return
                except Exception:
                    pass

            # время: OKX часто отдаёт миллисекунды
            ts_raw = r.get('fillTime') or r.get('ts') or r.get('uTime') or r.get('cTime')
            try:
                ts = float(ts_raw) if ts_raw is not None else time.time()
                if ts > 1e12:  # ms
                    ts = ts / 1000.0
            except Exception:
                ts = time.time()

            base_ccy = ''
            quote_ccy = ''
            try:
                parts = symbol.split('-', 1)
                base_ccy = parts[0].strip().upper() if len(parts) > 0 else ''
                quote_ccy = parts[1].strip().upper() if len(parts) > 1 else ''
            except Exception:
                base_ccy, quote_ccy = '', ''

            fee = 0.0
            fee_ccy = ''
            try:
                fee = float(r.get('fee') or 0.0)
                fee_ccy = str(r.get('feeCcy') or '').upper()
            except Exception:
                fee = 0.0
                fee_ccy = ''

            fee_amt = abs(float(fee or 0.0))
            fee_mode = ''
            if fee_amt > 0 and fee_ccy:
                if fee_ccy == quote_ccy:
                    fee_mode = 'quote'
                elif fee_ccy == base_ccy:
                    fee_mode = 'base'

            # qty с учётом комиссии в base
            sz = float(sz_gross)
            if fee_amt > 0 and fee_mode == 'base':
                if side == 'buy':
                    sz = max(0.0, float(sz_gross) - fee_amt)
                else:  # sell
                    sz = float(sz_gross) + fee_amt

            # notional: если OKX дал готовое поле — используем его, иначе px*fillSz_gross
            notional = 0.0
            for k in ('fillNotional', 'notional', 'fillNotionalUsd', 'fillNotionalUSDT'):
                try:
                    v = r.get(k)
                    if v is None:
                        continue
                    notional = float(v)
                    if notional > 0:
                        break
                except Exception:
                    continue
            if notional <= 0:
                notional = float(px) * float(sz_gross)

            # fee_usd для отображения/аналитики (если fee в base — переводим по px)
            fee_usd = 0.0
            if fee_amt > 0:
                if fee_mode == 'quote':
                    fee_usd = fee_amt
                elif fee_mode == 'base':
                    fee_usd = fee_amt * float(px)

            # 1) позиция
            # price handling
            # - okx_px: совпадает с OKX avgPx (notional / fillSz_gross)
            # - eff_px: эффективная цена под net-qty (если fee в BASE) для корректного avg_price позиции
            okx_px = (notional / sz_gross) if sz_gross > 0 else float(px)
            eff_px = (notional / sz) if sz > 0 else okx_px
            self.apply_local_fill(symbol=symbol, side=side, qty=float(sz), price=float(eff_px), fee=float(fee_usd))

            # 2) сделки (ledger)
            if side == 'buy':
                # multi-lot: ищем лот по ordId (если есть), иначе создаём новый
                tr = None
                # Поэтому держим мапу ordId->trade_id и стараемся найти уже созданный trade по trade_id.
                map_key = ''
                if ord_id:
                    map_key = f"{symbol}|buy|{ord_id}"
                    try:
                        tid = (self.ordid_trade_map or {}).get(map_key)
                        if tid:
                            for _tr in (self.open_trades or {}).get(str(symbol), []) or []:
                                if str(getattr(_tr, 'trade_id', '') or '') == str(tid):
                                    tr = _tr
                                    break
                    except Exception:
                        tr = None
                
                try:
                    if ord_id:
                        for _tr in (self.open_trades or {}).get(str(symbol), []) or []:
                            if str(getattr(_tr, 'buy_ord_id', '') or '') == str(ord_id):
                                tr = _tr
                                break
                except Exception:
                    tr = None
                if tr is None:
                    tr = Trade(trade_id=self._new_trade_id(), symbol=str(symbol), buy_ts=float(ts), source=str(source or 'okx'))
                    if ord_id:
                            try:
                                self.ordid_trade_map[f"{symbol}|buy|{ord_id}"] = str(tr.trade_id)
                            except Exception:
                                pass
                    sym = str(symbol)
                    lst = (self.open_trades or {}).get(sym)
                    if lst is None or not isinstance(lst, list):
                        lst = []
                        self.open_trades[sym] = lst
                    lst.append(tr)
                if not tr.buy_ts:
                    tr.buy_ts = float(ts)
                tr.buy_qty = float(tr.buy_qty or 0.0) + float(sz)
                tr.buy_usd = float(tr.buy_usd or 0.0) + float(notional)
                tr.buy_fee_usd = float(tr.buy_fee_usd or 0.0) + float(fee_usd)
                # store OKX fill detail for correct UI math
                tr.buy_qty_gross = float(getattr(tr, 'buy_qty_gross', 0.0) or 0.0) + float(sz_gross)
                tr.buy_fee_ccy = str(fee_ccy or tr.buy_fee_ccy or '')
                tr.buy_fee_mode = str(fee_mode or tr.buy_fee_mode or '')
                # buy_fee_amt/sell_fee_amt должны СУММИРОВАТЬСЯ по всем fills одного ordId
                tr.buy_fee_amt = float(getattr(tr, 'buy_fee_amt', 0.0) or 0.0) + float(fee_amt or 0.0)
                # Цена для UI должна совпадать с OKX avgPx
                tr.buy_px = float(okx_px or tr.buy_px or 0.0)
                # сохраняем валюту/режим/сумму комиссии — это нужно для точного PnL
                # и для отображения "как в OKX".
                if fee_mode:
                    tr.buy_fee_mode = str(fee_mode)
                if fee_ccy:
                    tr.buy_fee_ccy = str(fee_ccy)
                # buy_qty_gross уже инкрементирован выше — НЕ удваиваем
                tr.buy_px = (tr.buy_usd / tr.buy_qty_gross) if float(getattr(tr, 'buy_qty_gross', 0.0) or 0.0) else float(px)
                if ord_id and not tr.buy_ord_id:
                    tr.buy_ord_id = str(ord_id)
                tr.source = str(source or tr.source or 'okx')
                self._save_ledger_safe()

                # событие BUY в историю (только один раз на весь ordId / trade)
                if not bool(getattr(tr, '_buy_recorded', False)):
                    setattr(tr, '_buy_recorded', True)
                    self.record_trade({
                    'type': 'BUY',
                    'trade_id': tr.trade_id,
                    'symbol': symbol,
                    'ts': float(ts),
                    'usd': float(notional),
                    'qty': float(sz),
                    'px': float(px),
                    'fee_usd': float(fee_usd),
                    'ord_id': str(ord_id),
                    'source': str(source or 'okx'),
                })

            else:  # sell
                # SELL: если это multi-fill по уже известному ordId — ищем именно эту сделку.
                tr = None
                try:
                    if ord_id:
                        for _tr in (self.open_trades or {}).get(str(symbol), []) or []:
                            if str(getattr(_tr, 'sell_ord_id', '') or '') == str(ord_id):
                                tr = _tr
                                break
                except Exception:
                    tr = None
                if tr is None:
                    tr = self.last_open_trade(symbol)
                pos = self.position(symbol)

                # OKX может дробить один SELL на десятки fills.
                # - ордер SELL может быть multi-fill;
                # - у нас также есть частичные выходы (TP1/TP2), где SELL ордеров несколько.
                # Поэтому мы НЕ ставим sell_ts при каждом fill (иначе сделка в UI станет "закрытой").
                # Мы копим sold_* (фактически проданное) и финализируем sell_* только
                # когда позиция реально закрылась (pos.qty==0 или "пыль").
                if tr is not None:
                    try:
                        tr.sold_ts_last = float(ts or tr.sold_ts_last or 0.0) or float(tr.sold_ts_last or 0.0)
                    except Exception:
                        tr.sold_ts_last = float(ts or 0.0)
                    try:
                        tr.sold_qty_gross = float(getattr(tr, 'sold_qty_gross', 0.0) or 0.0) + float(sz_gross)
                    except Exception:
                        tr.sold_qty_gross = float(sz_gross)
                    try:
                        tr.sold_qty = float(getattr(tr, 'sold_qty', 0.0) or 0.0) + float(sz)
                    except Exception:
                        tr.sold_qty = float(sz)
                    try:
                        tr.sold_usd = float(getattr(tr, 'sold_usd', 0.0) or 0.0) + float(notional)
                    except Exception:
                        tr.sold_usd = float(notional)
                    # fee metadata: сохраняем режим/валюту и СУММУ комиссии в нативной валюте
                    try:
                        if fee_mode:
                            tr.sold_fee_mode = str(fee_mode)
                        if fee_ccy:
                            tr.sold_fee_ccy = str(fee_ccy)
                        if fee_amt > 0:
                            tr.sold_fee_amt = float(getattr(tr, 'sold_fee_amt', 0.0) or 0.0) + float(fee_amt)
                    except Exception:
                        pass
                    try:
                        tr.sold_fee_usd = float(getattr(tr, 'sold_fee_usd', 0.0) or 0.0) + float(fee_usd)
                    except Exception:
                        tr.sold_fee_usd = float(fee_usd)
                    try:
                        # средняя цена по фактически проданному
                        if float(getattr(tr, 'sold_qty_gross', 0.0) or 0.0) > 0:
                            avg_sell_px = float(tr.sold_usd) / float(tr.sold_qty_gross)
                            # пока сделка не закрыта, держим sell_px как "текущую среднюю продажи"
                            tr.sell_px = float(avg_sell_px)
                    except Exception:
                        pass
                    try:
                        if ord_id:
                            # основной sell_ord_id заполним на финализации
                            if ord_id not in (getattr(tr, 'sell_ord_ids', []) or []):
                                tr.sell_ord_ids.append(str(ord_id))
                    except Exception:
                        pass

                    # запоминаем "ногу" продажи (для UI и пост-разбора)
                    try:
                        tr.sell_legs.append({
                            'ts': float(ts),
                            'ord_id': str(ord_id),
                            'usd': float(notional),
                            'qty_gross': float(sz_gross),
                            'px': float(px),
                            'fee_usd': float(fee_usd),
                            'fee_ccy': str(fee_ccy),
                            'fee_amt': float(fee_amt),
                            'fee_mode': str(fee_mode),
                            'source': str(source or 'okx'),
                        })
                        # ограничиваем рост, чтобы ledger не раздувался
                        if len(tr.sell_legs) > 50:
                            tr.sell_legs = tr.sell_legs[-50:]
                    except Exception:
                        pass

                # после полной продажи на OKX часто остаётся микроскопический остаток
                # (из-за округлений и комиссии в base). Такой остаток нельзя продать (меньше min size),
                # но он блокирует режим "1 символ = 1 позиция" и не даёт закрыть сделку в UI.
                # Поэтому если остаток по USD-эквиваленту <= threshold (по умолчанию 1 USDT),
                # считаем позицию закрытой и переносим сделку в историю.
                dust_th = 1.0
                try:
                    # не импортируем конфиг сюда, берём типовой порог; реальный порог использует controller.
                    dust_th = float(os.environ.get('ATE_DUST_USD', '1.0'))
                except Exception:
                    dust_th = 1.0
                try:
                    is_dust = self.is_dust_qty(qty=float(pos.qty or 0.0), px=float(px or 0.0), threshold_usd=float(dust_th))
                except Exception:
                    is_dust = False

                # если продажа пришла из OKX, но в локальной истории нет открытой сделки
                # (например, ручная покупка/продажа происходили вне приложения, а BUY не попал в окно сканирования),
                # всё равно фиксируем событие, чтобы UI не оставался "пустым".
                if tr is None:
                    try:
                        orphan_id = f"ext_{symbol}_{int(float(ts or 0.0))}_{ord_id}"
                        ot = Trade(
                            trade_id=str(orphan_id),
                            symbol=str(symbol),
                            buy_ts=float(ts),
                            buy_usd=0.0,
                            buy_qty=0.0,
                            buy_fee_usd=0.0,
                            buy_fee_ccy=str(fee_ccy or ''),
                            buy_fee_amt=0.0,
                            sell_ts=float(ts),
                            sell_usd=float(notional),
                            sell_qty=float(sz_gross),
                            sell_fee_usd=float(fee_usd),
                            sell_fee_ccy=str(fee_ccy or ''),
                            sell_fee_amt=float(fee_amt or 0.0),
                            sell_reason='Внешняя продажа (BUY не найден)',
                        )
                        self.closed_trades.append(ot)
                        self._save_ledger_safe()
                    except Exception:
                        pass

                if tr is not None and (float(pos.qty or 0.0) <= 1e-12 or is_dust):
                    # закрыли позицию — закрываем строку сделки
                    # для отображения в UI qty показываем как в OKX (gross fillSz)
                    self.on_bot_sell(
                        symbol=symbol,
                        # закрываем суммарно по всем fills, а не по последнему
                        qty=float(getattr(tr, 'sold_qty_gross', 0.0) or sz_gross),
                        price=float(getattr(tr, 'sell_px', 0.0) or px),
                        fee_usd=float(getattr(tr, 'sold_fee_usd', 0.0) or fee_usd),

                        fee_mode=str(fee_mode or ''),
                        fee_ccy=str(fee_ccy or ''),
                        fee_amt=float(getattr(tr, 'sold_fee_amt', 0.0) or (fee_amt or 0.0)),
                        usd_amount=float(getattr(tr, 'sold_usd', 0.0) or notional),
                        ts=float(ts),
                        source=str(source or 'okx'),
                        ord_id=str(ord_id),
                    )
                    # остаток-пыль НЕ затираем (он реально остаётся на бирже),
                    # но сама сделка считается закрытой. Ограничения по BUY/SELL
                    # регулируются порогом "пыль" в контроллере/автотрейдере.
                else:
                    # частичная продажа — фиксируем событие, но сделку не закрываем
                    self._save_ledger_safe()
                    self.record_trade({
                        'type': 'SELL_PARTIAL',
                        'trade_id': tr.trade_id if tr else '',
                        'symbol': symbol,
                        'ts': float(ts),
                        'usd': float(notional),
                        'qty': float(sz_gross),
                        'px': float(px),
                        'fee_usd': float(fee_usd),
                        'ord_id': str(ord_id),
                        'source': str(source or 'okx'),
                    })
        except Exception:
            return

    def trade_rows(self) -> List[Trade]:
        """Список сделок для UI (открытые сверху, затем закрытые по времени)."""
        open_list = [t for lst in (self.open_trades or {}).values() for t in (lst or [])]
        # открытые: по buy_ts убыв.
        open_list.sort(key=lambda t: float(getattr(t, "buy_ts", 0.0) or 0.0), reverse=True)

        closed_list = list(self.closed_trades or [])
        # закрытые: по sell_ts убыв., иначе buy_ts
        closed_list.sort(key=lambda t: float(t.sell_ts or t.buy_ts or 0.0), reverse=True)
        return open_list + closed_list

    def clear_trade_ledger(self) -> None:
        self.open_trades = {}
        self.closed_trades = []
        self.pending_orders = {}
        self._save_ledger_safe()

    def prune_trade_ledger(self, hours: float) -> None:
        """Оставить в локальной истории только сделки за последние N часов.

        Это нужно, чтобы после тестов/сброса активов на OKX не висели "призрачные"
        сделки/ордера в интерфейсе. Pending очищаем всегда (они актуальны только в текущем запуске).
        """
        try:
            h = float(hours)
        except Exception:
            h = 0.0
        if h <= 0:
            self.clear_trade_ledger()
            return

        cutoff = time.time() - (h * 3600.0)

        def _trade_ts(t: Trade) -> float:
            try:
                return float(t.sell_ts or t.buy_ts or 0.0)
            except Exception:
                return 0.0

        try:
            self.closed_trades = [t for t in (self.closed_trades or []) if _trade_ts(t) >= cutoff]
        except Exception:
            pass
        try:
            self.open_trades = {sym: t for sym, t in (self.open_trades or {}).items() if _trade_ts(t) >= cutoff}
        except Exception:
            pass

        # pending всегда очищаем — это только "на сейчас"
        self.pending_orders = {}
        self._save_ledger_safe()