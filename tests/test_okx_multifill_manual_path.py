import tempfile


def _mk_fill(*, inst_id: str, side: str, ord_id: str, px: float, sz: float, fee: float, fee_ccy: str, ts_ms: int):
    return {
        "instId": inst_id,
        "side": side,
        "ordId": ord_id,
        "fillPx": str(px),
        "fillSz": str(sz),
        "fee": str(fee),
        "feeCcy": fee_ccy,
        "fillTime": str(ts_ms),
    }


def test_manual_multifill_ordid_not_deduped():
    """Ручной путь = reconcile_recent_fills -> portfolio.ingest_okx_fill.

    Критично: один ordId может иметь много fills.
    Раньше ingest_okx_fill ошибочно считал повторный ordId дублем и отбрасывал оставшиеся fills.
    """
    from engine.portfolio import Portfolio

    with tempfile.TemporaryDirectory() as d:
        p = Portfolio(data_dir=d)

        inst = "AUCTION-USDT"
        buy_ord = "BUY123"
        sell_ord = "SELL999"
        t0 = 1700000000000

        # BUY: 2 fills, суммарно ~100 USDT
        p.ingest_okx_fill(_mk_fill(inst_id=inst, side="buy", ord_id=buy_ord, px=4.94, sz=10.0, fee=-0.002, fee_ccy="AUCTION", ts_ms=t0), source="okx_fill_scan")
        p.ingest_okx_fill(_mk_fill(inst_id=inst, side="buy", ord_id=buy_ord, px=4.94, sz=10.2479, fee=-0.002, fee_ccy="AUCTION", ts_ms=t0 + 10), source="okx_fill_scan")

        tr = p.last_open_trade(inst)
        assert tr is not None
        assert tr.buy_ord_id == buy_ord
        # Должно быть агрегировано (а не только первый fill)
        assert tr.buy_qty_gross > 20.0
        assert tr.buy_usd > 95.0

        # SELL: 2 fills по тому же принципу
        p.ingest_okx_fill(_mk_fill(inst_id=inst, side="sell", ord_id=sell_ord, px=4.93, sz=10.0, fee=-0.05, fee_ccy="USDT", ts_ms=t0 + 1000), source="okx_fill_scan")
        p.ingest_okx_fill(_mk_fill(inst_id=inst, side="sell", ord_id=sell_ord, px=4.93, sz=10.256, fee=-0.049, fee_ccy="USDT", ts_ms=t0 + 1010), source="okx_fill_scan")

        # Сделка должна закрыться в историю как один агрегированный SELL
        rows = p.trade_rows()
        assert len(rows) >= 1
        closed = rows[0]
        assert closed.sell_ord_id == sell_ord
        assert closed.sell_qty_gross > 20.0
        assert closed.sell_usd > 95.0
