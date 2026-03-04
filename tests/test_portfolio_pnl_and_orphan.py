from engine.portfolio import Trade, Portfolio


def test_realized_pnl_uses_abs_fee_values_for_quote_and_base():
    tr_q = Trade(
        trade_id="tq",
        symbol="BTC-USDT",
        buy_ts=1.0,
        buy_usd=100.0,
        buy_qty=0.01,
        buy_px=10000.0,
        buy_fee_mode="quote",
        buy_fee_amt=-0.1,
        sell_ts=2.0,
        sell_usd=101.0,
        sell_qty=0.01,
        sell_px=10100.0,
        sell_fee_mode="quote",
        sell_fee_amt=-0.1,
    )
    pnl_q, _ = tr_q.realized_pnl()
    assert round(pnl_q, 6) == round(101.0 - 0.1 - (100.0 + 0.1), 6)

    tr_b = Trade(
        trade_id="tb",
        symbol="ETH-USDT",
        buy_ts=1.0,
        buy_usd=100.0,
        buy_qty=1.0,
        buy_px=100.0,
        sell_ts=2.0,
        sell_usd=101.0,
        sell_qty=1.0,
        sell_px=101.0,
        sell_fee_mode="base",
        sell_fee_amt=-0.001,
    )
    pnl_b, _ = tr_b.realized_pnl()
    assert round(pnl_b, 6) == round(101.0 - (0.001 * 101.0) - 100.0, 6)


def test_ingest_orphan_sell_creates_closed_trade(tmp_path):
    p = Portfolio(data_dir=str(tmp_path), cash_usd=1000.0, equity_usd=1000.0)

    p.ingest_okx_fill(
        {
            "instId": "XRP-USDT",
            "side": "sell",
            "ordId": "orphan-1",
            "fillPx": "0.5",
            "fillSz": "10",
            "fee": "-0.01",
            "feeCcy": "USDT",
            "fillTime": "1700000000000",
        },
        source="okx_fill_scan",
    )

    assert len(p.closed_trades) == 1
    ct = p.closed_trades[0]
    assert ct.symbol == "XRP-USDT"
    assert ct.sell_reason == "Внешняя продажа (BUY не найден)"
    assert ct.sell_usd > 0
