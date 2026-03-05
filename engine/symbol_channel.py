from __future__ import annotations
import time, threading, math, re, os
from typing import Dict, Any, Optional
from queue import Queue
from okx.public_client import OKXPublicClient
from engine.metrics import (
    rsi, ema, macd, atr_pct, bollinger, returns_pct, slope_pct,
    vol_sma, volatility_1h_pct, book_imbalance, buy_ratio_from_trades
)
from engine.market_regime import update_shared_market, inject_market_into_features
from engine.logging_utils import log_event
import json


def _rects_from_ratio(ok: int, total: int) -> int:
    """0..4 прямоугольника по доле ok/total."""
    try:
        ok_i = int(ok)
        tot_i = int(total)
        if tot_i <= 0:
            return 0
        r = float(ok_i) / float(tot_i)
        v = int(math.floor(r * 4.0))
        if v < 0:
            v = 0
        if v > 4:
            v = 4
        return v
    except Exception:
        return 0

def _reason_ru_short(code: str, raw_reason: str = "", *, max_len: int = 28) -> str:
    """Короткий RU-текст для колонки 'Причина' в мониторинге.

    Требования:
    - ВСЕ причины на русском
    - Без суффиксов _ERROR
    - Если не смогли распарсить — показываем исходный код/текст (но тоже коротко)
    """
    c = (code or "").strip()
    r = (raw_reason or "").strip()
    # нормализуем
    c_up = c.upper()

    # убираем суффикс _ERROR и лишние хвосты
    if c_up.endswith("_ERROR"):
        c_up = c_up[:-6]
    if r.upper().endswith("_ERROR"):
        r = re.sub(r"_ERROR\s*$", "", r, flags=re.IGNORECASE)

    # базовые коды
    mp = {
        "OK": "ОК",
        "NO_SIGNAL": "Нет сигн.",
        "LOW_SCORE": "Низк. увер.",
        "WARMUP": "Прогрев",
        "COOLDOWN": "Кулдаун",
        "HAVE_POS": "Уже в поз.",
        "NO_POS": "Нет поз.",
        "BLOCK_LIQ": "Ликвидн.",
        "FORCE_EXIT": "Force exit",
        "EXIT": "Выход",
        "EXIT_SL": "Стоп-лосс",
        "EXIT_TP": "Тейк-профит",
        "EXIT_LOCK": "Лок-профит",
        "EXIT_MP": "Микро-профит",
        "EXIT_TO": "Таймаут",
        "TRAIL_FROM_PEAK": "Трейлинг",
        "MAX_POS": "Лимит поз.",
        "EXEC_BLOCK": "Купить⛔",
    }
    if c_up in mp:
        out = mp[c_up]
    elif c_up.startswith("LAG>"):
        out = "Лаг " + c_up.replace("LAG>", ">")
    elif c_up.startswith("BLOCK_"):
        out = "Блок: " + c_up.replace("BLOCK_", "")
    elif c_up.startswith("ENTRY_BLOCK"):
        # ENTRY_BLOCK: BTC_DOWN / MARKET_BLOCK / falling_knife
        if "BTC" in c_up:
            out = "Блок: BTC вниз"
        elif "MARKET" in c_up:
            out = "Блок: рынок"
        elif "FALLING" in c_up:
            out = "Блок: падение"
        else:
            out = "Блок входа"

    elif c_up.startswith("PRV_STALE") or "PRV_STALE" in r.upper():
        out = "Купить⛔ PRV"
    elif c_up.startswith("PENDING") or "PENDING" in r.upper():
        out = "Купить⛔ Ордер"
    elif c_up.startswith("MAX_POS") or "MAX_POS" in r.upper():
        out = "Купить⛔ Лимит"
    elif c_up.startswith("COOLDOWN") or "COOLDOWN" in r.upper():
        out = "Купить⛔ Кулдаун"
    elif c_up.startswith("SPREAD") or "SPREAD" in r.upper():
        out = "Купить⛔ Спред"
    elif c_up.startswith("LAG") or "LAG" in r.upper():
        out = "Купить⛔ Лаг"
    elif c_up.startswith("LOW_SCORE") or "LOW_SCORE" in r.upper():
        out = "Купить⛔ Увер"
    elif c_up.startswith("SIGNAL_TTL") or "SIGNAL_TTL" in r.upper():
        out = "Купить⛔ TTL"

    elif "MARKET" in r.upper() and "СЛАБЫЙ" in r.lower():
        out = "Рынок слабый"
    elif "ТРЕНД" in r.lower() and "вниз" in r.lower():
        out = "Тренд вниз"

    elif "SPREAD" in (c_up + " " + r.upper()):
        # вытащим число если есть
        m = re.search(r"SPREAD[^0-9]*([0-9]+\.?[0-9]*)", r.upper())
        out = f"Спред {m.group(1)}%" if m else "Спред"
    elif "NO_PROGRESS" in (c_up + " " + r.upper()) or "НЕТ_ПРОГРЕССА" in r.upper():
        out = "Нет прогресса"
    elif "ENTRY_GATE" in (c_up + " " + r.upper()):
        m = re.search(r"score\s*=\s*(\d+)", r)
        out = f"EntryGate s={m.group(1)}" if m else "EntryGate"
    else:
        # fallback: сначала raw_reason, потом code
        out = r or c or "—"

    out = out.replace("_ERROR", "").strip()
    # укорачиваем
    if len(out) > max_len:
        out = out[:max_len-1] + "…"
    return out

class SymbolChannel(threading.Thread):
    def _safe_put(self, msg: dict) -> None:
        try:
            self.ui_queue.put_nowait(msg)
        except Exception:
            # queue full or closed — пропускаем, чтобы не подвесить поток
            pass


    def _safe_put_signal(self, msg: dict) -> None:
        try:
            if self.signal_queue is not None:
                self.signal_queue.put_nowait(msg)
        except Exception:
            pass

    
    def _build_position_snapshot(self, px_last: float, fee_rate: float, shared: dict) -> dict:
        """Позиция для стратегии/логов. Критично: без этого micro-profit и выходы не работают."""
        try:
            port = shared.get("portfolio_obj")
        except Exception:
            port = None
        if port is None:
            return {"status": "IDLE"}
        try:
            pos = (getattr(port, "positions", {}) or {}).get(self.symbol)
        except Exception:
            pos = None
        if not pos or float(getattr(pos, "qty", 0.0) or 0.0) <= 0:
            return {"status": "IDLE"}
        try:
            qty = float(getattr(pos, "qty", 0.0) or 0.0)
            avg = float(getattr(pos, "avg_price", 0.0) or 0.0)
            opened_ts = float(getattr(pos, "opened_ts", 0.0) or 0.0)
            peak = float(getattr(pos, "peak_price", 0.0) or 0.0)
            fee_paid = float(getattr(pos, "fee_paid", 0.0) or 0.0)
        except Exception:
            return {"status": "HOLDING"}
        # update last/peak (не ломаем основную логику — только если поле есть)
        try:
            pos.last_price = float(px_last)
            if pos.peak_price <= 0 or float(px_last) > float(pos.peak_price):
                pos.peak_price = float(px_last)
            peak = float(pos.peak_price)
        except Exception:
            pass
        hold_sec = 0.0
        try:
            if opened_ts > 0:
                hold_sec = max(0.0, float(time.time()) - opened_ts)
        except Exception:
            hold_sec = 0.0
        # NET PnL% (учитываем комиссии спота: buy fee уже в fee_paid, sell fee оцениваем как fee_rate от notional)
        net_pct = 0.0
        try:
            if qty > 0 and avg > 0:
                gross = (float(px_last) - avg) / avg
                notional = qty * avg
                # fee_paid в USD (quote) мы ведём суммарно; если вдруг base — уже переведено на записи
                fee_pct_buy = (fee_paid / notional) if notional > 0 else 0.0
                fee_pct_sell = float(fee_rate or 0.0)
                net_pct = gross - fee_pct_buy - fee_pct_sell
        except Exception:
            net_pct = 0.0
        return {
            "status": "HOLDING",
            "qty": qty,
            "avg_price": avg,
            "opened_ts": opened_ts,
            "holding_sec": hold_sec,
            "peak_price": peak,
            "pnl_net_pct": net_pct,
        }
    def __init__(
        self,
        *,
        symbol: str,
        public: OKXPublicClient,
        public_ws=None,
        strategy_instance,
        portfolio,
        ui_queue: Queue,
        signal_queue: Optional[Queue] = None,
        shared_state: Optional[dict] = None,
        stop_event: threading.Event,
        fetch_candles_every: float = 15.0,
        fetch_book_every: float = 2.0,
        fetch_trades_every: float = 2.0,
    ):
        super().__init__(daemon=True)
        self.symbol = symbol
        self.started_ts: float = time.time()

        self.public = public
        self.public_ws = public_ws
        self.strategy = strategy_instance
        self.portfolio = portfolio
        self.ui_queue = ui_queue
        self.last_tick_ts: float = 0.0            # last price tick timestamp (exchange)
        self.last_confidence: float = 0.0         # last computed confidence
        self.last_nonzero_conf_ts: float = 0.0    # local time when confidence last > 0
        self.signal_queue = signal_queue
        self.shared_state = shared_state or {}
        self.stop_event = stop_event
        self.fetch_candles_every = fetch_candles_every
        self.fetch_book_every = fetch_book_every
        self.fetch_trades_every = fetch_trades_every

        self.last_candles_ts = 0.0
        # отдельный таймер для 5m свечей (реже, но независимо)
        self.last_candles_5m_ts = 0.0
        self.last_book_ts = 0.0
        self.last_trades_ts = 0.0

        self.prices_1m = []
        self.highs_1m = []
        self.lows_1m = []
        self.volumes_1m = []

        # 5m свечи (для подтверждения смены тренда / режима входа)
        self.prices_5m = []
        self.highs_5m = []
        self.lows_5m = []
        self.volumes_5m = []

        self.book = {}
        self.trades = []

        self._last_aux_err_ts = 0.0
        self._last_aux_err_msg = ""

        self._last_rest_ticker_ts = 0.0
        self._rest_ticker_backoff_until = 0.0

        self._prev_last: float = 0.0

        # Анти-спам для диагностических логов PREP_BUY
        self._last_prep_buy_log_ts: float = 0.0
        self._last_prep_buy_sig = None

        # --- Warmup / readiness ---
        # Пользователь хочет видеть готовность к трейдингу по каждому каналу.
        self.warmup_need_1m: int = 120
        self.warmup_need_5m: int = 120
        self.warmup_ok_1m: bool = False
        self.warmup_ok_5m: bool = False
        self.warmup_ready: bool = False
        self.warmup_stage: str = "INIT"  # INIT/WARMUP/WARMUP_5M/OK/ERR

        try:
            if isinstance(self.shared_state, dict):
                self.shared_state.setdefault("warmup_ready_by_symbol", {})
                self.shared_state.setdefault("warmup_stage_by_symbol", {})
        except Exception:
            pass

    def _update_candles(self):
        candles = self.public.candles(self.symbol, bar="1m", limit=200)
        # OKX candles: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
        candles = list(reversed(candles))  # oldest->newest
        closes = []
        highs = []
        lows = []
        vols = []
        for c in candles:
            if len(c) < 6:
                continue
            highs.append(float(c[2]))
            lows.append(float(c[3]))
            closes.append(float(c[4]))
            vols.append(float(c[5]))
        if closes:
            self.prices_1m = closes
            self.highs_1m = highs
            self.lows_1m = lows
            self.volumes_1m = vols

    def _update_candles_5m(self):
        """Обновление 5m свечей.

        5m используем для режима входа по смене тренда (меньше шума, меньше пропусков).
        """
        candles = self.public.candles(self.symbol, bar="5m", limit=200)
        candles = list(reversed(candles))  # oldest->newest
        closes: list[float] = []
        highs: list[float] = []
        lows: list[float] = []
        vols: list[float] = []
        for c in candles:
            if len(c) < 6:
                continue
            highs.append(float(c[2]))
            lows.append(float(c[3]))
            closes.append(float(c[4]))
            vols.append(float(c[5]))
        if closes:
            self.prices_5m = closes
            self.highs_5m = highs
            self.lows_5m = lows
            self.volumes_5m = vols

    def _update_book(self):
        self.book = self.public.books(self.symbol, sz=20)

    def _update_trades(self):
        self.trades = self.public.trades(self.symbol, limit=100)

    def _compute_metrics(self, last_price: float) -> Dict[str, Any]:
        prices_src = self.prices_1m if self.prices_1m else [last_price]
        highs_src = self.highs_1m if self.highs_1m else list(prices_src)
        lows_src = self.lows_1m if self.lows_1m else list(prices_src)
        vols_src = self.volumes_1m if self.volumes_1m else [0.0] * len(prices_src)

        # ВАЖНО (live): индикаторы (RSI/MACD/ATR/BB/ret/slope) должны обновляться внутри 1m бара.
        # Поэтому "закрытие" последней свечи заменяем на текущую цену, а high/low последней свечи
        # расширяем текущей ценой. Это делает метрики живыми несколько раз в секунду.
        prices = list(prices_src)
        highs = list(highs_src) if len(highs_src) == len(prices_src) else list(prices_src)
        lows = list(lows_src) if len(lows_src) == len(prices_src) else list(prices_src)
        vols = list(vols_src) if len(vols_src) == len(prices_src) else [0.0] * len(prices_src)

        if prices:
            prices[-1] = float(last_price)
            try:
                highs[-1] = max(float(highs[-1]), float(last_price))
                lows[-1] = min(float(lows[-1]), float(last_price))
            except Exception:
                highs[-1] = float(last_price)
                lows[-1] = float(last_price)

        rsi14 = rsi(prices, 14)
        ema20 = ema(prices, 20)
        ema50 = ema(prices, 50)
        ema12 = ema(prices, 12)
        ema26 = ema(prices, 26)
        m_line, m_sig, m_hist = macd(prices, 12, 26, 9)
        atr14_pct = atr_pct(highs, lows, prices, 14)
        bb_upper, bb_lower, bb_width = bollinger(prices, 20, 2.0)
        imb = book_imbalance(self.book)
        # лучшие цены для расчёта реалистичной цены исполнения и спреда
        best_bid = 0.0
        best_ask = 0.0
        try:
            bids = (self.book or {}).get('bids') or []
            asks = (self.book or {}).get('asks') or []
            if bids and isinstance(bids, list) and len(bids[0]) >= 1:
                best_bid = float(bids[0][0])
            if asks and isinstance(asks, list) and len(asks[0]) >= 1:
                best_ask = float(asks[0][0])
        except Exception:
            best_bid = 0.0
            best_ask = 0.0

        spread_pct = 0.0
        try:
            if best_bid > 0 and best_ask > 0:
                mid = (best_bid + best_ask) / 2.0
                if mid > 0:
                    spread_pct = ((best_ask - best_bid) / mid) * 100.0
        except Exception:
            spread_pct = 0.0
        buy_ratio = buy_ratio_from_trades(self.trades)
        slope30 = slope_pct(prices, 30)
        ret_1 = returns_pct(prices, 1)
        ret_5 = returns_pct(prices, 5)
        ret_15 = returns_pct(prices, 15)
        volume = vols[-1] if vols else 0.0
        vol_sma20 = vol_sma(vols, 20)
        vol_1h = volatility_1h_pct(prices)
        utc_hour = int(time.gmtime().tm_hour)

        # --- 5m indicators (trend-change confirmation) ---
        prices5_src = self.prices_5m if self.prices_5m else []
        highs5_src = self.highs_5m if self.highs_5m else []
        lows5_src = self.lows_5m if self.lows_5m else []
        vols5_src = self.volumes_5m if self.volumes_5m else []

        prices5 = list(prices5_src)
        highs5 = list(highs5_src) if len(highs5_src) == len(prices5_src) else list(prices5_src)
        lows5 = list(lows5_src) if len(lows5_src) == len(prices5_src) else list(prices5_src)

        if prices5:
            prices5[-1] = float(last_price)
            try:
                highs5[-1] = max(float(highs5[-1]), float(last_price))
                lows5[-1] = min(float(lows5[-1]), float(last_price))
            except Exception:
                highs5[-1] = float(last_price)
                lows5[-1] = float(last_price)

        rsi14_5m = rsi(prices5, 14) if prices5 else 50.0
        _ml5, _ms5, macd_hist_5m = macd(prices5, 12, 26, 9) if prices5 else (0.0, 0.0, 0.0)
        ema20_5m = ema(prices5, 20) if prices5 else 0.0
        ema50_5m = ema(prices5, 50) if prices5 else 0.0
        slope30_5m = slope_pct(prices5, 30) if prices5 else 0.0
        ret_15_5m = returns_pct(prices5, 3) if prices5 else 0.0
        ret_60_5m = returns_pct(prices5, 12) if prices5 else 0.0
        atr14_pct_5m = atr_pct(highs5, lows5, prices5, 14) if prices5 else 0.0

        return {
            "last": last_price,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread_pct": spread_pct,
            "rsi14": rsi14,
            "ema20": ema20,
            "ema50": ema50,
            "ema12": ema12,
            "ema26": ema26,
            "macd": m_line,
            "macd_hist": m_hist,
            "atr14_pct": atr14_pct,
            "bb_upper": bb_upper,
            "bb_lower": bb_lower,
            "bb_width": bb_width,
            "imbalance": imb,
            "buy_ratio": buy_ratio,
            "slope30_pct": slope30,
            "ret_1": ret_1,
            "ret_5": ret_5,
            "ret_15": ret_15,
            "volume": volume,
            "vol_sma20": vol_sma20,
            "volatility_1h_pct": vol_1h,
            "utc_hour": utc_hour,
            "rsi14_5m": rsi14_5m,
            "macd_hist_5m": macd_hist_5m,
            "ema20_5m": ema20_5m,
            "ema50_5m": ema50_5m,
            "slope30_5m": slope30_5m,
            "ret_15_5m": ret_15_5m,
            "ret_60_5m": ret_60_5m,
            "atr14_pct_5m": atr14_pct_5m,
            "candles_1m_n": len(prices_src) if prices_src else 0,
            "candles_5m_n": len(prices5) if prices5 else 0,
        }

    
    def _data_dir(self) -> str:
        try:
            shared = self.shared_state if isinstance(self.shared_state, dict) else {}
            dd = shared.get("data_dir") or shared.get("DATA_DIR")
            if dd:
                return str(dd)
        except Exception:
            pass
        # fallback: <project_root>/data
        try:
            return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
        except Exception:
            return "data"

    def _log_candle_status(self, *, level: str, msg: str, extra: dict | None = None) -> None:
        try:
            log_event(self._data_dir(), {"level": level, "msg": msg, "extra": extra or {}})
        except Exception:
            pass

    def _warmup_history(self) -> None:
        """Загрузка исторических данных на старте (1m + 5m).

        Требование пользователя: при старте программа должна загрузить всю необходимую историю,
        а далее окно данных должно двигаться (обновления добавляют новые и выбрасывают старые).
        """
        # Стагер, чтобы 50 каналов не ударили REST в одну секунду
        try:
            cfg = (self.shared_state.get("cfg") or {}) if isinstance(self.shared_state, dict) else {}
            tcfg = (cfg.get("trading") or {}) if isinstance(cfg.get("trading"), dict) else {}
            stagger_max = float(tcfg.get("startup_stagger_max_sec", 8.0) or 8.0)
        except Exception:
            stagger_max = 8.0
        try:
            if stagger_max > 0:
                delay = (abs(hash(self.symbol)) % 1000) / 1000.0 * stagger_max
                time.sleep(delay)
        except Exception:
            pass

        # Сколько свечей нужно для расчёта EMA50/MACD/ATR и режимов.
        try:
            cfg = (self.shared_state.get("cfg") or {}) if isinstance(self.shared_state, dict) else {}
            tcfg = (cfg.get("trading") or {}) if isinstance(cfg.get("trading"), dict) else {}
            need_1m = int(tcfg.get("warmup_candles_1m", 120) or 120)
            need_5m = int(tcfg.get("warmup_candles_5m", 120) or 120)
        except Exception:
            need_1m, need_5m = 120, 120

        self.warmup_need_1m = int(need_1m)
        self.warmup_need_5m = int(need_5m)
        self.warmup_stage = "WARMUP"
        try:
            if isinstance(self.shared_state, dict):
                (self.shared_state.get("warmup_stage_by_symbol") or {})[self.symbol] = self.warmup_stage
        except Exception:
            pass

        # Пытаемся несколько раз (с мягким backoff)
        for attempt in range(1, 4):
            ok1 = False
            ok5 = False
            err1 = ""
            err5 = ""
            try:
                candles1 = self.public.candles(self.symbol, bar="1m", limit=max(need_1m, 200))
                if candles1:
                    # reuse existing parser
                    candles1 = list(reversed(candles1))
                    closes=[]; highs=[]; lows=[]; vols=[]
                    for c in candles1:
                        if len(c) < 6:
                            continue
                        highs.append(float(c[2])); lows.append(float(c[3])); closes.append(float(c[4])); vols.append(float(c[5]))
                    if closes:
                        self.prices_1m = closes[-max(need_1m, 200):]
                        self.highs_1m = highs[-max(need_1m, 200):]
                        self.lows_1m = lows[-max(need_1m, 200):]
                        self.volumes_1m = vols[-max(need_1m, 200):]
                        ok1 = True
            except Exception as e:
                err1 = str(e)

            try:
                candles5 = self.public.candles(self.symbol, bar="5m", limit=max(need_5m, 200))
                if candles5:
                    candles5 = list(reversed(candles5))
                    closes=[]; highs=[]; lows=[]; vols=[]
                    for c in candles5:
                        if len(c) < 6:
                            continue
                        highs.append(float(c[2])); lows.append(float(c[3])); closes.append(float(c[4])); vols.append(float(c[5]))
                    if closes:
                        self.prices_5m = closes[-max(need_5m, 200):]
                        self.highs_5m = highs[-max(need_5m, 200):]
                        self.lows_5m = lows[-max(need_5m, 200):]
                        self.volumes_5m = vols[-max(need_5m, 200):]
                        ok5 = True
            except Exception as e:
                err5 = str(e)

            if ok1:
                self.last_candles_ts = time.time()
            if ok5:
                self.last_candles_5m_ts = time.time()

            # фиксируем warmup статус
            self.warmup_ok_1m = bool(ok1) and len(self.prices_1m) >= int(self.warmup_need_1m)
            self.warmup_ok_5m = bool(ok5) and len(self.prices_5m) >= int(self.warmup_need_5m)
            self.warmup_ready = bool(self.warmup_ok_1m and self.warmup_ok_5m)
            self.warmup_stage = "OK" if self.warmup_ready else ("WARMUP_5M" if self.warmup_ok_1m else "WARMUP")
            try:
                if isinstance(self.shared_state, dict):
                    (self.shared_state.get("warmup_ready_by_symbol") or {})[self.symbol] = bool(self.warmup_ready)
                    (self.shared_state.get("warmup_stage_by_symbol") or {})[self.symbol] = self.warmup_stage
            except Exception:
                pass

            self._log_candle_status(
                level="INFO" if (ok1 or ok5) else "WARN",
                msg="CANDLES_WARMUP",
                extra={
                    "symbol": self.symbol,
                    "attempt": attempt,
                    "ok_1m": ok1,
                    "n_1m": len(self.prices_1m),
                    "err_1m": err1[:120],
                    "ok_5m": ok5,
                    "n_5m": len(self.prices_5m),
                    "err_5m": err5[:120],
                },
            )

            # Если есть хотя бы 1m — можно работать; 5m может догреться позже.
            if ok1:
                break

            # backoff
            time.sleep(2.0 * attempt)

    def run(self):
            # Прогрев истории (1m + 5m) до начала торговых решений
            try:
                self._warmup_history()
            except Exception:
                pass
            while not self.stop_event.is_set():
                try:
                    disabled = self.shared_state.get('disabled_symbols') if isinstance(self.shared_state, dict) else None
                    if disabled and self.symbol in disabled:
                        self._safe_put({"type":"warn","symbol":self.symbol,"warn":"channel disabled (untradeable/not available)"})
                        break
                    rstop = self.shared_state.get('runtime_stop_symbols') if isinstance(self.shared_state, dict) else None
                    if rstop and self.symbol in rstop:
                        self._safe_put({"type":"warn","symbol":self.symbol,"warn":f"Канал {self.symbol} остановлен runtime reconcile (reason=stopped_runtime_reconcile)"})
                        break
                except Exception:
                    pass
                t0 = time.time()
                try:
                    # Prefer WS tickers (lower lag, scalable to many symbols).
                    last = 0.0
                    last_ts = 0.0
                    if self.public_ws is not None:
                        try:
                            last, last_ts = self.public_ws.get_last(self.symbol)
                        except Exception:
                            last, last_ts = 0.0, 0.0
    
                    now_ts = time.time()
                    # Приоритет цены для открытых позиций: если позиция уже есть,
                    # разрешаем REST fallback чаще (чтобы не упустить момент выхода).
                    try:
                        _posd0 = self.portfolio.position_dict(self.symbol) or {}
                        _qty0 = float(_posd0.get("base_qty") or _posd0.get("qty") or 0.0)
                    except Exception:
                        _qty0 = 0.0
                    _rest_interval = 3.0 if _qty0 > 0.0 else 15.0
    
    
                    ws_is_fresh = bool(last) and bool(last_ts) and (now_ts - float(last_ts)) <= 20.0
    
                    if not ws_is_fresh:
                        # REST fallback: базово 15 сек, но для открытых позиций чаще (3 сек), с backoff при 429.
                        if now_ts >= float(getattr(self, '_rest_ticker_backoff_until', 0.0)) and (now_ts - float(getattr(self, '_last_rest_ticker_ts', 0.0))) >= float(_rest_interval):
                            try:
                                ticker = self.public.ticker(self.symbol)
                                last_val = ticker.get('last') or ticker.get('lastPx')
                                if last_val:
                                    last = float(last_val)
                                    last_ts = now_ts
                                self._last_rest_ticker_ts = now_ts
                            except Exception as e:
                                em = str(e)
                                if '429' in em or 'Too Many' in em or 'too many' in em:
                                    self._rest_ticker_backoff_until = now_ts + 30.0
                                    self._safe_put({'type':'warn','symbol':self.symbol,'warn':'REST ticker 429: backoff 30s'})
                                else:
                                    self._safe_put({'type':'warn','symbol':self.symbol,'warn':'REST ticker fail: ' + em})
    
                    # Если цены нет (пара недоступна/WS не даёт данные) — не считаем метрики
                    # и не обновляем health. Супервизор Auto-TOP заменит символ.
                    if not last:
                        try:
                            self._safe_put({
                                "type": "tick",
                                "symbol": self.symbol,
                                "last": 0.0,
                                "last_ts": 0.0,
                            "warmup_ready": bool(getattr(self, 'warmup_ready', False)),
                            "warmup_stage": str(getattr(self, 'warmup_stage', 'INIT')),
                                "metrics": {},
                                "decision": {"action": "HOLD", "confidence": 0.0, "action_ui": "HOLD", "reason_ui": "NO_PRICE", "confidence_ui": 0.0},
                                "position": self.portfolio.position_dict(self.symbol),
                                "portfolio": self.portfolio.portfolio_state(),
                                "ts": time.time(),
                            })
                        except Exception:
                            pass
                        time.sleep(1.0)
                        continue
    
                    if last:
                        try:
                            prev_last = float(getattr(self, "_prev_last", 0.0) or 0.0)
                        except Exception:
                            prev_last = 0.0
                        self.portfolio.on_price(self.symbol, last)
    
                except Exception as e:
                    # Если инструмент не существует/не доступен — не крутимся бесконечно.
                    em = str(e)
                    self._safe_put({"type":"error", "symbol": self.symbol, "error": em})
                    if "51001" in em or "Instrument" in em and "exist" in em:
                        break
                    time.sleep(1.0)
                    continue
    
                now = time.time()
                try:
                    if now - self.last_candles_ts >= self.fetch_candles_every:
                        try:
                            self._update_candles()
                        except Exception as e:
                            em = str(e)
                            if em != self._last_aux_err_msg or (time.time() - self._last_aux_err_ts) > 30:
                                self._last_aux_err_msg = em
                                self._last_aux_err_ts = time.time()
                                self._safe_put({"type":"warn","symbol":self.symbol,"warn":"aux fetch: " + em})
    
                        self.last_candles_ts = now
    
                    # 5m свечи нужны реже. По умолчанию — раз в 60 секунд,
                    # но можно переопределить через shared_state (trading.fetch_candles_5m_every).
                    try:
                        shared = (self.shared_state or {})
                        _tcfg = (shared.get('cfg') or {}).get('trading') if isinstance(shared.get('cfg'), dict) else None
                        fetch_5m_every = float((_tcfg or {}).get('fetch_candles_5m_every', 60.0) or 60.0)
                    except Exception:
                        fetch_5m_every = 60.0
                    if now - float(getattr(self, 'last_candles_5m_ts', 0.0) or 0.0) >= fetch_5m_every:
                        try:
                            self._update_candles_5m()
                        except Exception as e:
                            em = str(e)
                            if em != self._last_aux_err_msg or (time.time() - self._last_aux_err_ts) > 30:
                                self._last_aux_err_msg = em
                                self._last_aux_err_ts = time.time()
                                self._safe_put({"type":"warn","symbol":self.symbol,"warn":"aux fetch(5m): " + em})
                        self.last_candles_5m_ts = now

                        # обновим readiness по 5m (может догреться после старта)
                        try:
                            self.warmup_ok_5m = bool(self.prices_5m) and len(self.prices_5m) >= int(self.warmup_need_5m)
                            self.warmup_ok_1m = bool(self.prices_1m) and len(self.prices_1m) >= int(self.warmup_need_1m)
                            self.warmup_ready = bool(self.warmup_ok_1m and self.warmup_ok_5m)
                            self.warmup_stage = "OK" if self.warmup_ready else ("WARMUP_5M" if self.warmup_ok_1m else "WARMUP")
                            if isinstance(self.shared_state, dict):
                                (self.shared_state.get("warmup_ready_by_symbol") or {})[self.symbol] = bool(self.warmup_ready)
                                (self.shared_state.get("warmup_stage_by_symbol") or {})[self.symbol] = self.warmup_stage
                        except Exception:
                            pass
                    if now - self.last_book_ts >= self.fetch_book_every:
                        try:
                            self._update_book()
                        except Exception as e:
                            em = str(e)
                            if em != self._last_aux_err_msg or (time.time() - self._last_aux_err_ts) > 30:
                                self._last_aux_err_msg = em
                                self._last_aux_err_ts = time.time()
                                self._safe_put({"type":"warn","symbol":self.symbol,"warn":"aux fetch: " + em})
    
                        self.last_book_ts = now
                    if now - self.last_trades_ts >= self.fetch_trades_every:
                        try:
                            self._update_trades()
                        except Exception as e:
                            em = str(e)
                            if em != self._last_aux_err_msg or (time.time() - self._last_aux_err_ts) > 30:
                                self._last_aux_err_msg = em
                                self._last_aux_err_ts = time.time()
                                self._safe_put({"type":"warn","symbol":self.symbol,"warn":"aux fetch: " + em})
    
                        self.last_trades_ts = now
                except Exception as e:
                    # non-fatal
                    self._safe_put({"type":"warn", "symbol": self.symbol, "warn": f"aux fetch: {e}"})
    
                metrics = self._compute_metrics(last)
                position = self.portfolio.position_dict(self.symbol)
                pstate = self.portfolio.portfolio_state()
    
                try:
                    shared = (self.shared_state or {})
                    dust_th = float(shared.get("dust_usd_threshold", 1.0) or 1.0)
                    bq = float((position or {}).get("base_qty", 0.0) or 0.0)
                    px_ref = float((metrics or {}).get("last") or 0.0)
                    fee_rate = float((shared.get("fee_rate_by_symbol") or {}).get(str(self.symbol), shared.get("fee_rate", 0.001)) or 0.001)
                    position_live = self._build_position_snapshot(px_last=float(px_ref), fee_rate=fee_rate, shared=shared)
    
                    if dust_th > 0 and bq > 0 and px_ref > 0 and self.portfolio.is_dust_qty(qty=bq, px=px_ref, threshold_usd=dust_th):
                        # чистим локально, чтобы стратегия/автотрейдер не считали это позицией
                        try:
                            p = self.portfolio.position(self.symbol)
                            p.qty = 0.0
                            p.avg_price = 0.0
                            p.opened_ts = 0.0
                            p.peak_price = 0.0
                            p.last_price = float(px_ref or 0.0)
                        except Exception:
                            pass
                        fee_rate = float((shared.get('fee_rate_by_symbol') or {}).get(str(self.symbol), 0.001) or 0.001)
                        position = self._build_position_snapshot(px_last=float(px_ref), fee_rate=fee_rate, shared=shared)
                        position = {"status": "IDLE", "is_dust": True}
                        try:
                            dm = shared.setdefault("dust_ignored", {})
                            dm[str(self.symbol)] = {"ts": float(time.time()), "px": float(px_ref), "usd": float(bq * px_ref)}
                        except Exception:
                            pass
                except Exception:
                    pass
    
    
                # Формируем фичи для стратегии (это и будет логироваться в Decision Log).
                n1 = int(len(self.prices_1m) if hasattr(self, "prices_1m") else 0)
                n5 = int(len(self.prices_5m) if hasattr(self, "prices_5m") else 0)

                features = {
                    "symbol": self.symbol,
                    "last_price": metrics["last"],
                    # Совместимость ключей: стратегия может смотреть и *_n, и без суффикса.
                    "candles_1m": n1,
                    "candles_5m": n5,
                    "candles_1m_n": n1,
                    "candles_5m_n": n5,
                    "rsi14": metrics["rsi14"],
                    "ema12": metrics["ema12"],
                    "ema26": metrics["ema26"],
                    "macd": metrics["macd"],
                    "macd_hist": metrics["macd_hist"],
                    "atr14_pct": metrics["atr14_pct"],
                    "best_bid": metrics.get("best_bid", 0.0),
                    "best_ask": metrics.get("best_ask", 0.0),
                    "spread_pct": metrics.get("spread_pct", 0.0),
                    "bb_upper": metrics["bb_upper"],
                    "bb_lower": metrics["bb_lower"],
                    "bb_width": metrics["bb_width"],
                    "imbalance": metrics.get("imbalance", metrics.get("imb", 0.0)) ,
                    "imb": metrics.get("imb", metrics.get("imbalance", 0.0)),
                    "buy_ratio": metrics.get("buy_ratio", 0.5),
                    "slope30_pct": metrics.get("slope30_pct", 0.0),
                    "ret_1": metrics.get("ret_1", 0.0),
                    "ret_5": metrics.get("ret_5", 0.0),
                    "ret_15": metrics.get("ret_15", 0.0),
                    "volume": metrics.get("volume", 0.0),
                    "vol_sma20": metrics.get("vol_sma20", 0.0),
                    "volatility_1h_pct": metrics.get("volatility_1h_pct", 0.0),
                    "utc_hour": metrics.get("utc_hour", 0),
                }
    
                # 1) BTC канал обновляет shared_state['market_ctx'].
                # 2) Все каналы инжектят market_* ключи в свои features.
                try:
                    # стратегия использует cfg из config.json -> shared_state может хранить копию
                    cfg_all = (self.shared_state or {}).get('cfg') if isinstance(self.shared_state, dict) else None
                    # ВАЖНО: стратегия и market-regime должны видеть trading-конфиг (а не только strategy.params),
                    # иначе per-symbol thresholds и параметры v3 не попадают в расчёты.
                    cfg_params = {}
                    try:
                        if isinstance(cfg_all, dict):
                            tcfg = (cfg_all.get('trading') or {}) if isinstance(cfg_all.get('trading'), dict) else {}
                            sp = ((cfg_all.get('strategy') or {}).get('params') or {}) if isinstance((cfg_all.get('strategy') or {}).get('params'), dict) else {}
                            cfg_params = dict(tcfg)
                            cfg_params.update(sp)  # локальные параметры стратегии поверх trading
                    except Exception:
                        cfg_params = {}
                    update_shared_market(self.shared_state, symbol=self.symbol, features=features, cfg=cfg_params)
                    inject_market_into_features(self.shared_state, features)
                except Exception as e:
                    self._safe_put({"type":"warn","symbol":self.symbol,"warn":f"market regime: {e}"})
    
                decision = {}
                try:
                    shared_thr = (self.shared_state or {}) if isinstance(self.shared_state, dict) else {}
                    try:
                        min_buy = float(shared_thr.get("v3_buy_score_min", 0.70) or 0.70)
                    except Exception:
                        min_buy = 0.70
                    try:
                        min_sell = float(shared_thr.get("v3_sell_score_min", 0.65) or 0.65)
                    except Exception:
                        min_sell = 0.65
    
                    cfg_params_eff = dict(cfg_params or {})
                    # Передаём пороги из UI в стратегию (иначе стратегия может использовать более жёсткие дефолты)
    
                    # v3: per-symbol thresholds (основной источник порогов)
                    try:
                        pst = shared_thr.get("per_symbol_thresholds")
                        if isinstance(pst, dict):
                            cfg_params_eff["per_symbol_thresholds"] = pst
                    except Exception:
                        pass
    
                    # v3: пробрасываем важные параметры из config.trading в стратегию.
                    # Это критично для динамических порогов/режимов (TREND5/TREND1/MR/BREAKOUT).
                    try:
                        _cfg = (shared_thr.get('cfg') or {}) if isinstance(shared_thr, dict) else {}
                        tcfg_v3 = (_cfg.get('trading') or {}) if isinstance(_cfg, dict) else {}
                        if isinstance(tcfg_v3, dict):
                            allow_keys = [
                                'v3_buy_score_min','v3_sell_score_min','v3_buy_ratio_min','v3_atr_max_pct','v3_ret5_min_pct',
                                'v3_rsi_buy_trend_max','v3_rsi_extreme_block','micro_profit_take_net_pct',
                                'max_spread_buy_pct','max_lag_buy_sec','market_ref_symbol'
                            ]
                            for k in allow_keys:
                                if k in tcfg_v3:
                                    cfg_params_eff[k] = tcfg_v3.get(k)
                    except Exception:
                        pass
                    try:
                        _tcfg = (((self.shared_state or {}).get("cfg") or {}).get("trading") or {}) if isinstance((self.shared_state or {}).get("cfg"), dict) else ((self.shared_state or {}).get("trading") or {})
                        cfg_params_eff["max_positions"] = int(float(_tcfg.get("max_positions", 1) or 1))
                    except Exception:
                        cfg_params_eff["max_positions"] = 1
    
                    try:
                        mp = shared_thr.get("micro_profit_enabled", None)
                        if mp is None:
                            # fallback: читаем прямо из cfg (если runtime apply не делали)
                            try:
                                _cfg = (shared_thr.get('cfg') or {}) if isinstance(shared_thr, dict) else {}
                                mp = bool(((_cfg.get('trading') or {}).get('micro_profit_enabled', False)))
                            except Exception:
                                mp = False
                        cfg_params_eff["micro_profit_enabled"] = bool(mp)
                    except Exception:
                        cfg_params_eff["micro_profit_enabled"] = bool(mp)
    
                    decision = self.strategy.decide(features=features, position=position_live, portfolio_state=pstate, cfg=cfg_params_eff)
                except Exception as e:
                    self._safe_put({"type":"error", "symbol": self.symbol, "error": f"strategy: {e}"})
                    decision = {"action":"HOLD", "confidence":0.0, "reason":"STRATEGY_ERROR", "meta":{"error": str(e)[:200]}}
    
                try:
                    if (decision or {}).get('action') == 'BUY':
                        tcfg = (((self.shared_state or {}).get('cfg') or {}).get('trading') or {}) if isinstance((self.shared_state or {}).get('cfg'), dict) else ((self.shared_state or {}).get('trading') or {})
                        try:
                            max_pos = int(float(tcfg.get('max_positions', 1) or 1))
                        except Exception:
                            max_pos = 1
                        if max_pos > 0:
                            try:
                                allow_exceed = bool(tcfg.get('allow_exceed_max_positions', False)) and float((decision or {}).get('confidence', 0.0) or 0.0) >= float(tcfg.get('exceed_max_positions_score', 0.95) or 0.95)
                            except Exception:
                                allow_exceed = False
                            open_cnt = int((pstate or {}).get('positions_count', 0) or 0)
                            pending_cnt = int((pstate or {}).get('pending_orders', 0) or 0)
                            if (open_cnt + pending_cnt) >= max_pos and (not allow_exceed):
                                decision = dict(decision or {})
                                decision['action'] = 'HOLD'
                                decision['reason'] = (str(decision.get('reason') or '') + ' / MAX_POSITIONS').strip(' /')
                                meta0 = dict((decision.get('meta') or {}) if isinstance(decision.get('meta'), dict) else {})
                                meta0['blocked'] = 'max_positions'
                                decision['meta'] = meta0
                except Exception:
                    pass
    
    
    
                # Force SELL when:
                # 1) position had positive peak, then retraced and now price is falling (micro-profit protection)
                # 2) position is stuck too long and it is a weak trade (timeout exit)
                try:
                    shared = (self.shared_state or {})
                    base_qty = float((position or {}).get('base_qty', 0.0) or 0.0)
                    if base_qty > 0.0:
                        # last open lot (for entry confidence and net pnl estimate)
                        tr = None
                        entry_conf = 0.0
                        try:
                            tr = getattr(self.portfolio, 'last_open_trade', lambda _s: None)(self.symbol)
                            if tr is not None:
                                entry_conf = float(getattr(tr, 'buy_score', 0.0) or 0.0)
                        except Exception:
                            tr = None
                            entry_conf = 0.0
    
                        # fee rate (best effort)
                        fee_rate = 0.001
                        try:
                            fr_map = (shared.get('fee_rate_by_symbol', {}) or {})
                            if isinstance(fr_map, dict) and self.symbol in fr_map:
                                fee_rate = float(fr_map.get(self.symbol) or fee_rate)
                        except Exception:
                            pass
                        try:
                            cfg = shared.get('cfg') or {}
                            fee_rate = float(((cfg.get('trading') or {}).get('fee_rate')) or fee_rate)
                        except Exception:
                            pass
    
                        last_px = float(metrics.get('best_bid') or metrics.get('last') or 0.0)
    
                        pnl_net = 0.0
                        pnl_pct = 0.0
                        try:
                            if tr is not None:
                                pnl_net, pnl_pct = tr.est_pnl_now(last_px=last_px, fee_rate=fee_rate)
                        except Exception:
                            pnl_net, pnl_pct = 0.0, 0.0
    
                        try:
                            accept_pos = 0.0
                            accept_neg = -0.35
                            cfg = (shared.get('cfg') or {})
                            scfg = (cfg.get('strategy') or {})
                            a = (scfg.get('analytics') or {}) if isinstance(scfg.get('analytics'), dict) else {}
                            if isinstance(a, dict):
                                accept_pos = float(a.get('accept_pos_pct', accept_pos) or accept_pos)
                                accept_neg = float(a.get('accept_neg_pct', accept_neg) or accept_neg)
                            if tr is not None:
                                self.portfolio.update_open_trade_analytics(
                                    symbol=self.symbol,
                                    trade_id=str(getattr(tr, 'trade_id', '') or ''),
                                    pnl_net_usd=float(pnl_net or 0.0),
                                    pnl_net_pct=float(pnl_pct or 0.0),
                                    now_ts=time.time(),
                                    accept_pos_pct=accept_pos,
                                    accept_neg_pct=accept_neg,
                                )
                        except Exception:
                            pass
    
                        # ------------------------------------------------------------------
                        # и режут "лотерею".
                        # 1) Реальный hard-stop на уровне движка (force SELL).
                        # 2) Fail-fast: отрицательный пик / нет прогресса.
                        # 3) Ловим первые пики: частичные тейки (TP1/TP2) + остаток ведём как раньше.
                        # ------------------------------------------------------------------
                        try:
                            cfg0 = (shared.get('cfg') or {})
                            tcfg0 = (cfg0.get('trading') or {}) if isinstance(cfg0.get('trading'), dict) else {}
    
                            # percent points helpers
                            pnl_pp = float(pnl_pct or 0.0) * 100.0
                            hold_s = float((position or {}).get('holding_sec', holding_sec) or holding_sec)
    
                            hard_stop = float(tcfg0.get('hard_stop_loss_pct', 1.20) or 1.20)
                            hard_force = bool(tcfg0.get('hard_stop_force_enabled', True))
                            min_hard_hold = float(tcfg0.get('min_hold_for_hard_stop_sec', 12) or 12)
                            hard_emerg_pp = float(tcfg0.get('hard_stop_emergency_pp', -2.5) or -2.5)
                            if hard_force and ((hold_s >= min_hard_hold) or (pnl_pp <= hard_emerg_pp)) and pnl_pp <= (-abs(hard_stop)):
                                meta = dict((decision or {}).get('meta') or {})
                                meta['force_exit'] = True
                                meta['force_exit_kind'] = 'STOP_LOSS'
                                meta['net_pnl_usd'] = float(pnl_net)
                                meta['net_pnl_pct'] = float(pnl_pct)
                                decision = dict(decision or {})
                                decision['action'] = 'SELL'
                                decision['confidence'] = max(float(decision.get('confidence') or 0.0), 0.99)
                                decision['reason'] = 'HARD_STOP'
                                decision['meta'] = meta
    
                            # Fail-fast: negative peak (пик <= 0 в первые N секунд и уже ушли в минус)
                            neg_enabled = bool(tcfg0.get('neg_peak_exit_enabled', False))
                            neg_window = float(tcfg0.get('neg_peak_window_sec', 240) or 240)
                            neg_cur_pp = float(tcfg0.get('neg_peak_cur_net_pp', -0.45) or -0.45)
                            min_failfast_hold = float(tcfg0.get('min_hold_for_failfast_sec', 25) or 25)
                            if neg_enabled and tr is not None and hold_s <= neg_window and hold_s >= min_failfast_hold:
                                mx_pp = float(getattr(tr, 'max_net_pnl_pct', 0.0) or 0.0)
                                if mx_pp <= 0.0 and pnl_pp <= float(neg_cur_pp):
                                    meta = dict((decision or {}).get('meta') or {})
                                    meta['force_exit'] = True
                                    meta['force_exit_kind'] = 'NEG_PEAK'
                                    meta['net_pnl_usd'] = float(pnl_net)
                                    meta['net_pnl_pct'] = float(pnl_pct)
                                    meta['max_net_pnl_pp'] = float(mx_pp)
                                    decision = dict(decision or {})
                                    decision['action'] = 'SELL'
                                    decision['confidence'] = max(float(decision.get('confidence') or 0.0), 0.95)
                                    decision['reason'] = 'NEG_PEAK_FAILFAST'

                            # Profit-lock: если достигали целевой прибыли, не даём ей превратиться в минус.
                            # Логика: как только max_net_pnl_pp >= arm_pp, включаем трейлинг от пика.
                            try:
                                pl_enabled = bool(tcfg0.get('profit_lock_enabled', True))
                            except Exception:
                                pl_enabled = True
                            try:
                                arm_pp = float(tcfg0.get('profit_lock_arm_pp', 0.25) or 0.25)  # активировать с +0.25%
                                trail_pp = float(tcfg0.get('profit_lock_trail_pp', 0.18) or 0.18)  # откат от пика
                                min_keep_pp = float(tcfg0.get('profit_lock_min_keep_pp', 0.05) or 0.05)  # хотим выйти хотя бы +0.05%
                            except Exception:
                                arm_pp, trail_pp, min_keep_pp = 0.25, 0.18, 0.05

                            if pl_enabled and tr is not None:
                                mx_pp = float(getattr(tr, 'max_net_pnl_pct', 0.0) or 0.0) * 100.0
                                cur_pp = float(pnl_pp)
                                # если уже ловили хороший пик — защищаемся
                                if mx_pp >= arm_pp:
                                    # 1) если откат от пика превысил trail_pp и ещё есть шанс закрыться в плюс
                                    if (mx_pp - cur_pp) >= trail_pp and cur_pp >= min_keep_pp:
                                        meta = dict((decision or {}).get('meta') or {})
                                        meta['force_exit'] = True
                                        meta['force_exit_kind'] = 'PROFIT_LOCK'
                                        meta['net_pnl_usd'] = float(pnl_net)
                                        meta['net_pnl_pct'] = float(pnl_pct)
                                        meta['max_net_pnl_pp'] = float(mx_pp)
                                        decision = dict(decision or {})
                                        decision['action'] = 'SELL'
                                        decision['confidence'] = max(float(decision.get('confidence') or 0.0), 0.97)
                                        decision['reason'] = 'PROFIT_LOCK_TRAIL'
                                        decision['meta'] = meta
                                    # 2) если пик был, но мы почти вернулись к нулю — лучше закрыть около нуля, чем уйти в минус
                                    elif cur_pp < min_keep_pp and cur_pp > -0.10:
                                        meta = dict((decision or {}).get('meta') or {})
                                        meta['force_exit'] = True
                                        meta['force_exit_kind'] = 'PROFIT_LOCK_BE'
                                        meta['net_pnl_usd'] = float(pnl_net)
                                        meta['net_pnl_pct'] = float(pnl_pct)
                                        meta['max_net_pnl_pp'] = float(mx_pp)
                                        decision = dict(decision or {})
                                        decision['action'] = 'SELL'
                                        decision['confidence'] = max(float(decision.get('confidence') or 0.0), 0.96)
                                        decision['reason'] = 'PROFIT_LOCK_BREAKEVEN'
                                        decision['meta'] = meta

                            # No-progress exit: если позиция не дала прогресса за разумное время, выходим около нуля.
                            try:
                                np_enabled = bool(tcfg0.get('no_progress_exit_enabled', True))
                            except Exception:
                                np_enabled = True
                            try:
                                np_hold = float(tcfg0.get('no_progress_hold_sec', 12*60) or 720.0)
                                np_min_peak = float(tcfg0.get('no_progress_min_peak_pp', 0.12) or 0.12)  # пик меньше +0.12% => прогресса не было
                                np_band_lo = float(tcfg0.get('no_progress_band_lo_pp', -0.08) or -0.08)
                                np_band_hi = float(tcfg0.get('no_progress_band_hi_pp', 0.08) or 0.08)
                            except Exception:
                                np_hold, np_min_peak, np_band_lo, np_band_hi = 720.0, 0.12, -0.08, 0.08

                            if np_enabled and tr is not None and hold_s >= np_hold:
                                mx_pp = float(getattr(tr, 'max_net_pnl_pct', 0.0) or 0.0) * 100.0
                                cur_pp = float(pnl_pp)
                                if mx_pp < np_min_peak and (np_band_lo <= cur_pp <= np_band_hi):
                                    meta = dict((decision or {}).get('meta') or {})
                                    meta['force_exit'] = True
                                    meta['force_exit_kind'] = 'NO_PROGRESS'
                                    meta['net_pnl_usd'] = float(pnl_net)
                                    meta['net_pnl_pct'] = float(pnl_pct)
                                    meta['max_net_pnl_pp'] = float(mx_pp)
                                    decision = dict(decision or {})
                                    decision['action'] = 'SELL'
                                    decision['confidence'] = max(float(decision.get('confidence') or 0.0), 0.90)
                                    decision['reason'] = 'NO_PROGRESS_TIMEOUT'
                                    decision['meta'] = meta
                                    decision['meta'] = meta
    
                            # Fail-fast: no progress (через N минут пик так и не стал "зелёным")
                            np_enabled = bool(tcfg0.get('no_progress_hard_exit_enabled', False))
                            np_window = float(tcfg0.get('no_progress_window_sec', 600) or 600)
                            np_need_pp = float(tcfg0.get('no_progress_need_peak_pp', 0.12) or 0.12)
                            if np_enabled and tr is not None and hold_s >= np_window:
                                mx_pp = float(getattr(tr, 'max_net_pnl_pct', 0.0) or 0.0)
                                if mx_pp < float(np_need_pp):
                                    meta = dict((decision or {}).get('meta') or {})
                                    meta['force_exit'] = True
                                    meta['force_exit_kind'] = 'NO_PROGRESS'
                                    meta['net_pnl_usd'] = float(pnl_net)
                                    meta['net_pnl_pct'] = float(pnl_pct)
                                    meta['max_net_pnl_pp'] = float(mx_pp)
                                    decision = dict(decision or {})
                                    decision['action'] = 'SELL'
                                    decision['confidence'] = max(float(decision.get('confidence') or 0.0), 0.92)
                                    decision['reason'] = 'NO_PROGRESS_HARD'
                                    decision['meta'] = meta
    
                            # Partial take-profit on first peaks (TP1/TP2)
                            ptp_enabled = bool(tcfg0.get('partial_tp_enabled', True))
                            if ptp_enabled and tr is not None and base_qty > 0.0:
                                tp1 = float(tcfg0.get('partial_tp1_net_pct', 0.0030) or 0.0030)
                                tp2 = float(tcfg0.get('partial_tp2_net_pct', 0.0060) or 0.0060)
                                f1 = float(tcfg0.get('partial_tp1_fraction', 0.45) or 0.45)
                                f2 = float(tcfg0.get('partial_tp2_fraction', 0.30) or 0.30)
                                min_rem = float(tcfg0.get('partial_tp_min_remain_fraction', 0.20) or 0.20)
    
                                done1 = bool(getattr(tr, 'tp1_done', False))
                                done2 = bool(getattr(tr, 'tp2_done', False))
    
                                # TP2 имеет приоритет (если TP1 уже сделан)
                                want_frac = 0.0
                                if (not done1) and pnl_pct >= tp1:
                                    want_frac = float(f1)
                                    setattr(tr, 'tp1_done', True)
                                elif done1 and (not done2) and pnl_pct >= tp2:
                                    want_frac = float(f2)
                                    setattr(tr, 'tp2_done', True)
    
                                if want_frac > 0.0:
                                    # не продаём, если останется слишком мало (иначе пыль/мин-ордер)
                                    if (1.0 - want_frac) >= float(min_rem):
                                        meta = dict((decision or {}).get('meta') or {})
                                        meta['force_exit'] = True
                                        meta['force_exit_kind'] = 'PARTIAL_TP'
                                        meta['sell_fraction'] = float(want_frac)
                                        meta['net_pnl_usd'] = float(pnl_net)
                                        meta['net_pnl_pct'] = float(pnl_pct)
                                        decision = dict(decision or {})
                                        decision['action'] = 'SELL'
                                        decision['confidence'] = max(float(decision.get('confidence') or 0.0), 0.93)
                                        decision['reason'] = 'PARTIAL_TP'
                                        decision['meta'] = meta
    
                                        # persist flags (best effort)
                                        try:
                                            self.portfolio._save_ledger_safe()
                                        except Exception:
                                            pass
                        except Exception:
                            pass
    
                        # 1) Micro-profit protection (engine-level)
                        # Теперь micro-profit управляется стратегией (настраиваемый порог + умная лестница).
                        mp_enabled = bool(shared.get('micro_profit_engine_guard', False))
                        if mp_enabled:
                            peak_thr = float(shared.get('micro_profit_peak_pct', 0.15) or 0.15)
                            retr_thr = float(shared.get('micro_profit_retrace_pct', 0.10) or 0.10)
                            min_net = float(shared.get('micro_profit_min_net_usd', 0.0) or 0.0)
    
                            entry_px = float((position or {}).get('entry_price', 0.0) or 0.0)
                            peak_px = float((position or {}).get('peak_price', 0.0) or 0.0)
                            falling = (float(prev_last or 0.0) > 0.0 and last_px > 0.0 and last_px < float(prev_last))
    
                            if entry_px > 0.0 and peak_px > 0.0 and last_px > 0.0:
                                peak_gain_pct = ((peak_px - entry_px) / entry_px) * 100.0
                                retr_from_peak_pct = ((peak_px - last_px) / peak_px) * 100.0
                                if peak_gain_pct >= peak_thr and retr_from_peak_pct >= retr_thr and falling and pnl_net >= min_net:
                                    meta = dict((decision or {}).get('meta') or {})
                                    meta['force_exit'] = True
                                    meta['force_exit_kind'] = 'MICRO_PROFIT'
                                    meta['net_pnl_usd'] = float(pnl_net)
                                    meta['net_pnl_pct'] = float(pnl_pct)
                                    decision = dict(decision or {})
                                    decision['action'] = 'SELL'
                                    decision['confidence'] = max(float(decision.get('confidence') or 0.0), 0.99)
                                    decision['reason'] = 'MICRO_PROFIT'
                                    decision['meta'] = meta
    
                        # 2) Timeout exit (only for weak trades)
    
                        timeout_enabled = bool(tcfg0.get('timeout_exit_enabled', False))
                        timeout_sec = float(shared.get('trade_timeout_sec', 15.0 * 60.0) or (15.0 * 60.0))
                        strong_conf = float(shared.get('strong_trade_conf', 0.90) or 0.90)
                        timeout_max_loss_pct = float(shared.get('timeout_exit_max_loss_pct', 0.35) or 0.35)
                        timeout_require_positive = bool(shared.get('timeout_exit_require_positive', False))
    
                        try:
                            holding_sec = float((position or {}).get('holding_sec', 0.0) or 0.0)
                        except Exception:
                            holding_sec = 0.0
    
                        if timeout_enabled and timeout_sec > 0 and holding_sec > timeout_sec and entry_conf < strong_conf:
                            if timeout_require_positive:
                                ok_loss = (pnl_net >= 0.0)
                            else:
                                ok_loss = (pnl_pct >= (-abs(timeout_max_loss_pct) / 100.0))
                            if ok_loss:
                                meta = dict((decision or {}).get('meta') or {})
                                meta['force_exit'] = True
                                meta['force_exit_kind'] = 'TIMEOUT'
                                meta['entry_conf'] = float(entry_conf)
                                meta['net_pnl_usd'] = float(pnl_net)
                                meta['net_pnl_pct'] = float(pnl_pct)
                                decision = dict(decision or {})
                                decision['action'] = 'SELL'
                                decision['confidence'] = max(float(decision.get('confidence') or 0.0), 0.90)
                                decision['reason'] = 'TIMEOUT'
                                decision['meta'] = meta
    
                        # Не даём закрывать позицию в первые N секунд, если это не аварийный стоп.
                        try:
                            min_hold_exit = float(shared.get('min_exit_hold_sec', 30.0) or 30.0)
                        except Exception:
                            min_hold_exit = 30.0
                        try:
                            hard_stop = float(shared.get('hard_stop_loss_pct', 1.20) or 1.20)
                        except Exception:
                            hard_stop = 1.20
                        try:
                            holding_sec2 = float((position or {}).get('holding_sec', holding_sec) or holding_sec)
                        except Exception:
                            holding_sec2 = holding_sec
    
                        try:
                            if holding_sec2 < min_hold_exit and (decision or {}).get('action') == 'SELL':
                                meta_x = (decision or {}).get('meta') or {}
                                force_x = bool((decision or {}).get('force_exit') or (meta_x.get('force_exit') if isinstance(meta_x, dict) else False))
                                # если это принудительный выход (hard-stop / partial-tp / fail-fast) — не блокируем
                                if (not force_x) and float(pnl_pct or 0.0) > (-abs(hard_stop) / 100.0):
                                    decision = dict(decision or {})
                                    decision['action'] = 'HOLD'
                                    decision['reason'] = (str(decision.get('reason') or '') + ' / MIN_HOLD').strip(' /')
                                    meta0 = dict((decision.get('meta') or {}) if isinstance(decision.get('meta'), dict) else {})
                                    meta0['blocked'] = 'min_hold_exit'
                                    meta0['holding_sec'] = float(holding_sec2)
                                    meta0['min_exit_hold_sec'] = float(min_hold_exit)
                                    meta0['net_pnl_pct'] = float(pnl_pct)
                                    decision['meta'] = meta0
                        except Exception:
                            pass
                except Exception:
                    pass
    
                # --- Применяем пороги/ограничения UI к решению стратегии (для отображения и автоторговли) ---
                try:
                    shared = (self.shared_state or {})
                    now_ts = time.time()
                    warmup_until = float(shared.get("warmup_until", 0.0) or 0.0)
                    try:
                        wb = shared.get("warmup_by_symbol", {}) or {}
                        if isinstance(wb, dict):
                            warmup_until = max(warmup_until, float(wb.get(self.symbol, 0.0) or 0.0))
                    except Exception:
                        pass
                    legacy_cd = float(shared.get("cooldown_sec", 10.0) or 10.0)
                    buy_cooldown_sec = float(shared.get("buy_cooldown_sec", legacy_cd) or legacy_cd)
                    sell_cooldown_sec = float(shared.get("sell_cooldown_sec", legacy_cd) or legacy_cd)
    
                    # подтверждение сигнала N тиков подряд (anti-noise)
                    buy_confirm = int(shared.get("buy_confirm_ticks", 4) or 4)
                    sell_confirm = int(shared.get("sell_confirm_ticks", 3) or 3)
    
                    min_buy = float(shared.get("v3_buy_score_min", 0.70) or 0.70)
                    min_sell = float(shared.get("v3_sell_score_min", 0.65) or 0.65)
    
                    
                    # сохраняем исходное решение стратегии (до UI-фильтров)
                    decision_raw = dict(decision or {})
    
                    meta = (decision or {}).get("meta") or {}
                    # ВАЖНО: v3 стратегия возвращает score в decision['confidence'] (совместимость).
                    score = float((decision or {}).get('confidence') or 0.0)
    
                    force_exit = bool((decision or {}).get("force_exit") or (meta or {}).get("force_exit"))
                    force_exit_kind = str((decision or {}).get("force_exit_kind") or (meta or {}).get("force_exit_kind") or "")
    
                    hint = str((decision or {}).get("action") or "HOLD").upper()
                    if hint not in ("BUY", "SELL"):
                        hint = "HOLD"
    
    
                    # reset streaks when no eligible signal
                    if hint != "BUY":
                        self._v3_buy_streak = 0
                    if hint != "SELL":
                        self._v3_sell_streak = 0
    
                    reason_ui = ""
                    action_ui = "HOLD"
    
                    # Доп. защита: если цена устарела (lag) — BUY блокируем (SELL не блокируем).
                    try:
                        _lts = float(last_ts or 0.0)
                        lag_sec = (now_ts - _lts) if _lts > 0 else 9999.0
                    except Exception:
                        lag_sec = 9999.0
                    try:
                        max_lag_buy_sec = float(shared.get("max_lag_buy_sec", 10.0) or 10.0)
                    except Exception:
                        max_lag_buy_sec = 10.0
    
                    try:
                        spread_pct = float(metrics.get("spread_pct") or 0.0)
                    except Exception:
                        spread_pct = 0.0
    
                    # warmup (важно: SELL/выходы не блокируем warmup’ом)
                    if hint == "BUY" and warmup_until and now_ts < warmup_until:
                        reason_ui = "WARMUP"
                    elif hint == "BUY" and lag_sec > max_lag_buy_sec:
                        reason_ui = f"LAG>{int(max_lag_buy_sec)}s"
                    else:
                        try:
                            spread_pct = float(metrics.get("spread_pct") or 0.0)
                        except Exception:
                            spread_pct = 0.0
                        try:
                            max_spread = float(shared.get("max_spread_buy_pct", 0.25) or 0.25)
                        except Exception:
                            max_spread = 0.25
    
                        # Адаптивный потолок спреда для BUY: при волатильном рынке даём небольшой люфт,
                        # но ограничиваем его сверху, чтобы не покупать в неликвиде.
                        try:
                            adaptive_on = bool(shared.get("adaptive_spread_enabled", True))
                        except Exception:
                            adaptive_on = True
                        if hint == "BUY" and adaptive_on:
                            try:
                                atrp = float(metrics.get("atrp") or 0.0)  # ATR% за 14
                            except Exception:
                                atrp = 0.0
                            try:
                                volr = float(metrics.get("vol_ratio") or metrics.get("volr") or 0.0)
                            except Exception:
                                volr = 0.0
                            spread_bonus = max(0.0, min(0.12, atrp * 0.06))
                            if volr >= 1.25:
                                spread_bonus += 0.02
                            max_spread = min(max_spread * 1.8, max_spread + spread_bonus)
    
    
                        if hint == "BUY" and max_spread > 0 and spread_pct > max_spread:
                            # Исторически это отображалось как SPREAD, но пользователю важнее понимать,
                            # что это именно блок по ликвидности/спреду.
                            # Оставляем короткий код для таблицы и пишем детали в decision-поля.
                            reason_ui = "BLOCK_LIQ"
                        else:
                            # cooldown по символу: BUY/SSELL раздельно
                            last_sig = float((self.portfolio.last_signal_ts or {}).get(self.symbol) or 0.0)
                            cd_use = buy_cooldown_sec if hint == "BUY" else sell_cooldown_sec
                            if last_sig and (now_ts - last_sig) < cd_use and not (hint == "SELL" and force_exit):
                                reason_ui = "COOLDOWN"
                            else:
                                # Доп. защита re-entry: после SELL даём рынку остыть, иначе будет churn
                                try:
                                    reentry_cd = float(((shared.get('cfg') or {}).get('trading') or {}).get('reentry_cooldown_sec') or 180.0)
                                except Exception:
                                    reentry_cd = 180.0
                                try:
                                    last_exit = float((getattr(self.portfolio, 'last_exit_ts', {}) or {}).get(self.symbol) or 0.0)
                                except Exception:
                                    last_exit = 0.0
                                if hint == 'BUY' and last_exit and (now_ts - last_exit) < reentry_cd:
                                    reason_ui = 'COOLDOWN'
                                else:
                                    # Market regime (BTC proxy): в risk-off блокируем новые BUY.
                                    try:
                                        market_block_buy = bool(metrics.get("market_block_buy") or features.get("market_block_buy"))
                                    except Exception:
                                        market_block_buy = False
    
                                    if hint == "BUY":
                                        if float(position.get("base_qty", 0.0) or 0.0) > 0.0:
                                            reason_ui = "HAVE_POS"
                                        elif market_block_buy:
                                            reason_ui = "MARKET_RISK_OFF"
                                        elif (score >= min_buy):
                                            # анти-шум: сигнал должен держаться N тиков подряд
                                            self._v3_buy_streak = int(getattr(self, "_v3_buy_streak", 0) or 0) + 1
                                            if self._v3_buy_streak >= int(max(1, buy_confirm)):
                                                action_ui = "BUY"
                                                reason_ui = "OK"
                                            else:
                                                action_ui = "HOLD"
                                                reason_ui = f"CONFIRM {self._v3_buy_streak}/" + str(max(1, buy_confirm))
    
                                        else:
                                            reason_ui = "LOW_SCORE"
                                            self._v3_sell_streak = 0
                                            self._v3_buy_streak = 0
                                    elif hint == "SELL":
                                        if float(position.get("base_qty", 0.0) or 0.0) <= 0.0:
                                            reason_ui = "NO_POS"
                                        # FORCE EXIT: выходы по стопу/защите прибыли должны проходить даже при score < min_sell
                                        elif force_exit:
                                            action_ui = "SELL"
                                            _k = (force_exit_kind or "").lower()
                                            if _k in ("stop_loss", "sl"):
                                                reason_ui = "EXIT_SL"
                                            elif _k.startswith("take_profit") or _k in ("tp", "tp_fast"):
                                                reason_ui = "EXIT_TP"
                                            elif _k in ("profit_lock", "profit_floor", "break_even"):
                                                reason_ui = "EXIT_LOCK"
                                            elif _k in ("micro_profit", "micro"):
                                                reason_ui = "EXIT_MP"
                                            elif _k in ("timeout",):
                                                reason_ui = "EXIT_TO"
                                            else:
                                                reason_ui = "EXIT"
                                        elif (score >= min_sell):
                                            self._v3_sell_streak = int(getattr(self, "_v3_sell_streak", 0) or 0) + 1
                                            if self._v3_sell_streak >= int(max(1, sell_confirm)):
                                                action_ui = "SELL"
                                                reason_ui = "OK"
                                            else:
                                                action_ui = "HOLD"
                                                reason_ui = f"CONFIRM {self._v3_sell_streak}/" + str(max(1, sell_confirm))
    
                                        else:
                                            reason_ui = "LOW_SCORE"
                                    else:
                                        # HOLD: если стратегия вернула явную причину блокировки — показываем её.
                                        # Иначе — оставляем старый fallback.
                                        dr = str((decision or {}).get('reason') or '').strip().upper()
                                        if dr.startswith('BLOCK_'):
                                            reason_ui = dr
                                        elif 'SPREAD_BLOCK' in dr or dr.startswith('SPREAD_BLOCK'):
                                            reason_ui = 'BLOCK_LIQ'
                                        elif dr.startswith('LAG_BLOCK'):
                                            reason_ui = 'LAG>' + str(int(max_lag_buy_sec)) + 's'
                                        else:
                                            # legacy: meta.blocked (если вдруг заполнен)
                                            b = str((meta or {}).get('blocked') or '')
                                            if b:
                                                _map = {
                                                    'anti_chase': 'AN',
                                                    'falling_knife': 'FK',
                                                    'max_atr': 'ATR',
                                                }
                                                bb = _map.get(b, b).upper()
                                                reason_ui = f'BLOCK_{bb}'
                                            else:
                                                # По умолчанию не переопределяем причину стратегии.
                                                # Если оставить NO_SIGNAL, то даже WARMUP/TP_NET будут теряться в UI.
                                                reason_ui = ''
    
                    # обогащаем decision для UI
                    decision = dict(decision or {})
                    decision["action_ui"] = action_ui
                    base_reason = str(decision.get("reason") or "").strip()
                    token = (base_reason.split()[0] if base_reason else "")
                    # если UI-оверрайд установлен (например LAG_BLOCK/BLOCK_LIQ/EXEC_BLOCK) — используем его
                    if reason_ui:
                        reason_code = str(reason_ui)
                    else:
                        reason_code = ("STRATEGY_ERR" if token=="STRATEGY_ERROR" else (token or "NO_SIGNAL"))
                    decision["reason_code"] = reason_code
                    decision["reason_ui"] = _reason_ru_short(str(reason_code), str(decision.get("reason") or ""))
    
                    # Диагностика ликвидности: когда BUY блокируется из-за спреда,
                    # записываем детали, чтобы потом было видно, почему по большинству альтов
                    # висит BLOCK_LIQ.
                    if reason_ui == "BLOCK_LIQ":
                        try:
                            decision["liq_spread_pct"] = float(spread_pct)
                        except Exception:
                            decision["liq_spread_pct"] = 0.0
                        try:
                            decision["liq_max_spread_pct"] = float(max_spread)
                        except Exception:
                            decision["liq_max_spread_pct"] = 0.0
                        try:
                            decision["liq_best_bid"] = float(metrics.get("best_bid") or metrics.get("bid") or 0.0)
                        except Exception:
                            decision["liq_best_bid"] = 0.0
                        try:
                            decision["liq_best_ask"] = float(metrics.get("best_ask") or metrics.get("ask") or 0.0)
                        except Exception:
                            decision["liq_best_ask"] = 0.0
                    # защитимся от NaN/inf и пробросим score для UI
                    try:
                        _craw = float((decision or {}).get('confidence') or 0.0)
                        decision['confidence_ui'] = _craw if __import__('math').isfinite(_craw) else 0.0
                    except Exception:
                        decision['confidence_ui'] = 0.0
                    decision["force_exit"] = bool(force_exit)
                    decision["force_exit_kind"] = str(force_exit_kind)
                    try:
                        decision["spread_pct"] = float(metrics.get("spread_pct") or 0.0)
                    except Exception:
                        decision["spread_pct"] = 0.0
                    try:
                        decision["lag_sec"] = float(lag_sec)
                    except Exception:
                        decision["lag_sec"] = 9999.0
    
                    # AutoTrader может блокировать BUY из-за pending/max_pos/PRV stale/...
                    # В таких случаях UI должен показывать "КУПИТЬ⛔" и причину.
                    try:
                        eb = (shared.get('exec_block') or {})
                        if isinstance(eb, dict) and str(decision.get('action_ui') or '').upper() == 'BUY':
                            rec = eb.get(self.symbol)
                            if isinstance(rec, dict):
                                tsb = float(rec.get('ts') or 0.0)
                                if tsb > 0 and (time.time() - tsb) <= 6.0:
                                    codeb = str(rec.get('code') or 'EXEC_BLOCK')
                                    detb = str(rec.get('detail') or '')
                                    decision['exec_block_code'] = codeb
                                    decision['exec_block_detail'] = detb
                                    decision['action_ui'] = 'BUY_BLOCKED'
                                    decision['reason_code'] = 'EXEC_BLOCK'
                                    decision['reason_ui'] = _reason_ru_short('EXEC_BLOCK', f"{codeb} {detb}")
                    except Exception:
                        pass
    
                    try:
                        logger = shared.get('decision_logger')
                        # даже если runtime-config не был применён (UI мог просто сохранить config).
                        try:
                            _cfg = shared.get('cfg') or {}
                            _dbg = bool(((_cfg.get('trading') or {}).get('snapshots_enabled', False)))
                        except Exception:
                            _dbg = False
                        if (logger is not None) and (not _dbg):
                            logger = None
                        if logger is not None:
                            thresholds = {
                                "v3_buy_score_min": float(shared.get('v3_buy_score_min', 0.70) or 0.70),
                                "v3_sell_score_min": float(shared.get('v3_sell_score_min', 0.65) or 0.65),
                                "buy_cooldown_sec": float(buy_cooldown_sec),
                                "sell_cooldown_sec": float(sell_cooldown_sec),
                                "cooldown_sec": float(legacy_cd),
                                "warmup_until": float(warmup_until or 0.0),
                                "reason_ui": str(reason_ui),
                                "action_ui": str(action_ui),
                                "strategy_name": str(shared.get('strategy_name') or ""),
                                "strategy_params": shared.get('strategy_params') or {},
                            }
                            # сигнал = когда UI считает, что условия выполнены (OK) и действие BUY/SELL
                            try:
                                if str(action_ui).upper() == 'BUY':
                                    # Основной конфиг: 'trading'. 'trade' оставляем как legacy-fallback.
                                    cfg_root = (self.controller.config or {})
                                    tcfg = (cfg_root.get('trading') or cfg_root.get('trade') or {})
                                    max_pos = int(float(tcfg.get('max_positions', 1) or 1))
                                    if max_pos > 0:
                                        pcnt = 0
                                        try:
                                            port = msg.get('portfolio') or {}
                                            pcnt = int(port.get('positions_count') or 0)
                                        except Exception:
                                            pcnt = 0
                                        if pcnt >= max_pos:
                                            action_ui = 'HOLD'
                                            decision['action_ui'] = 'HOLD'
                                            decision['reason_ui'] = 'MAX_POS'
                                            decision['reason'] = 'BLOCK_MAX_POS'
                            except Exception:
                                pass
                            
                            is_signal = bool(action_ui in ("BUY", "SELL") and reason_ui in ("OK", "FORCE_EXIT"))
                            logger.log_tick(
                                symbol=self.symbol,
                                features=features,
                                position=position_live,
                                portfolio_state=pstate,
                                decision_raw=decision_raw,
                                decision_ui=decision,
                                thresholds=thresholds,
                                metrics=metrics,
                                is_signal=is_signal,
                            )
                    except Exception:
                        pass
    
                    try:
                        self.last_tick_ts = float(last_ts or now)
                        conf_v = float((decision or {}).get("confidence") or 0.0)
                        self.last_confidence = conf_v
                        if conf_v > 0.00001:
                            self.last_nonzero_conf_ts = float(now)
                    except Exception:
                        pass
    
                    try:
                        self.last_lag_sec = float(lag_sec) if isinstance(lag_sec, (int, float)) else 9999.0
                    except Exception:
                        self.last_lag_sec = 9999.0
                    try:
                        # keep timestamps of lag spikes to detect "repeated lag" patterns
                        lag_thr = float(shared.get("lag_swap_sec", 5.0) or 5.0)
                        lag_hits = int(shared.get("lag_swap_hits", 3) or 3)
                        lag_win = float(shared.get("lag_swap_window_sec", 30.0) or 30.0)
                        if not hasattr(self, "_lag_hit_ts"):
                            self._lag_hit_ts = []
                        # purge old
                        now2 = float(now)
                        self._lag_hit_ts = [t for t in (self._lag_hit_ts or []) if (now2 - float(t)) <= lag_win]
                        if float(self.last_lag_sec) >= lag_thr:
                            self._lag_hit_ts.append(now2)
                        self.lag_over_window_hits = len(self._lag_hit_ts)
                        self.lag_over_window_target = int(lag_hits)
                    except Exception:
                        self.lag_over_window_hits = 0
                        self.lag_over_window_target = 3
    
                    # --- Диагностика подготовки BUY (кратко) ---
                    # Требование: когда "загорелся" 3-й прямоугольник (>=3/4), но BUY не выдан,
                    # логируем раз в N секунд: что ОК и что НЕ ОК (без снапшотов и метрик).
                    try:
                        if float(position.get('base_qty', 0.0) or 0.0) <= 0.0:
                            meta_e = (decision or {}).get('meta') or {}
                            bo = int(meta_e.get('buy_ok') or 0) if isinstance(meta_e, dict) else 0
                            bt = int(meta_e.get('buy_total') or 0) if isinstance(meta_e, dict) else 0
                            rects = _rects_from_ratio(bo, bt)
                            try:
                                every_sec = float(shared.get('prep_log_every_sec', 5.0) or 5.0)
                            except Exception:
                                every_sec = 5.0
                            try:
                                rects_min = int(shared.get('prep_log_rects_min', 3) or 3)
                            except Exception:
                                rects_min = 3
    
                            act0 = str((decision or {}).get('action') or 'HOLD').upper()
                            # только когда прогресс высокий и стратегия не дала BUY
                            if rects >= rects_min and act0 != 'BUY':
                                ts = float(time.time())
                                passed = []
                                failed = []
                                try:
                                    passed = list(meta_e.get('buy_passed') or meta_e.get('buy_checks') or [])
                                except Exception:
                                    passed = []
                                try:
                                    failed = list(meta_e.get('buy_failed') or [])
                                except Exception:
                                    failed = []
                                # ограничим, чтобы лог был коротким
                                passed = [str(x) for x in passed][:12]
                                failed = [str(x) for x in failed][:12]
    
                                reason_s = str((decision or {}).get('reason') or '')
                                reason_ui_s = str((decision or {}).get('reason_code') or (decision or {}).get('reason_ui') or '')
    
                                # Сигнатура состояния: логируем только если что-то изменилось
                                try:
                                    sig_obj = {
                                        'rects': int(rects),
                                        'ok': f"{bo}/{bt}",
                                        'passed': passed,
                                        'failed': failed,
                                        'reason': reason_s,
                                        'reason_ui': reason_ui_s,
                                    }
                                    sig = json.dumps(sig_obj, sort_keys=True, ensure_ascii=False)
                                except Exception:
                                    sig = f"{rects}|{bo}/{bt}|{','.join(passed)}|{','.join(failed)}|{reason_s}|{reason_ui_s}"
    
                                last_sig = getattr(self, '_last_prep_buy_sig', None)
                                last_log_ts = float(getattr(self, '_last_prep_buy_log_ts', 0.0) or 0.0)
    
                                if sig != last_sig:
                                    # анти-спам: даже при изменениях не чаще, чем раз в every_sec
                                    if (ts - last_log_ts) >= float(every_sec):
                                        setattr(self, '_last_prep_buy_sig', sig)
                                        self._last_prep_buy_log_ts = ts
                                        log_event(self.portfolio.data_dir if hasattr(self, 'portfolio') else '.', {
                                            'level': 'INFO',
                                            'msg': 'prep_buy',
                                            'extra': {
                                                'symbol': self.symbol,
                                                'rects': rects,
                                                'ok': f"{bo}/{bt}",
                                                'passed': passed,
                                                'failed': failed,
                                                'reason': reason_s,
                                                'reason_ui': reason_ui_s,
                                            }
                                        })
                    except Exception:
                        pass
    
                    self._safe_put({
                        "type": "tick",
                        "symbol": self.symbol,
                        "last": metrics["last"],
                        "last_ts": last_ts,
                        "warmup_ready": bool(getattr(self, 'warmup_ready', False)),
                        "warmup_stage": str(getattr(self, 'warmup_stage', 'INIT')),
                        "metrics": metrics,
                        "decision": decision,
                        "position": position,
                        "portfolio": pstate,
                        "ts": now
                    })
    
                    # сигнал для автотрейдера (отдельная очередь, чтобы UI не тормозил)
                    if shared.get("auto_trade", False) and action_ui in ("BUY", "SELL"):
                        self._safe_put_signal({
                            "type": "signal",
                            "symbol": self.symbol,
                            "action": action_ui,
                            "confidence": float((decision or {}).get("confidence") or 0.0),
                            "last": float(metrics.get("last") or 0.0),
                            "best_bid": float(metrics.get("best_bid") or 0.0),
                            "best_ask": float(metrics.get("best_ask") or 0.0),
                            "spread_pct": float(metrics.get("spread_pct") or 0.0),
                            "lag_sec": float(lag_sec) if isinstance(lag_sec, (int, float)) else 9999.0,
                            "reason": reason_ui,
                            "force_exit": bool(force_exit),
                            "force_exit_kind": str(force_exit_kind),
                            "ts": now,
                        })
                except Exception:
                    # если обогащение упало, всё равно отправим базовый tick
                    try:
                        self._safe_put({
                            "type": "tick",
                            "symbol": self.symbol,
                            "last": metrics["last"],
                            "last_ts": last_ts,
                            "warmup_ready": bool(getattr(self, 'warmup_ready', False)),
                            "warmup_stage": str(getattr(self, 'warmup_stage', 'INIT')),
                            "metrics": metrics,
                            "decision": decision,
                            "position": position,
                            "portfolio": pstate,
                            "ts": now
                        })
                    except Exception:
                        pass
    
    
                try:
                    self._prev_last = float(last or 0.0)
    
    
                except Exception:
                    pass
    
                # loop period (по умолчанию ~0.25s, настраивается shared.metrics_loop_sec)
                dt = time.time() - t0
                try:
                    target = float((self.shared_state or {}).get("metrics_loop_sec", 0.25) or 0.25)
                except Exception:
                    target = 0.25
                sleep = max(0.05, float(target) - float(dt))
                time.sleep(sleep)
