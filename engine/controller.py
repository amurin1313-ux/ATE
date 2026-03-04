from __future__ import annotations
import time, threading, os, json
from queue import Queue
from typing import Dict, Any, List, Optional, Set

from okx.public_client import OKXPublicClient
from okx.ws_public import OKXPublicWS
from okx.ws_private import OKXPrivateWS
from okx.private_client import OKXPrivateClient

from engine.strategy_runtime import StrategyRegistry, SymbolStrategyInstance
from engine.portfolio import Portfolio
from engine.trader import Trader
from engine.auto_trader import AutoTrader
from queue import Queue
from engine.logging_utils import log_event
from engine.safe_exec import ui_warn_once
from engine.banlist import BanList
from engine.decision_logger import DecisionLogger
from engine.symbol_universe import load_symbol_universe


# ВАЖНО: Этот whitelist используется и при Auto-TOP=OFF, и при Auto-TOP=ON.
# Количество активных котировок регулируется полем auto_top_count.
# При Auto-TOP=ON дополнительно применяется проверка "живости" (ticker/инструмент live).
OKX_EMBEDDED_SYMBOLS_V2365 = [
    "BTC-USDT",
    "ETH-USDT",
    "SOL-USDT",
    "DOGE-USDT",
    "XRP-USDT",
    "OKB-USDT",
    "XAUT-USDT",
    "HYPE-USDT",
    "PEPE-USDT",
    "BNB-USDT",
    "UNI-USDT",
    "AUCTION-USDT",
    "AAVE-USDT",
    "DOT-USDT",
    "SUI-USDT",
    "FIL-USDT",
    "LTC-USDT",
    "BCH-USDT",
    "ADA-USDT",
    "MAGIC-USDT",
    "SHIB-USDT",
    "LINK-USDT",
    "TRX-USDT",
    "WLD-USDT",
    "CFX-USDT",
    "AVAX-USDT",
    "OP-USDT",
    "ICP-USDT",
    "ETC-USDT",
    "HBAR-USDT",
    "ARB-USDT",
    "SAND-USDT",
    "WIF-USDT",
    "FLOW-USDT",
    "NEAR-USDT",
    "CRV-USDT",
    "LUNA-USDT",
    "ETHFI-USDT",
    "LQTY-USDT",
    "ZK-USDT",
    "LAT-USDT",
    "YGG-USDT",
    "XLM-USDT",
    "TURBO-USDT",
    "STRK-USDT",
    "JUP-USDT",
    "PNUT-USDT",
    "ORBS-USDT",
    "YFI-USDT",
    "ATOM-USDT",
    "MEME-USDT",
    "OM-USDT",
    "FLOKI-USDT",
    "AEVO-USDT",
    "LDO-USDT",
    "ETHW-USDT",
    "FET-USDT",
    "DORA-USDT",
    "1INCH-USDT",
    "SUSHI-USDT",
    "LPT-USDT",
    "W-USDT",
    "INJ-USDT",
    "MASK-USDT",
    "RAY-USDT",
    "MET-USDT",
    "CELO-USDT",
    "ALGO-USDT",
    "LSK-USDT",
    "COMP-USDT",
    "SNT-USDT",
    "LRC-USDT",
    "IOST-USDT",
    "THETA-USDT",
    "ENJ-USDT"
]

# IMPORTANT: We now support an expanded symbol universe via data/okx_symbol_universe.txt
# (user can drop 200-300 symbols). We still keep the embedded list as fallback.

class EngineController:
    def __init__(self, data_dir: str, config: dict, ui_queue: Queue):
        self.data_dir = data_dir
        self.config = config
        self.ui_queue = ui_queue

        # separate queue for autotrade signals
        self.signal_queue: Queue = Queue(maxsize=5000)
        self.shared_state: dict = {
            "auto_trade": False,
            "warmup_until": 0.0,
            "warmup_by_symbol": {},  # per-symbol warmup

            # Глобальный — блокирует только новые BUY на короткое время,
            # чтобы не было импульсных покупок после добавления/уменьшения.
            "symbols_change_pause_until": 0.0,
            # Частный — по каждому символу (только BUY) сразу после старта канала.
            "symbol_warmup_until": {},

            # ДЕФОЛТЫ ПОД ЭТАЛОННЫЙ ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ (прибыльная версия):
            # Эти значения можно менять в настройках, они применяются на лету.
            # v3: пороги rule_score (Score), НЕ вероятность
            "v3_buy_score_min": 1.0,
            "v3_sell_score_min": 1.0,

            # BUY: минимум 90 секунд между входами по символу
            "buy_cooldown_sec": 90.0,
            # SELL: отдельный (короткий) cooldown, чтобы не зажимать выходы
            "sell_cooldown_sec": 10.0,
            # legacy (если старый UI шлёт cooldown_sec)
            "cooldown_sec": 10.0,

            "signal_ttl_sec": 3.0,
            "global_buy_throttle_sec": 4.0,
            "global_last_buy_ts": 0.0,

            "max_spread_buy_pct": 0.25,
            "max_lag_buy_sec": 10.0,

            "micro_profit_enabled": False,
            "micro_profit_peak_pct": 0.15,
            "micro_profit_retrace_pct": 0.10,
            "micro_profit_min_net_usd": 0.0,
            # Timeout-exit: ВАЖНО. Ранний timeout способен отрезать хорошие сделки,
            # которые разворачиваются через 30–90 минут (как в реальных тестах).
            # Дефолт делаем длинным и разрешаем timeout-выход только при нулевом/плюсовом
            # net PnL. Убыточные позиции закрывает SL/force (это безопаснее).
            "timeout_exit_enabled": False,
            "trade_timeout_sec": 0.0,  # отключено: раньше давало тупые продажи

            "strong_trade_conf": 0.90,
            "timeout_exit_max_loss_pct": 0.00,
            "timeout_exit_require_positive": True,


            "fee_rate_by_symbol": {},
            "fee_rate_ts": {},
            # Плавный стоп
            "smooth_stop": False,
            "smooth_stop_deadline_ts": 0.0,
            "smooth_stop_max_time": False,
        }

        # использовать strategy.params (например для market regime фильтра) без импорта UI.
        # Это не кэш «в систему» — только в памяти.
        self.shared_state["cfg"] = self.config
        # allow disabling a specific symbol (untradeable in current OKX env)
        self.shared_state["disabled_symbols"] = set()
        self.shared_state["runtime_stop_symbols"] = set()
        self.auto_trader: Optional[AutoTrader] = None

        self.public = OKXPublicClient()
        self.public_ws = OKXPublicWS()
        # Health supervisor: если OKX Public REST реально недоступен (таймаут/ошибка) — останавливаемся безопасно.
        self._okx_health_thread: Optional[threading.Thread] = None
        self._okx_health_last_ok_ts: float = 0.0
        self._okx_health_fail_count: int = 0
        self.private_ws: Optional[OKXPrivateWS] = None
        self._prv_last_ok_ts: float = 0.0
        self._prv_last_restart_ts: float = 0.0
        self._prv_first_start_ts: float = 0.0
        self._ws_pump_thread: Optional[threading.Thread] = None
        self._ws_pump_stop = threading.Event()
        self.private: Optional[OKXPrivateClient] = None
        self.trader: Optional[Trader] = None

        # безопасно перезапускать PRV без участия UI.
        self._okx_plain_keys: Dict[str, Any] = {
            'api_key': '',
            'api_secret': '',
            'passphrase': '',
            'simulated_trading': False,
        }

        self.registry = StrategyRegistry()
        self.portfolio = Portfolio(data_dir=data_dir)
        # shared reference for SymbolChannel to build position snapshot
        self.shared_state['portfolio_obj'] = self.portfolio


        # Пишет строго в ./data/decision_logs
        # Требование пользователя: если чекбокс "Снапшоты BUY/SELL (отладка)" выключен,
        # то decision-файлы НЕ пишем.
        try:
            lcfg = (self.config.get("logging", {}) or {})
            dbg_enabled = bool((self.config.get('trading', {}) or {}).get('snapshots_enabled', False))
            enabled_final = bool(lcfg.get("decision_log_enabled", True)) and dbg_enabled
            self.decision_logger = DecisionLogger(
                data_dir=self.data_dir,
                enabled=enabled_final,
                mode=str(lcfg.get("decision_log_mode", "signals")),
                tick_every_sec=float(lcfg.get("decision_log_tick_sec", 15.0)),
                max_bytes_per_file=int(lcfg.get("decision_log_max_bytes", 50*1024*1024)),
            )
        except Exception:
            self.decision_logger = DecisionLogger(data_dir=self.data_dir, enabled=False)
        # делаем доступным в каналах (SymbolChannel) через shared_state
        self.shared_state['decision_logger'] = self.decision_logger

        self.stop_event = threading.Event()
        # Runtime state: distinguishes "never started" from "running"
        self._is_running: bool = False
        self._lock = threading.Lock()
        self.channels: Dict[str, Any] = {}  # symbol->SymbolChannel

        self._top_refresh_thread: Optional[threading.Thread] = None
        self._balance_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartbeat_path = os.path.join(self.data_dir, 'heartbeat.txt')

        # cache OKX available SPOT instruments (to filter auto-top)
        self._okx_spot_inst_cache: Optional[Set[str]] = None
        self._okx_spot_inst_cache_ts: float = 0.0

        # cache "tradeable for this account" instruments.
        # Причина: в auto-top могут попадать пары, которые есть на OKX, но
        # недоступны для торговли в текущем аккаунте/регионе (local compliance) или в demo.
        # Мы проверяем доступность через private endpoint /account/max-size и кэшируем.
        self._okx_tradeable_cache: Dict[str, Dict[str, Any]] = {}  # instId -> {ok:bool, ts:float, reason:str}
        self._untradeable_path = os.path.join(self.data_dir, 'untradeable_symbols.json')
        self._load_untradeable_safe()
        self._inst_state_cache: Dict[str, Dict[str, Any]] = {}  # instId -> {ok:bool, ts:float, reason:str}

        # временный ban-лист (TTL) для символов после повторных сбоев.
        # Это критично для условия пользователя: не спамить несуществующими/недоступными парами.
        self._banlist_path = os.path.join(self.data_dir, 'temp_bans.json')
        self.banlist = BanList(path=self._banlist_path)
        self.banlist.load()
        # сделаем доступным в shared_state (для каналов)
        self.shared_state['banlist'] = self.banlist
        # переносим сохранённые untradeable в disabled_symbols, чтобы они
        # никогда не попадали в мониторинг на старте (особенно через top-cache).
        try:
            ds = self.shared_state.get('disabled_symbols')
            if isinstance(ds, set):
                for k, v in (self._okx_tradeable_cache or {}).items():
                    if isinstance(v, dict) and v.get('ok') is False:
                        ds.add(str(k))
        except Exception:
            pass


        self._order_tracker_thread: Optional[threading.Thread] = None

        # сканер fills для синхронизации ручных сделок и страховки, если pending не совпал.
        self._seen_fills_path = os.path.join(self.data_dir, 'seen_fills.json')
        self._seen_fills: Set[str] = set()
        self._load_seen_fills_safe()

        # время старта текущей сессии. Нужно, чтобы при выключенной проверке
        # старых ордеров мы НЕ подтягивали fills за прошлые часы/сутки (иначе после
        # очистки ledger сканер recent_fills снова импортирует старые сделки).
        self._run_started_at: float = 0.0

        # кэш балансов и базовая «защита» активов.
        # Балансы берем из account/balance (Trading) и asset/balances (Funding).
        self._balances_cache: dict = {"trading": {}, "funding": {}}  # {source:{ccy:{total,avail,usd}}}
        self._baseline_ccy: dict = {}  # ccy -> float (базовый остаток на момент START)
        self._protect_ccy: Set[str] = set()

        # порог "пыли" в USD. Остатки меньше этого порога:
        # - не блокируют режим "1 символ = 1 позиция",
        # - позволяют корректно закрывать сделки в UI после SELL (когда остаётся микродробь).
        try:
            tcfg = self.config.get('trading', {}) or {}
            dust_usd = float(tcfg.get('dust_usd_threshold', 1.0))
        except Exception:
            dust_usd = 1.0
        os.environ['ATE_DUST_USD'] = str(dust_usd)
        self.shared_state['dust_usd_threshold'] = float(dust_usd)

    def _fills_since_ts(self) -> float:
        """Нижняя граница по времени для импортируемых fills."""
        try:
            tcfg = self.config.get('trading', {}) or {}
            enabled = bool(tcfg.get('check_old_orders', False))
            hours = float(tcfg.get('check_old_orders_hours', 5))
        except Exception:
            enabled = False
            hours = 5.0

        now = time.time()
        # Если НЕ проверяем старые ордера — импортируем fills только с момента START (с небольшой форой).
        if not enabled:
            base = float(self._run_started_at or now)
            return max(0.0, base - 120.0)

        # Если проверяем — ограничиваем окном N часов от текущего времени.
        try:
            h = max(0.0, float(hours))
        except Exception:
            h = 0.0
        if h <= 0:
            base = float(self._run_started_at or now)
            return max(0.0, base - 120.0)
        return max(0.0, now - (h * 3600.0))


    
    def _apply_ledger_start_policy(self) -> None:
        """Политика загрузки локального trade_ledger при каждом START.

        Проблема: после тестов/сброса активов на OKX в data/trade_ledger.json могут оставаться
        старые "сделки", которых уже нет в балансе. Тогда UI показывает строки, а SELL невозможен.
        Решение: по настройке либо очищаем ledger, либо оставляем только последние N часов.
        """
        try:
            tcfg = self.config.get('trading', {}) or {}
            enabled = bool(tcfg.get('check_old_orders', False))
            hours = float(tcfg.get('check_old_orders_hours', 5))
        except Exception:
            enabled = False
            hours = 5.0

        # Очистка при START ломает бухгалтерию: open_trades пропадают, затем reconcile
        # создаёт сделки по текущей цене (external_balance), что даёт "500 → 1000" и FORCE_EXIT.
        # Вместо очистки допускаем только prune по окну часов (если включено).
        try:
            if enabled:
                self.portfolio.prune_trade_ledger(hours)
                log_event(self.data_dir, {"level":"INFO","msg":"ledger_prune_on_start","extra":{"hours":hours}})
            else:
                # ничего не стираем
                log_event(self.data_dir, {"level":"INFO","msg":"ledger_keep_on_start"})
        except Exception:
            return

    def set_private(self, api_key: str, api_secret: str, passphrase: str, *, simulated_trading: bool = False):
        """Подключить приватный клиент OKX.

        simulated_trading=True нужен, если ключ создан в ДЕМО-среде OKX.
        Иначе OKX вернёт 401 code=50101 (environment mismatch).
        """
        try:
            self._okx_plain_keys = {
                'api_key': api_key or '',
                'api_secret': api_secret or '',
                'passphrase': passphrase or '',
                'simulated_trading': bool(simulated_trading),
            }
        except Exception:
            pass
        # REST private client (execution)
        if api_key and api_secret and passphrase:
            self.private = OKXPrivateClient(api_key, api_secret, passphrase, simulated_trading=simulated_trading)
        else:
            self.private = None

        # WS private (state sync accelerator)
        try:
            if self.private_ws is not None:
                self.private_ws.stop()
        except Exception:
            pass
        self.private_ws = None

        try:
            wcfg = (self.config.get('ws', {}) or {})
            ws_private_enabled = bool(wcfg.get('private_enabled', True))
        except Exception:
            ws_private_enabled = True

        if (self.private is not None) and ws_private_enabled:
            try:
                def _on_evt(evt: Dict[str, Any]) -> None:
                    """Handle OKX private WS events.

                    Private WS is used as a *state sync accelerator*:
                    - fills: import into portfolio ledger (1:1 like OKX, supports multi-fill)
                    - orders: resolve pending orders faster (reduce REST polling)
                    - account: keep latest snapshot timestamp for operator visibility

                    Important: any error here must never crash engine.
                    """

                    # basic visibility + timestamps
                    try:
                        self.shared_state["ws_private_last_evt_ts"] = time.time()
                    except Exception:
                        pass

                    try:
                        ch = str((evt.get('arg') or {}).get('channel') or evt.get('channel') or '').strip()
                        if not ch:
                            ch = str(evt.get('channel') or '').strip()
                    except Exception:
                        ch = ""

                    # compact log to engine log (safe) — THROTTLED
                    # account channel can be a frequent keep-alive stream; do not spam logs.
                    try:
                        now_ts = time.time()
                        ts_map = getattr(self, "_ws_private_log_ts", None)
                        if not isinstance(ts_map, dict):
                            ts_map = {}
                        last_ts = float(ts_map.get(ch, 0.0) or 0.0)

                        throttle_sec = 60.0 if ch == 'account' else 5.0
                        if (now_ts - last_ts) >= throttle_sec:
                            log_event(self.data_dir, {
                                'level': 'INFO',
                                'msg': 'ws_private_event',
                                'extra': {
                                    'channel': ch,
                                    'n': int(len(evt.get('data') or []) if isinstance(evt.get('data'), list) else 0),
                                }
                            })
                            ts_map[ch] = float(now_ts)
                            setattr(self, "_ws_private_log_ts", ts_map)
                    except Exception:
                        pass

                    # 1) FILLS => ledger/positions (dedup by stable fill_uid)
                    if ch == 'fills':
                        try:
                            rows = evt.get('data') or []
                            if not isinstance(rows, list):
                                return
                            now = time.time()
                            for r in rows:
                                try:
                                    if not isinstance(r, dict):
                                        continue
                                    uid = self._fill_uid(r)
                                    if not uid:
                                        continue
                                    if uid in self._seen_fills:
                                        continue
                                    self._seen_fills.add(uid)
                                    # Apply into portfolio under controller lock
                                    with self._lock:
                                        self.portfolio.ingest_okx_fill(r, source='ws_private')
                                except Exception:
                                    continue
                            # throttle seen_fills persistence
                            try:
                                last_save = float(getattr(self, "_ws_private_seen_save_ts", 0.0) or 0.0)
                            except Exception:
                                last_save = 0.0
                            if (now - last_save) >= 3.0:
                                self._save_seen_fills_safe()
                                setattr(self, "_ws_private_seen_save_ts", float(now))
                        except Exception:
                            return

                    # 2) ORDERS => speed up pending order resolution
                    if ch == 'orders':
                        try:
                            rows = evt.get('data') or []
                            if not isinstance(rows, list):
                                return
                            for r in rows:
                                try:
                                    if not isinstance(r, dict):
                                        continue

                                    # а фактические исполнения прилетают как часть событий 'orders'
                                    # (fillSz/fillPx/fee/feeCcy/fillTime). Если мы их не импортируем —
                                    # ledger остаётся пустым и блок "Торговля" ничего не показывает.
                                    # IMPORTANT: Не используем accFillSz/суммарные поля как "fill".
                                    # Канал orders иногда присылает только кумулятивный объём, который
                                    # приводит к двойному учёту (250$ -> 500$) или к случайным перекосам.
                                    # Импортируем fill из orders ТОЛЬКО если OKX дал явный last-fill:
                                    #   fillSz + fillPx (+ tradeId, если есть).
                                    try:
                                        side0 = str(r.get('side') or '').lower().strip()
                                        px0 = float(r.get('fillPx') or 0.0)
                                        sz0 = float(r.get('fillSz') or 0.0)
                                        if side0 in ('buy', 'sell') and px0 > 0 and sz0 > 0:
                                            fill = {
                                                'instId': r.get('instId'),
                                                'side': side0,
                                                'fillPx': px0,
                                                'fillSz': sz0,
                                                'ordId': r.get('ordId'),
                                                'tradeId': r.get('tradeId') or r.get('fillId') or r.get('billId'),
                                                'fee': r.get('fee'),
                                                'feeCcy': r.get('feeCcy'),
                                                # OKX time fields (ms)
                                                'fillTime': r.get('fillTime') or r.get('uTime') or r.get('cTime') or r.get('ts'),
                                            }
                                            uid = self._fill_uid(fill)
                                            if uid and uid not in self._seen_fills:
                                                self._seen_fills.add(uid)
                                                with self._lock:
                                                    self.portfolio.ingest_okx_fill(fill, source='ws_orders')
                                                try:
                                                    self._save_seen_fills_safe()
                                                except Exception:
                                                    pass
                                    except Exception:
                                        pass

                                    sym = str(r.get('instId') or '').strip().upper()
                                    oid = str(r.get('ordId') or '').strip()
                                    state = str(r.get('state') or r.get('ordState') or '').strip().lower()
                                    if not sym or not oid:
                                        continue
                                    # If this ordId matches pending for that symbol and it's final => clear pending
                                    if state in ('filled', 'canceled', 'cancelled', 'rejected', 'failed'):
                                        po = None
                                        try:
                                            po = (self.portfolio.pending_orders or {}).get(sym)
                                        except Exception:
                                            po = None
                                        if isinstance(po, dict) and str(po.get('ord_id') or '') == oid:
                                            try:
                                                with self._lock:
                                                    self.portfolio.clear_pending(sym)
                                            except Exception:
                                                pass
                                except Exception:
                                    continue
                        except Exception:
                            return

                    # 3) ACCOUNT => keep last snapshot (optional, for UI)
                    if ch == 'account':
                        try:
                            self.shared_state['ws_private_account_ts'] = time.time()
                        except Exception:
                            pass

                self.private_ws = OKXPrivateWS(
                    api_key=api_key,
                    api_secret=api_secret,
                    passphrase=passphrase,
                    simulated_trading=simulated_trading,
                    on_event=_on_evt,
                )
                try:
                    stale = float(self.shared_state.get('prv_watchdog_stale_sec', 60.0) or 60.0)
                except Exception:
                    stale = 60.0
                try:
                    self.private_ws.set_watchdog(stale_sec=stale, check_every_sec=10.0)
                except Exception:
                    pass
                try:
                    if float(self._prv_first_start_ts or 0.0) <= 0:
                        self._prv_first_start_ts = time.time()
                except Exception:
                    pass
                try:
                    self._prv_last_restart_ts = time.time()
                except Exception:
                    pass
                self.private_ws.start()
            except Exception as e:
                try:
                    self.ui_queue.put({'type':'warn','symbol':'OKX','warn':f'WS private: {e}'})
                except Exception:
                    pass

        self.trader = Trader(self.data_dir, self.private)

    def _compute_order_size_base(self, symbol: str, usd_amount: float, last_price: float) -> float:
        if last_price <= 0:
            return 0.0
        return max(0.0, usd_amount / last_price)


    # ---------------- PRV health (24/7) ----------------

    def _restart_private_ws(self, *, reason: str = "auto") -> None:
        """Жёсткий перезапуск PRV (WS private) через повторный set_private.

        Мы сознательно делаем restart через set_private(), потому что:
        - callback/ingest fills формируется там же;
        - важно не потерять корректную дедупликацию fills.
        
        Безопасность:
        - ошибки в WS не должны ломать торговлю; REST продолжит работать.
        """
        try:
            keys = getattr(self, '_okx_plain_keys', {}) or {}
            api_key = str(keys.get('api_key') or '')
            api_secret = str(keys.get('api_secret') or '')
            passphrase = str(keys.get('passphrase') or '')
            sim = bool(keys.get('simulated_trading', False))
            if not (api_key and api_secret and passphrase):
                return
        except Exception:
            return

        try:
            self._prv_last_restart_ts = time.time()
        except Exception:
            pass

        try:
            self.ui_queue.put({'type': 'warn', 'symbol': 'OKX', 'warn': f'PRV auto-reconnect: {reason}'})
        except Exception:
            pass

        try:
            self.set_private(api_key, api_secret, passphrase, simulated_trading=sim)
            # и не деградировать после долгой работы.
            try:
                self.request_balances_refresh()
            except Exception:
                pass
            try:
                self._reconcile_recent_fills()
            except Exception:
                pass
        except Exception as e:
            try:
                self.ui_queue.put({'type': 'warn', 'symbol': 'OKX', 'warn': f'PRV reconnect failed: {e}'})
            except Exception:
                pass


    def _prv_health_check(self) -> None:
        """Проверка здоровья PRV и автопереподключение."""
        if getattr(self, 'private_ws', None) is None:
            return
        st = {}
        try:
            st = self.private_ws.status() or {}
        except Exception:
            st = {}

        try:
            self.shared_state['prv_status'] = st
        except Exception:
            pass

        try:
            ok = (str(st.get('connected') or '0') == '1') and (str(st.get('authed') or '0') == '1')
        except Exception:
            ok = False
        now = time.time()
        if ok:
            try:
                self._prv_last_ok_ts = now
            except Exception:
                pass
            return

        # если PRV OFF слишком долго — перезапуск (rate-limited)
        try:
            off_limit = float(self.shared_state.get('prv_restart_if_off_sec', 120.0) or 120.0)
        except Exception:
            off_limit = 120.0
        try:
            last_ok = float(self._prv_last_ok_ts or 0.0)
        except Exception:
            last_ok = 0.0
        try:
            first = float(self._prv_first_start_ts or 0.0)
        except Exception:
            first = 0.0
        base_ts = max(first, last_ok)

        try:
            last_restart = float(self._prv_last_restart_ts or 0.0)
        except Exception:
            last_restart = 0.0

        # не дёргаем reconnect слишком часто
        if last_restart and (now - last_restart) < 25.0:
            return

        if off_limit > 0 and base_ts > 0 and (now - base_ts) >= off_limit:
            # добавить чуть диагностики
            try:
                err = str(st.get('last_error') or '')
                age = str(st.get('last_msg_age_sec') or '')
                rsn = f"off>{off_limit}s age={age} err={err}"[:180]
            except Exception:
                rsn = f"off>{off_limit}s"
            self._restart_private_ws(reason=rsn)


    def _update_session_realized_pnl(self) -> None:
        """Обновляет shared_state.session_realized_pnl (только реализованный PnL, закрытые сделки).

        Требование пользователя:
        - прибыль сессии — не плавающая equity, а сумма realized PnL по закрытым сделкам
          (в USDT/quote), максимально близко к OKX.
        """
        try:
            baseline = float(self.shared_state.get('session_realized_baseline', 0.0) or 0.0)
        except Exception:
            baseline = 0.0
        try:
            realized = 0.0
            for tr in self.portfolio.trade_rows():
                try:
                    if float(getattr(tr, 'sell_ts', 0.0) or 0.0) > 0.0:
                        p, _ = tr.realized_pnl()
                        realized += float(p or 0.0)
                except Exception:
                    continue
            self.shared_state['session_realized_pnl'] = float(realized) - float(baseline)
        except Exception:
            return


    # ---------------- pending orders + snapshots ----------------

    def _extract_ord_id(self, place_order_resp: dict) -> str:
        try:
            data = (place_order_resp or {}).get('response', {}).get('data') or []
            if isinstance(data, list) and data:
                return str((data[0] or {}).get('ordId') or (data[0] or {}).get('orderId') or '')
        except Exception:
            return ''
        return ''

    def _snapshots_enabled(self) -> bool:
        try:
            return bool((self.config.get('trading', {}) or {}).get('snapshots_enabled', False))
        except Exception:
            return False

    def _maybe_snapshot(self, *, name: str, payload: dict) -> None:
        if not self._snapshots_enabled():
            return
        try:
            from engine.snapshots import write_snapshot
            write_snapshot(self.data_dir, name=name, payload=payload)
        except Exception:
            return

    # ---------------- fills sync (manual trades + страховка) ----------------

    def _load_seen_fills_safe(self) -> None:
        try:
            p = self._seen_fills_path
            if os.path.exists(p):
                data = json.loads(open(p, 'r', encoding='utf-8').read() or '[]')
                if isinstance(data, list):
                    self._seen_fills = set(str(x) for x in data if x)
        except Exception:
            self._seen_fills = set()

    def _save_seen_fills_safe(self) -> None:
        try:
            os.makedirs(self.data_dir, exist_ok=True)
            data = sorted(list(self._seen_fills))[-5000:]
            with open(self._seen_fills_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            return


    # ---------------- instrument tradeability cache ----------------

    def _load_untradeable_safe(self) -> None:
        """Load untradeable symbols persisted from previous runs.

        В демо OKX историю ордеров нельзя чистить, а список доступных для demo инструментов
        может быть ограничен. Если какой-то инструмент однажды вернул error (например
        local compliance restriction / instrument doesn't exist), мы запоминаем его и
        не включаем в auto-top в следующих запусках.
        """
        try:
            p = self._untradeable_path
            if os.path.exists(p):
                data = json.loads(open(p, 'r', encoding='utf-8').read() or '{}')
                if isinstance(data, dict):
                    self._okx_tradeable_cache = data
        except Exception:
            # если файл битый — просто стартуем без него
            self._okx_tradeable_cache = {}

    def _save_untradeable_safe(self) -> None:
        try:
            os.makedirs(self.data_dir, exist_ok=True)
            # режем размер (чтобы файл не рос бесконечно)
            cache = self._okx_tradeable_cache or {}
            # оставляем только "плохие" и свежие записи
            out = {}
            now = time.time()
            for k, v in cache.items():
                try:
                    ok = bool((v or {}).get('ok', True))
                    ts = float((v or {}).get('ts', 0.0))
                except Exception:
                    ok = True
                    ts = 0.0
                if (not ok) or (now - ts) < (7 * 86400):
                    out[str(k)] = v
            with open(self._untradeable_path, 'w', encoding='utf-8') as f:
                json.dump(out, f, ensure_ascii=False, indent=2)
        except Exception:
            return

    def _okx_is_tradeable(self, inst_id: str) -> tuple[bool, str]:
        """Return (ok, reason) for trading.

        Ранние версии пытались "проверять торгуемость" через private endpoints
        max-size/max-avail-size. В demo/сим-режиме эти эндпоинты часто возвращают
        maxBuy/maxSell=0 при корректных инструментах (BTC-USDT, ETH-USDT и т.д.),
        из-за чего auto-top начинал выбирать только мусорные/недоступные пары.

        Теперь мы НЕ делаем проактивные private проверки торгуемости.
        Реальную недоступность определяем только по факту отклонённого ордера
        (51001 / 51155) и по пользовательскому blacklist.
        """
        inst = str(inst_id or '').strip().upper()
        if not inst:
            return False, 'empty instId'

        # user blacklist
        try:
            scfg = self.config.get('symbols', {}) or {}
            bl = set(str(x).strip().upper() for x in (scfg.get('symbol_blacklist', []) or []) if str(x).strip())
            if inst in bl:
                return False, 'blacklisted'
        except Exception:
            pass

        # cached hard-bans from order rejects
        try:
            rec = (self._okx_tradeable_cache or {}).get(inst)
            if isinstance(rec, dict):
                ok_cached = bool(rec.get('ok', True))
                if not ok_cached:
                    src = str(rec.get('source', '') or '').lower().strip()
                    # legacy entries without source are ignored (they were often false bans)
                    if src in ('order', 'user'):
                        ts = float(rec.get('ts', 0.0) or 0.0)
                        if ts and (time.time() - ts) < (7 * 86400.0):
                            return False, str(rec.get('reason', '') or '')
        except Exception:
            pass

        
        # public instruments state (fast + reliable): skip suspended/canceled listings
        try:
            cache = getattr(self, "_inst_state_cache", None)
            if not isinstance(cache, dict):
                cache = {}
                setattr(self, "_inst_state_cache", cache)
            rec2 = cache.get(inst)
            if isinstance(rec2, dict):
                ts2 = float(rec2.get("ts", 0.0) or 0.0)
                if ts2 and (time.time() - ts2) < (6 * 3600.0):
                    if not bool(rec2.get("ok", True)):
                        return False, str(rec2.get("reason", "") or "")
            # fetch from public only when needed (on-demand)
            data = []
            try:
                data = self.public.instruments_spot(inst_id=inst)
            except Exception:
                data = []
            if isinstance(data, list) and data:
                r0 = data[0] or {}
                state = str(r0.get("state", "") or "").lower().strip()
                if state and state != "live":
                    reason = f"instrument_state:{state}"
                    cache[inst] = {"ok": False, "ts": time.time(), "reason": reason, "source": "public"}
                    # also persist as hard-untradeable for 7d window
                    try:
                        self._mark_symbol_untradeable(inst_id=inst, reason=reason, source="order")
                    except Exception:
                        pass
                    return False, reason
                # cache OK result
                cache[inst] = {"ok": True, "ts": time.time(), "reason": "", "source": "public"}
        except Exception:
            pass
        return True, ''

    def _mark_symbol_untradeable(self, inst_id: str, reason: str, source: str = 'order') -> None:
        inst = str(inst_id or '').strip().upper()
        if not inst:
            return
        try:
            self._okx_tradeable_cache[inst] = {'ok': False, 'ts': time.time(), 'reason': str(reason or '')[:240], 'source': str(source or 'order')[:16]}
            self._save_untradeable_safe()
        except Exception:
            pass

    def _maybe_disable_symbol_from_order_error(self, inst_id: str, res: dict) -> None:
        """Если OKX вернул что инструмент недоступен (compliance / not exist),
        выключаем его канал и запоминаем как untradeable.

        Это нужно чтобы auto-top не продолжал снова и снова включать такие пары.
        """
        try:
            inst_id = str(inst_id or '').strip().upper()
            if not inst_id:
                return
            if not isinstance(res, dict) or res.get('ok') is True:
                return

            # Trader.place_order может вернуть либо raw OKX, либо wrapper:
            # {"ok":false,"error":"...","response":{...OKX...}}
            j = res.get('response')
            if isinstance(j, str):
                try:
                    j = json.loads(j)
                except Exception:
                    j = None
            if not isinstance(j, dict):
                # fallback: вдруг кто-то передал raw OKX напрямую
                j = res if isinstance(res, dict) else {}
            code = str((j or {}).get('code', '') or '')
            msg = str((j or {}).get('msg', '') or '')
            s_code = ''
            s_msg = ''
            try:
                data = (j or {}).get('data') or []
                if isinstance(data, list) and data:
                    r0 = data[0] or {}
                    s_code = str(r0.get('sCode', '') or '')
                    s_msg = str(r0.get('sMsg', '') or '')
            except Exception:
                pass

            # Самые частые: 51001 (instrument doesn't exist), 51155 (local compliance)
            ban_codes = {'51001', '51155', '51087'}
            text = (res.get('error') or '') + ' ' + msg + ' ' + s_msg
            need_ban = (code in ban_codes) or (s_code in ban_codes)
            if not need_ban:
                # эвристика по тексту
                low = text.lower()
                if ('compliance' in low) or ('not exist' in low) or ('instrument' in low and 'exist' in low) or ('listing canceled' in low):
                    need_ban = True
            if not need_ban:
                return

            reason = f"order_reject code={code} sCode={s_code} {s_msg or msg}".strip()
            self._mark_symbol_untradeable(inst_id=str(inst_id), reason=reason, source='order')
            # disable channel for this run + сразу удаляем из активных каналов,
            # чтобы UI видел РЕАЛЬНО доступное количество котировок.
            try:
                ds = self.shared_state.get('disabled_symbols')
                if isinstance(ds, set):
                    ds.add(str(inst_id).strip().upper())
            except Exception:
                pass

            removed = False
            try:
                with self._lock:
                    if str(inst_id).strip().upper() in self.channels:
                        self.channels.pop(str(inst_id).strip().upper(), None)
                        removed = True
            except Exception:
                removed = False

            # сообщим UI и обновим список каналов (особенно важно при auto-top)
            try:
                self.ui_queue.put({"type":"warn","symbol":"ENGINE","warn":f"Символ {inst_id} отключён: {reason}"})
            except Exception:
                pass
            if removed:
                try:
                    scfg = self.config.get('symbols', {}) or {}
                    if bool(scfg.get('auto_top')):
                        self.ui_queue.put({"type":"top_symbols", "symbols": list(self.channels.keys())})
                except Exception:
                    pass
        except Exception:
            return


    def _fill_uid(self, r: dict) -> str:
        """Стабильный uid для одного исполнения (fill).

        Критично: один и тот же fill может прийти через PRIVATE WS и через REST.
        При этом время может называться по-разному (fillTime/ts/uTime/cTime) и
        приходить в секундах или миллисекундах. Если сохранять timestamp "как есть",
        одинаковый fill получает разные uid → мы импортируем его дважды →
        в UI сумма "Списано/Получено" удваивается (250$ → 500$).

        Поэтому timestamp нормализуем к ЦЕЛЫМ миллисекундам.
        uid строим по устойчивому набору: ordId + ts_ms + px + sz + side.
        """
        # Prefer OKX per-fill identifiers when available.
        # They are stable across WS and REST and avoid collisions when multiple fills
        # share the same millisecond timestamp.
        trade_id = r.get('tradeId') or r.get('fillId') or r.get('billId')
        if trade_id:
            try:
                side_s = str(r.get('side') or '').lower().strip()
                return f"{r.get('ordId','')}|{str(trade_id).strip()}|{side_s}"
            except Exception:
                pass

        t_raw = r.get('fillTime') or r.get('ts') or r.get('uTime') or r.get('cTime')
        try:
            t = float(t_raw) if t_raw is not None else 0.0
            # seconds -> ms
            if t > 0 and t < 1e12:
                t = t * 1000.0
            t_ms = str(int(round(t)))
        except Exception:
            t_ms = '0'
        try:
            px = float(r.get('fillPx') or r.get('px') or 0.0)
        except Exception:
            px = 0.0
        try:
            sz = float(r.get('fillSz') or r.get('sz') or 0.0)
        except Exception:
            sz = 0.0
        # округление, чтобы "0.30000000004" и "0.3" считались одинаковыми
        px_s = f"{px:.12f}".rstrip('0').rstrip('.')
        sz_s = f"{sz:.12f}".rstrip('0').rstrip('.')
        side_s = str(r.get('side') or '').lower().strip()
        return f"{r.get('ordId','')}|{t_ms}|{px_s}|{sz_s}|{side_s}"

    def _ord_uid(self, *, ord_id: str, side: str) -> str:
        """DEPRECATED.

        Ранее мы добавляли в seen_fills маркер ORD|ordId|side, чтобы сканер recent_fills
        пропускал весь ордер, если он уже был обработан pending-трекером.

        Это оказалось ошибкой на OKX: один ордер может приходить частями (multi-fill),
        а /trade/fills может отдавать неполный список в первые секунды.
        Если пометить ORD как "seen" слишком рано — оставшиеся fills никогда не будут
        импортированы и в UI появятся нереалистичные суммы/PnL.

        мы ДЕДУПЛИЦИРУЕМ ТОЛЬКО по fill_uid, а не по ordId целиком.
        Функцию оставляем для обратной совместимости, но не используем в логике.
        """
        return f"ORD|{str(ord_id or '').strip()}|{str(side or '').lower()}"

    def _start_order_tracker(self) -> None:
        if self._order_tracker_thread and self._order_tracker_thread.is_alive():
            return

        def run():
            # дополнительная синхронизация по "балансам + история ордеров".
            # Нужно, когда пользователь покупает/продаёт ВРУЧНУЮ (внутри приложения ATE или в мобильном OKX),
            # а /trade/fills может вернуть неполный набор (особенно на DEMO).
            last_reconcile_orders = 0.0
            while not self.stop_event.is_set():
                try:
                    self._process_pending_orders()
                    # страховка — синхронизация fills (в т.ч. ручные сделки в приложении OKX)
                    self._reconcile_recent_fills()

                    now = time.time()
                    if (now - last_reconcile_orders) >= 2.0:
                        last_reconcile_orders = now
                        self._reconcile_trades_by_balances_and_orders()
                except Exception as e:
                    log_event(self.data_dir, {"level":"ERROR","msg":"order_tracker_exception","extra":{"err":str(e)}})
                # чувствительный сон, чтобы STOP срабатывал быстро
                for _ in range(10):
                    if self.stop_event.is_set():
                        break
                    time.sleep(0.2)

        self._order_tracker_thread = threading.Thread(target=run, daemon=True)
        self._order_tracker_thread.start()

    def _process_pending_orders(self) -> None:
        """подтверждение ордеров через OKX fills.

        Правило: позиция/сделка обновляются ТОЛЬКО после подтверждения fills.
        """
        if self.trader is None or getattr(self.trader, 'private', None) is None:
            return

        # если pending-ордер подтвердился и мы уже обработали fills тут,
        # сканер recent_fills НЕ должен импортировать те же fills повторно.
        # Иначе суммы покупки/комиссии могут удваиваться (200$ -> 400$).
        seen_changed = False

        items = list((self.portfolio.pending_orders or {}).items())
        for symbol, p in items:
            try:
                side = str((p or {}).get('side') or '').lower()
                ord_id = str((p or {}).get('ord_id') or '')
                if not symbol or not ord_id or side not in ('buy', 'sell'):
                    self.portfolio.clear_pending(symbol)
                    continue

                # и портфель обновляется через portfolio.ingest_okx_fill().
                # В этом режиме PENDING-трекер НЕ ДОЛЖЕН повторно применять d_qty/d_notional через
                # apply_local_fill(), иначе возникает регресс "покупка 100$ -> отображается 200$".
                ws_auth = False
                try:
                    if getattr(self, 'private_ws', None) is not None:
                        st = self.private_ws.status() or {}
                        ws_auth = str(st.get('authed') or '0') == '1'
                except Exception:
                    ws_auth = False

                # order state (optional)
                state = ''
                try:
                    od = self.trader.private.order_details(inst_id=symbol, ord_id=ord_id)
                    d = (od or {}).get('data') or []
                    if isinstance(d, list) and d:
                        state = str((d[0] or {}).get('state') or '')
                except Exception:
                    state = ''

                last_px = float((p or {}).get('last_px') or 0.0)
                if last_px <= 0:
                    try:
                        t = self.public.ticker(symbol)
                        last_px = float(t.get('last') or t.get('lastPx') or 0.0)
                    except Exception:
                        last_px = 0.0

                # передаём side, чтобы корректно пересчитать qty при комиссии в base.
                fills = self.trader.fetch_fills_for_order(inst_id=symbol, ord_id=ord_id, last_px=last_px, side=side)
                if not fills.get('ok'):
                    try:
                        log_event(self.data_dir, {"level":"WARN","msg":"pending_fills_failed","extra":{"symbol":symbol,"ord_id":ord_id,"error":fills.get("error"),"state":state}})
                    except Exception:
                        pass
                    continue

                # помечаем все полученные fills как уже обработанные, чтобы
                # сканер recent_fills не продублировал их импорт в portfolio.
                try:
                    rows = fills.get('rows') or []
                    if isinstance(rows, list):
                        for rr in rows:
                            uid = self._fill_uid(rr or {})
                            if uid and uid not in self._seen_fills:
                                self._seen_fills.add(uid)
                                seen_changed = True
                except Exception:
                    pass

                filled_qty = float(fills.get('filled_qty') or 0.0)
                filled_qty_gross = float(fills.get('filled_qty_gross') or 0.0)
                notional_usd = float(fills.get('notional_usd') or 0.0)
                fee_usd = float(fills.get('fee_usd') or 0.0)
                avg_px = float(fills.get('avg_px') or 0.0)
                fee_mode = str(fills.get('fee_mode') or '')
                fee_ccy = str(fills.get('fee_ccy') or '')
                # для отображения: предпочитаем "живую" сумму комиссии в валюте,
                # а если её нет — оставляем 0.
                fee_amt = 0.0
                try:
                    fqa = float(fills.get('fee_quote_amt') or 0.0)
                    fba = float(fills.get('fee_base_amt') or 0.0)
                    if fqa > 0:
                        fee_amt = fqa
                    elif fba > 0:
                        fee_amt = fba
                except Exception:
                    fee_amt = 0.0

                prev_qty = float((p or {}).get('filled_qty') or 0.0)
                prev_notional = float((p or {}).get('notional_usd') or 0.0)
                prev_fee = float((p or {}).get('fee_usd') or 0.0)

                if filled_qty > prev_qty + 1e-12:
                    # Здесь мы обновляем только метаданные pending, НЕ трогая портфель.
                    if not ws_auth:
                        d_qty = filled_qty - prev_qty
                        d_notional = max(0.0, notional_usd - prev_notional)
                        d_fee = max(0.0, fee_usd - prev_fee)
                        d_px = (d_notional / d_qty) if d_qty > 0 else (avg_px or last_px)

                        # применяем подтверждённый fill
                        self.portfolio.apply_local_fill(symbol=symbol, side=side, qty=d_qty, price=d_px, fee=d_fee)

                        if side == 'buy':
                            first_ts = float((p or {}).get('first_fill_ts') or 0.0) or time.time()
                            if not (p or {}).get('first_fill_ts'):
                                p['first_fill_ts'] = first_ts
                            self.portfolio.set_open_trade_buy_totals(
                                symbol=symbol,
                                trade_id=str((p or {}).get('trade_id') or ''),
                                filled_qty=filled_qty,
                                filled_qty_gross=filled_qty_gross,
                                notional_usd=notional_usd,
                                fee_usd=fee_usd,
                                fee_mode=fee_mode,
                                fee_ccy=fee_ccy,
                                fee_amt=fee_amt,
                                avg_px=avg_px or d_px,
                                ts=first_ts,
                                source=str((p or {}).get('source') or 'bot'),
                                ord_id=ord_id,
                                buy_score=float((p or {}).get("buy_score") or (((p or {}).get("meta") or {}).get("confidence") or 0.0)),
                            )
                            # фиксируем BUY в истории сразу, когда появился первый подтверждённый fill
                            if prev_qty <= 0 and filled_qty > 0:
                                try:
                                    self.portfolio.record_trade({
                                        'type': 'BUY',
                                        'trade_id': str((p or {}).get('trade_id') or ''),
                                        'symbol': symbol,
                                        'ts': float(first_ts),
                                        'usd': float(notional_usd or 0.0),
                                        'qty': float(filled_qty or 0.0),
                                        'px': float(avg_px or d_px or last_px or 0.0),
                                        'fee_usd': float(fee_usd or 0.0),
                                        'ord_id': str(ord_id),
                                        'source': str((p or {}).get('source') or 'bot'),
                                    })
                                except Exception:
                                    pass


                    p['filled_qty'] = filled_qty
                    p['notional_usd'] = notional_usd
                    p['fee_usd'] = fee_usd
                    p['avg_px'] = avg_px
                    p['updated_ts'] = time.time()

                # done?
                # OKX market/limit ордер может состоять из множества fills, а /trade/fills
                # иногда отдаёт неполный список сразу после 'filled'.
                # Если мы очистим pending слишком рано — дальнейшие fills не попадут в totals,
                # и появится баг "BUY на 0.68$ / SELL на 99$".
                state_l = (state or '').lower()
                age = time.time() - float((p or {}).get('created_ts') or time.time())
                is_canceled = state_l in ('canceled', 'cancelled')
                is_filled = state_l == 'filled'

                # целевой accFillSz из order_details (если доступно)
                target_fill_sz = 0.0
                try:
                    if isinstance(od, dict):
                        d = (od or {}).get('data') or []
                        if isinstance(d, list) and d:
                            r0 = d[0] or {}
                            target_fill_sz = float(r0.get('accFillSz') or r0.get('fillSz') or 0.0)
                except Exception:
                    target_fill_sz = 0.0

                # критерий завершения pending: 
                # 1) order state canceled -> done
                # 2) order state filled AND totals выглядят завершёнными (fills >= accFillSz) -> done
                # 3) order state filled AND totals не меняются N секунд -> done (страховка)
                done = False
                if is_canceled:
                    done = True
                elif is_filled and filled_qty > 0:
                    try:
                        # если accFillSz доступен — сравниваем по GROSS (как OKX)
                        if float(target_fill_sz or 0.0) > 0 and float(filled_qty_gross or 0.0) >= (float(target_fill_sz) * 0.999):
                            done = True
                    except Exception:
                        pass

                    # если fills "не догнали" — держим pending короткую форту
                    try:
                        if not done:
                            if not (p or {}).get('filled_state_seen_ts'):
                                p['filled_state_seen_ts'] = time.time()
                                p['last_totals_qty'] = float(filled_qty or 0.0)
                                p['last_totals_ts'] = time.time()
                            else:
                                # фиксируем момент последнего изменения totals
                                last_q = float((p or {}).get('last_totals_qty') or 0.0)
                                if abs(float(filled_qty or 0.0) - last_q) > 1e-12:
                                    p['last_totals_qty'] = float(filled_qty or 0.0)
                                    p['last_totals_ts'] = time.time()
                                # если totals не меняются > 6 сек после filled — можем считать завершённым,
                                # НО для BUY (tgtCcy=quote_ccy) дополнительно проверяем, что notional близок
                                # к запрошенной сумме. Иначе OKX мог отдать неполный /fills, и мы зафиксируем
                                # «покупку на 0.68$» вместо ~100$ (как в баг-репорте пользователя).
                                quiet_sec = time.time() - float((p or {}).get('last_totals_ts') or time.time())
                                if quiet_sec >= 6.0:
                                    # sanity-check по запрошенной сумме (quote-ордер)
                                    try:
                                        req_usd = float((p or {}).get('req_usd') or 0.0)
                                    except Exception:
                                        req_usd = 0.0
                                    if str(side or '').lower() == 'buy' and req_usd > 0:
                                        if float(notional_usd or 0.0) >= float(req_usd) * 0.70:
                                            done = True
                                        else:
                                            # оставляем pending, чтобы следующая итерация подтянула totals
                                            done = False
                                    else:
                                        done = True
                    except Exception:
                        pass

                # Важно: если OKX уже говорит 'filled', но /trade/fills временно пустой — НЕ чистим pending.
                # Иначе позиция/сделка в UI никогда не появится (как у ETH).
                if filled_qty <= 0:
                    if is_canceled:
                        log_event(self.data_dir, {'level':'WARN','msg':'pending_canceled','extra':{'symbol':symbol,'ord_id':ord_id,'state':state}})
                        self.portfolio.clear_pending(symbol)
                    else:
                        if is_filled and age > 20:
                            log_event(self.data_dir, {'level':'WARN','msg':'pending_filled_but_no_fills_yet','extra':{'symbol':symbol,'ord_id':ord_id,'state':state,'age':age}})
                        # страховка: если pending живёт слишком долго — чистим, чтобы не блокировать навсегда
                        if age > 600:
                            log_event(self.data_dir, {'level':'WARN','msg':'pending_stale_cleared','extra':{'symbol':symbol,'ord_id':ord_id,'state':state,'age':age}})
                            self.portfolio.clear_pending(symbol)
                    continue

                # fills подтверждены
                if filled_qty > 0:
                    if side == 'sell':
                        pos = self.portfolio.position(symbol)
                                                # закрываем сделку даже если после SELL осталась "пыль".
                        thr = float(self.shared_state.get('dust_usd_threshold', 1.0) or 1.0)
                        ref_px = float(last_px or avg_px or ((notional_usd / filled_qty) if filled_qty > 0 else 0.0) or 0.0)
                        is_dust = False
                        try:
                            is_dust = self.portfolio.is_dust_qty(qty=float(pos.qty or 0.0), px=ref_px, threshold_usd=thr)
                        except Exception:
                            is_dust = False
                        if filled_qty > 0:
                            # если это пыль — сбрасываем позицию, чтобы UI/логика не зависали на микродолях
                            if is_dust and pos.qty > 0:
                                try:
                                    pos.qty = 0.0
                                    pos.avg_price = 0.0
                                    pos.opened_ts = 0.0
                                except Exception:
                                    pass

                            src_name = str((p or {}).get('source') or 'bot')
                            self.portfolio.on_bot_sell(
                                symbol=symbol,
                                trade_id=str((p or {}).get('trade_id') or ''),
                                qty=(filled_qty_gross if filled_qty_gross > 0 else filled_qty),
                                price=float(avg_px or last_px or 0.0),
                                fee_usd=fee_usd,
                                fee_mode=fee_mode,
                                fee_ccy=fee_ccy,
                                fee_amt=fee_amt,
                                usd_amount=notional_usd,
                                ts=time.time(),
                                source=src_name,
                                ord_id=ord_id,
                            )
                            self._maybe_snapshot(name='SELL_CONFIRMED', payload={
                                'symbol': symbol,
                                'ord_id': ord_id,
                                'fills': fills,
                                'pending': dict(p or {}),
                                'portfolio': self.portfolio.to_ui_dict(),
                            })

                    if side == 'buy' and filled_qty > 0:
                        self._maybe_snapshot(name='BUY_CONFIRMED', payload={
                            'symbol': symbol,
                            'ord_id': ord_id,
                            'fills': fills,
                            'pending': dict(p or {}),
                            'portfolio': self.portfolio.to_ui_dict(),
                        })

                    if done:
                        self.portfolio.clear_pending(symbol)
                    else:
                        # оставляем pending активным, чтобы следующая итерация могла
                        # подтянуть оставшиеся части multi-fill и обновить totals.
                        p['updated_ts'] = time.time()

            except Exception:
                # не зацикливаемся на битом pending
                try:
                    self.portfolio.clear_pending(symbol)
                except Exception:
                    pass

        if seen_changed:
            self._save_seen_fills_safe()


    def _reconcile_recent_fills(self) -> None:
        """Синхронизация фактических сделок с OKX через последний список fills.

        Зачем:
        - если /trade/fills по ordId временно пустой (OKX задерживает),
        - если сделка была сделана вручную в приложении OKX,
        - если pending/ordId не совпал из-за перезапуска.

        Это гарантирует, что UI и портфель подтянутся к реальному состоянию.
        """
        if self.trader is None or getattr(self.trader, 'private', None) is None:
            return
        try:
            res = self.trader.fetch_recent_fills(limit=100)
            if not res.get('ok'):
                try:
                    log_event(self.data_dir, {"level":"WARN","msg":"recent_fills_failed","extra":{k:res.get(k) for k in ("error","via") if k in res}})
                except Exception:
                    pass
                return
            rows = res.get('rows') or []
            if not isinstance(rows, list) or not rows:
                return
        except Exception:
            return

        new_cnt = 0
        cutoff_ts = float(self._fills_since_ts() or 0.0)
        for r in rows:
            try:
                # если пользователь выключил проверку старых ордеров.
                try:
                    ts_raw = (r or {}).get('fillTime') or (r or {}).get('ts') or (r or {}).get('uTime') or (r or {}).get('cTime')
                    ts = float(ts_raw) if ts_raw is not None else 0.0
                    if ts > 1e12:
                        ts = ts / 1000.0
                except Exception:
                    ts = 0.0
                if cutoff_ts > 0 and ts > 0 and ts < cutoff_ts:
                    continue
                uid = self._fill_uid(r or {})
                if not uid or uid in self._seen_fills:
                    continue
                self._seen_fills.add(uid)
                new_cnt += 1
                # импортируем в локальный портфель/историю
                try:
                    self.portfolio.ingest_okx_fill(r or {}, source='okx_fill_scan')
                except Exception as e:
                    log_event(self.data_dir, {'level':'WARN','msg':'fill_ingest_failed','extra':{'err':str(e),'row':r}})
            except Exception:
                continue

        if new_cnt > 0:
            self._save_seen_fills_safe()


    # ---------------- Балансы + история ордеров ----------------

    @staticmethod
    def _order_ts_sec(o: dict) -> float:
        """OKX order timestamps → seconds."""
        try:
            ts_raw = (o or {}).get('uTime') or (o or {}).get('cTime') or (o or {}).get('fillTime') or (o or {}).get('ts')
            ts = float(ts_raw) if ts_raw is not None else 0.0
            if ts > 1e12:
                ts = ts / 1000.0
            return float(ts or 0.0)
        except Exception:
            return 0.0

    def _find_latest_filled_order(self, *, symbol: str, side: str, after_ts: float) -> Optional[dict]:
        """Находит самый свежий filled-ордер по symbol и side, новее after_ts."""
        try:
            res = self.trader.fetch_recent_orders(inst_id=str(symbol), limit=50, state="filled")
            if not res.get('ok'):
                return None
            rows = res.get('data') or []
            if not isinstance(rows, list) or not rows:
                return None
            side_l = str(side or '').lower()
            best = None
            best_ts = 0.0
            for o in rows:
                try:
                    if str((o or {}).get('instId') or '') != str(symbol):
                        continue
                    if str((o or {}).get('side') or '').lower() != side_l:
                        continue
                    ts = self._order_ts_sec(o or {})
                    if after_ts > 0 and ts > 0 and ts < float(after_ts):
                        continue
                    if ts >= best_ts:
                        best_ts = ts
                        best = o
                except Exception:
                    continue
            return best
        except Exception:
            return None

    def _recover_position_cost_from_fills_history(self, *, symbol: str, bal_qty: float, last_px: float, cutoff_ts: float = 0.0) -> Optional[dict]:
        """Восстановление себестоимости позиции по fills-history.

        Зачем (REV5):
        - если trade_ledger был очищен/повреждён;
        - если позиция появилась вручную;
        - если OKX /trade/orders-history временно не возвращает нужный ordId.

        Мы НЕ создаём сделку по текущей цене (external_balance), потому что это даёт
        ложный buy_usd (например 500 превращается в 1000 при росте цены) и триггерит FORCE_EXIT.

        Метод: средняя цена (moving average accounting).
        Ведём (qty, cost_quote) и на SELL уменьшаем cost пропорционально проданному.
        """
        if self.trader is None or getattr(self.trader, 'private', None) is None:
            return None
        try:
            rows_info = self.trader.fetch_fills_history_for_symbol(inst_id=str(symbol), limit=100)
            if not (rows_info or {}).get('ok'):
                return None
            rows = (rows_info or {}).get('rows') or []
            if not isinstance(rows, list) or not rows:
                return None
        except Exception:
            return None

        # sort by time asc
        def _ts(r: dict) -> float:
            try:
                ts_raw = (r or {}).get('fillTime') or (r or {}).get('ts') or (r or {}).get('uTime') or (r or {}).get('cTime')
                ts = float(ts_raw) if ts_raw is not None else 0.0
                if ts > 1e12:
                    ts = ts / 1000.0
                return float(ts or 0.0)
            except Exception:
                return 0.0

        rows2 = []
        for r in rows:
            try:
                if str((r or {}).get('instId') or '') != str(symbol):
                    continue
                ts = _ts(r)
                if cutoff_ts and ts and ts < float(cutoff_ts):
                    continue
                rows2.append(r)
            except Exception:
                continue
        if not rows2:
            return None
        rows2.sort(key=_ts)

        qty = 0.0
        cost_quote = 0.0
        last_ts = 0.0
        for r in rows2:
            try:
                side = str((r or {}).get('side') or '').lower().strip()
                fill_sz = float((r or {}).get('fillSz') or (r or {}).get('sz') or 0.0)
                fill_px = float((r or {}).get('fillPx') or (r or {}).get('px') or 0.0)
                if fill_sz <= 0 or fill_px <= 0:
                    continue
                notional = fill_sz * fill_px
                fee = float((r or {}).get('fee') or 0.0)
                fee_ccy = str((r or {}).get('feeCcy') or '').upper().strip()
                # fee -> quote approx
                fee_quote = 0.0
                try:
                    base_ccy, quote_ccy = (str(symbol).split('-', 1)[0].upper(), str(symbol).split('-', 1)[1].upper())
                except Exception:
                    base_ccy, quote_ccy = ('', 'USDT')
                f_abs = abs(float(fee or 0.0))
                if f_abs > 0 and fee_ccy:
                    if quote_ccy and fee_ccy == quote_ccy:
                        fee_quote = f_abs
                    elif base_ccy and fee_ccy == base_ccy:
                        fee_quote = f_abs * (float(fill_px) or float(last_px) or 0.0)
                    else:
                        fee_quote = f_abs * (float(last_px) or float(fill_px) or 0.0)

                last_ts = max(last_ts, _ts(r))
                if side == 'buy':
                    qty += fill_sz
                    cost_quote += (notional + fee_quote)
                elif side == 'sell':
                    if qty <= 0:
                        continue
                    sell_qty = min(fill_sz, qty)
                    if sell_qty <= 0:
                        continue
                    # reduce cost proportionally
                    frac = sell_qty / qty
                    cost_quote = max(0.0, cost_quote * (1.0 - frac))
                    qty = max(0.0, qty - sell_qty)
                else:
                    continue
            except Exception:
                continue

        if qty <= 0:
            return None

        # if balance qty is known, prefer it (OKX may have dust differences)
        if bal_qty > 0 and abs(qty - bal_qty) / max(bal_qty, 1e-9) <= 0.25:
            qty_use = float(bal_qty)
            # scale cost to qty_use proportionally
            if qty > 0:
                cost_quote = cost_quote * (qty_use / qty)
            qty = qty_use

        avg_px = (cost_quote / qty) if qty > 0 else 0.0
        if avg_px <= 0 and float(last_px or 0.0) > 0:
            avg_px = float(last_px)
        return {
            'qty': float(qty),
            'cost_quote': float(cost_quote),
            'avg_px': float(avg_px),
            'ts': float(last_ts or time.time()),
        }

    def _reconcile_trades_by_balances_and_orders(self) -> None:
        """синхронизация сделок с реальным состоянием (балансы) и history ордеров.

        Проблема, которую решаем:
        - пользователь может BUY/SELL вручную (в ATE или в мобильном OKX)
        - balances меняются, но open_trades остаются «висящими» без SELL,
          если fills временно не вернулись или были отфильтрованы.

        Решение:
        - если позиция по symbol уже 0 или «пыль» → ищем свежий SELL в orders-history и закрываем сделку.
        - если позиция > 0, а open_trade нет → ищем свежий BUY и создаём строку сделки.
        """
        if self.trader is None or getattr(self.trader, 'private', None) is None:
            return

        tcfg = (self.shared_state.get('trade_config') or {})
        try:
            dust_usd = float(tcfg.get('dust_usd_threshold', 1.0))
        except Exception:
            dust_usd = 1.0

        # Берём last_px по symbol для корректного порога «пыли».
        last_prices = (self.shared_state.get('last_prices') or {})

        def _base_ccy(inst_id: str) -> str:
            try:
                return str(inst_id or '').split('-')[0].upper().strip()
            except Exception:
                return ''

        def _trading_qty_usd(inst_id: str, last_px: float) -> tuple[float, float]:
            """Сколько базовой валюты реально есть на OKX (Trading), и её USD-экв.

            Важно: мы НЕ можем полагаться только на локальные позиции, потому что
            ручные SELL/BUY могли исполниться, но fills временно не вернулись.
            Поэтому в FIX14 синхронизируемся по balance.details.
            """
            ccy = _base_ccy(inst_id)
            if not ccy:
                return 0.0, 0.0
            with self._lock:
                row = (self._balances_cache.get('trading') or {}).get(ccy) or {}
            qty = 0.0
            usd = 0.0
            try:
                qty = float(row.get('total') or 0.0)
            except Exception:
                qty = 0.0
            try:
                usd = float(row.get('usd') or 0.0)
            except Exception:
                usd = 0.0
            # На демо eqUsd иногда 0 для мелких остатков — пересчитаем сами.
            if usd <= 0.0 and qty > 0.0 and float(last_px or 0.0) > 0.0:
                usd = qty * float(last_px)
            return qty, usd

        # 1) Закрываем open_trades, которые фактически уже проданы (баланс 0/пыль)
        for sym, tr in list((self.portfolio.open_trades or {}).items()):
            try:
                if not tr or not tr.is_open:
                    continue
                if (self.portfolio.pending_orders or {}).get(sym):
                    continue

                # определяем факт закрытия позиции по ОФИЦИАЛЬНОМУ OKX балансу,
                # а не по локальным позициям (они могут не обновиться, если fills временно пустые).
                pos = self.portfolio.position(sym)
                last_px = float(last_prices.get(sym) or pos.last_price or tr.buy_px or 0.0)
                bal_qty, bal_usd = _trading_qty_usd(sym, last_px)
                is_dust = (bal_usd > 0.0 and bal_usd < float(dust_usd)) or (bal_qty > 0.0 and bal_usd <= 0.0)

                # Если на OKX реально есть позиция (выше порога «пыли») — ничего не делаем.
                if bal_qty > 0.0 and not is_dust:
                    continue

                # Позиции уже нет — значит SELL был (внешний или наш). Пытаемся найти SELL ордер.
                after_ts = float(tr.buy_ts or 0.0) - 60.0
                o = self._find_latest_filled_order(symbol=sym, side='sell', after_ts=after_ts)
                ord_id = str((o or {}).get('ordId') or '')
                if ord_id and ord_id == str(tr.sell_ord_id or ''):
                    continue

                if ord_id:
                    info = self.trader.fetch_fills_for_order(inst_id=sym, ord_id=ord_id, last_px=last_px, side='sell')
                    if info.get('ok') and float(info.get('filled_qty') or 0.0) > 0:
                        fee_mode = str(info.get('fee_mode') or '')
                        fee_ccy = str(info.get('fee_ccy') or '')
                        fee_amt = float(info.get('fee_quote_amt') or 0.0) if fee_mode == 'quote' else float(info.get('fee_base_amt') or 0.0)
                        self.portfolio.on_bot_sell(
                            symbol=sym,
                            qty=float(info.get('filled_qty') or 0.0),
                            price=float(info.get('avg_px') or last_px or 0.0),
                            fee_usd=float(info.get('fee_usd') or 0.0),
                            fee_mode=fee_mode,
                            fee_ccy=fee_ccy,
                            fee_amt=fee_amt,
                            usd_amount=float(info.get('notional_usd') or 0.0),
                            ts=float(info.get('ts') or time.time()),
                            source='external',
                            ord_id=ord_id,
                        )
                        continue

                # fallback: если ордер не нашли, но на OKX позиции уже нет — закрываем синтетически
                # Вместо этого ждём, пока OKX отдаст fills-history (обычно это минуты).
                # Если хотим принудительно закрыть висяк — используем кнопку "Продать" в UI.
                continue
            except Exception:
                continue

        # 2) Создаём open_trades для позиций, которые появились вручную (balances > 0, а строки сделки нет)
        cutoff_ts = float(self._fills_since_ts() or 0.0)
        # создаём сделки по реальным OKX балансам, даже если локальные позиции пусты.
        # Берём символы из каналов и (опционально) из мониторинга/конфига.
        symbols_for_reconcile: Set[str] = set()
        try:
            with self._lock:
                symbols_for_reconcile |= set(self.channels.keys())
        except Exception:
            pass
        try:
            last_map = (self.shared_state.get('last_prices') or {})
            symbols_for_reconcile |= set(last_map.keys())
        except Exception:
            pass

        for sym in sorted(list(symbols_for_reconcile)):
            try:
                if not sym or str(sym).endswith('-USD-SWAP'):
                    continue
                if (self.portfolio.pending_orders or {}).get(sym):
                    continue
                if (self.portfolio.open_trades or {}).get(sym):
                    continue
                last_px = float(last_prices.get(sym) or 0.0)
                bal_qty, bal_usd = _trading_qty_usd(sym, last_px)
                if bal_qty <= 0.0:
                    continue
                if bal_usd > 0.0 and bal_usd < float(dust_usd):
                    # пыль не создаёт сделку
                    continue

                o = self._find_latest_filled_order(symbol=sym, side='buy', after_ts=cutoff_ts)
                ord_id = str((o or {}).get('ordId') or '')
                if ord_id:
                    info = self.trader.fetch_fills_for_order(inst_id=sym, ord_id=ord_id, last_px=last_px, side='buy')
                    if info.get('ok') and float(info.get('filled_qty') or 0.0) > 0:
                        fee_mode = str(info.get('fee_mode') or '')
                        fee_ccy = str(info.get('fee_ccy') or '')
                        fee_amt = float(info.get('fee_quote_amt') or 0.0) if fee_mode == 'quote' else float(info.get('fee_base_amt') or 0.0)
                        self.portfolio.on_bot_buy(
                            symbol=sym,
                            qty=float(info.get('filled_qty') or 0.0),
                            price=float(info.get('avg_px') or last_px or 0.0),
                            fee_usd=float(info.get('fee_usd') or 0.0),
                            fee_mode=fee_mode,
                            fee_ccy=fee_ccy,
                            fee_amt=fee_amt,
                            usd_amount=float(info.get('notional_usd') or 0.0),
                            ts=float(info.get('ts') or time.time()),
                            source='external',
                            ord_id=ord_id,
                        )
                        continue

                # чтобы не создавать сделку по текущей цене (это даёт "500 → 1000" и FORCE_EXIT).
                rec = self._recover_position_cost_from_fills_history(symbol=sym, bal_qty=float(bal_qty), last_px=float(last_px), cutoff_ts=0.0)
                if rec and float(rec.get('qty') or 0.0) > 0 and float(rec.get('avg_px') or 0.0) > 0:
                    self.portfolio.on_bot_buy(
                        symbol=sym,
                        qty=float(rec.get('qty') or 0.0),
                        price=float(rec.get('avg_px') or last_px or 0.0),
                        fee_usd=0.0,
                        fee_mode='quote',
                        fee_ccy='USDT',
                        fee_amt=0.0,
                        usd_amount=float(rec.get('cost_quote') or 0.0),
                        ts=float(rec.get('ts') or time.time()),
                        source='recovery_fills',
                        ord_id='',
                    )
                # иначе: не создаём синтетическую сделку и не триггерим FORCE_EXIT.
                continue
            except Exception:
                continue


    def manual_trade(self, symbol: str, side: str, last_price: float, source: str = "manual", force: bool = False, meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Исполнить BUY/SELL.

        - pending_orders: реальный ордер OKX сначала становится pending
        - позиция и сделки обновляются только по подтверждённым fills (трекер)
        - режим 1 символ = 1 позиция (и 1 pending)
        """
        tcfg = self.config.get("trading", {})

        # ensure meta dict (used for reasons/decision_price)
        if meta is None or not isinstance(meta, dict):
            meta = {}
        # For manual SELL, still attach a reason so History can show it
        if str(side).lower().strip() == 'sell' and not str(meta.get('reason') or ''):
            meta['reason'] = 'MANUAL'
        # decision_price should reflect realistic executable side: BUY=ask, SELL=bid
        try:
            lp = (self.shared_state.get('last_prices') or {}).get(symbol) or {}
            if str(side).lower().strip() == 'buy':
                px = float(lp.get('ask') or lp.get('last') or last_price or 0.0)
            else:
                px = float(lp.get('bid') or lp.get('last') or last_price or 0.0)
            if px > 0:
                meta.setdefault('decision_price', px)
        except Exception:
            pass

        # Комиссия: пытаемся взять с OKX, иначе из конфига
        fee_rate = 0.001
        try:
            if self.trader is not None:
                fee_rate = float(self.trader.get_trade_fee_rate(inst_id=symbol))
            else:
                fee_rate = float(tcfg.get("fee_rate", 0.001))
        except Exception:
            fee_rate = 0.001
        dry = False  # dry-run удалён
        mode = str(tcfg.get("order_size_mode", "fixed")).lower().strip()

        # enforce pending guard
        # План#4: запрещаем дублирование SELL-запросов даже при force_exit.
        # Иначе при сетевых лагах / повторных сигналах можно отправить несколько SELL подряд.
        try:
            if getattr(self.portfolio, "has_pending", None) is not None and self.portfolio.has_pending(symbol):
                if side.lower().strip() == 'sell':
                    # при плавном стопе/аварийном выходе разрешаем SELL даже если pending висит
                    if not force:
                        return {"ok": False, "error": f"По {symbol} уже есть ожидающий ордер SELL (pending). Дублирование запрещено."}
                if not force:
                    return {"ok": False, "error": f"По {symbol} уже есть ожидающий ордер (pending)."}
        except Exception as e:
            log_event(self.data_dir, {"level":"WARN","msg":"manual_trade_pending_guard_exception","extra":{"symbol":symbol,"error":str(e)}})
            ui_warn_once(self, "manual_trade_pending_guard", "Внутренняя ошибка проверки pending-ордера (см. logs).", ttl_sec=10.0, extra={"symbol":symbol})

        side_l = side.lower().strip()
        if self.trader is None or getattr(self.trader, "private", None) is None:
            return {"ok": False, "error": "NO_PRIVATE_CLIENT: подключи ключи OKX и перезапусти"}

        # не допускаем новые BUY (иначе UI видит "КУПИТЬ", но ордера постоянно отклоняются).
        # SELL не блокируем, чтобы пользователь мог попытаться закрыть позицию (если она есть).
        try:
            sym_u = str(symbol or '').strip().upper()
            ds = self.shared_state.get('disabled_symbols')
            cache = (self._okx_tradeable_cache or {}).get(sym_u) or {}
            cache_says_no = (isinstance(cache, dict) and cache.get('ok') is False)
            if side_l == 'buy' and ((isinstance(ds, set) and sym_u in ds) or cache_says_no):
                why = str(cache.get('reason','') or '')
                return {"ok": False, "error": f"Символ {sym_u} отключён как недоступный для торговли. {why}".strip()}
            # нормализуем дальше
            symbol = sym_u
        except Exception as e:
            log_event(self.data_dir, {"level":"WARN","msg":"manual_trade_disabled_symbol_check_exception","extra":{"symbol":symbol,"error":str(e)}})
            ui_warn_once(self, "manual_trade_disabled_symbol_check", "Внутренняя ошибка проверки disabled_symbols (см. logs).", ttl_sec=10.0, extra={"symbol":symbol})

        # при плавном стопе запрещаем новые BUY (SELL разрешаем)
        try:
            if side_l == 'buy' and bool(self.shared_state.get('smooth_stop', False)):
                return {"ok": False, "error": "Плавный стоп активен: новые покупки запрещены."}
        except Exception as e:
            log_event(self.data_dir, {"level":"WARN","msg":"manual_trade_smooth_stop_check_exception","extra":{"symbol":symbol,"error":str(e)}})
            ui_warn_once(self, "manual_trade_smooth_stop_check", "Внутренняя ошибка проверки smooth_stop (см. logs).", ttl_sec=10.0, extra={"symbol":symbol})

        if side_l == "buy":
            # SAFETY: для реальной торговли требуем актуальную синхронизацию баланса OKX,
            # иначе можем начать "скупать всё подряд" до того, как подтянется кэш баланса.
            now_ts = time.time()
            if not dry:
                if float(self.portfolio.cash_usdt or 0.0) <= 0 or (now_ts - float(self.portfolio.last_okx_sync_ts or 0.0)) > 20:
                    return {"ok": False, "error": "OKX баланс не синхронизирован (cash_usdt=0 или устарел). Подожди 5–10 секунд после START и повтори."}

            # SAFETY: лимит количества одновременно открытых позиций (включая pending).
            try:
                max_pos = int(tcfg.get("max_positions", 1))
            except Exception:
                max_pos = 1
            # "пыль" (остатки меньше порога) не считаем как открытые позиции,
            # иначе max_positions будет блокировать покупки после частичных/округлённых продаж.
            dust_th = float(self.shared_state.get('dust_usd_threshold', 1.0) or 1.0)
            try:
                open_lots_total = int(getattr(self.portfolio, "open_trade_total_count", lambda: 0)())
            except Exception:
                open_lots_total = 0
            pending_cnt = len(self.portfolio.pending_orders or {})
            # NEW: разрешить превышать max_positions при высоком Score
            conf = 0.0
            try:
                if isinstance(meta, dict):
                    conf = float(meta.get("confidence") or 0.0)
            except Exception:
                conf = 0.0
            allow_exceed = False
            try:
                allow_exceed = bool(tcfg.get("allow_exceed_max_positions", False)) and conf >= float(tcfg.get("exceed_max_positions_score", 0.95))
            except Exception:
                allow_exceed = False
            if max_pos > 0 and (open_lots_total + pending_cnt) >= max_pos and not allow_exceed:
                return {"ok": False, "error": f"Достигнут лимит позиций (max_positions={max_pos})."}

            pos = self.portfolio.position(symbol)

            try:
                max_per_sym = int(float(tcfg.get("max_positions_per_symbol", 1)))
            except Exception:
                max_per_sym = 1
            if max_per_sym <= 0:
                max_per_sym = 1

            # если есть остаток и он НЕ "пыль", считаем что есть позиция
            try:
                dust_th = float(self.shared_state.get('dust_usd_threshold', 1.0) or 1.0)
            except Exception:
                dust_th = 1.0

            # сколько лотов открыто
            try:
                open_cnt = int(getattr(self.portfolio, "open_trade_count", lambda _s: 0)(symbol))
            except Exception:
                open_cnt = 0

            if pos.qty > 0:
                # если остаток — "пыль" (ниже порога) — НЕ блокируем BUY.
                try:
                    if self.portfolio.is_dust_qty(qty=float(pos.qty or 0.0), px=float(last_price or pos.last_price or 0.0), threshold_usd=float(dust_th)):
                        open_cnt = 0
                    else:
                        # есть реальная позиция
                        if open_cnt <= 0:
                            open_cnt = 1  # восстановление после потери ledger
                except Exception:
                    # если не смогли определить пыль — считаем что позиция есть
                    if open_cnt <= 0:
                        open_cnt = 1

            if open_cnt >= 1:
                if open_cnt >= max_per_sym:
                    return {"ok": False, "error": f"По {symbol} уже открыто {open_cnt} поз(и)ций. Лимит на символ = {max_per_sym}."}

                # повторный BUY разрешён ТОЛЬКО если текущий лот в минусе и сигнал очень сильный
                last_tr = None
                try:
                    last_tr = getattr(self.portfolio, "last_open_trade", lambda _s: None)(symbol)
                except Exception:
                    last_tr = None

                # определяем, в минусе ли последняя покупка
                is_negative = False
                try:
                    if last_tr is not None:
                        bp = float(getattr(last_tr, 'buy_px', 0.0) or 0.0)
                        if bp > 0 and float(last_price or 0.0) > 0:
                            is_negative = float(last_price) < bp
                except Exception:
                    is_negative = False
                # v3: legacy DCA-порог удалён
                except Exception:
                    dca_thr = 0.70
                dca_thr = min(0.95, max(0.0, dca_thr))

                if not is_negative:
                    return {"ok": False, "error": f"Повторный BUY по {symbol} запрещён: текущая позиция не в минусе."}

                if conf < dca_thr:
                    return {"ok": False, "error": f"Повторный BUY по {symbol} запрещён: уверенность {conf:.3f} < DCA-порог {dca_thr:.3f}."}

            usd = 0.0
            if mode == "percent":
                try:
                    pct = float(tcfg.get("order_size_pct", 5.0))
                except Exception:
                    pct = 5.0
                cash = float(self.portfolio.cash_usdt or 0.0)
                usd = max(0.0, cash * (pct / 100.0))
            else:
                usd = float(tcfg.get("default_order_usd", 20.0))

            # резерв по USDT
            try:
                reserve_pct = float(tcfg.get("min_cash_reserve_pct", 10.0))
            except Exception:
                reserve_pct = 10.0
            cash = float(self.portfolio.cash_usdt or 0.0)
            if cash > 0:
                reserve = cash * (reserve_pct / 100.0)
                if (cash - reserve) <= 0:
                    return {"ok": False, "error": "Недостаточно свободных USDT (весь баланс в резерве)."}
                if usd > (cash - reserve):
                    usd = max(0.0, cash - reserve)

            if usd <= 0:
                return {"ok": False, "error": "Сумма ордера <= 0 (проверь настройки/баланс)."}

            # --- Per-symbol order cap (OKX market value limits) ---
            # Требование: если по конкретной позиции лимит меньше заданного размера,
            # берём допустимый лимит ДЛЯ ЭТОЙ позиции и продолжаем дальше по настройкам.
            # Применяем только для MARKET (tgt_ccy=quote), т.к. ограничение 51201 относится к market value.
            def _cap_usd_for_symbol(_sym: str, _usd: float, _ord_type: str) -> float:
                try:
                    if str(_ord_type).lower().strip() != 'market':
                        return float(_usd)
                    caps = self.shared_state.get('symbol_max_market_usd')
                    if not isinstance(caps, dict):
                        caps = {}
                        self.shared_state['symbol_max_market_usd'] = caps
                    # статический override из конфига
                    cfg_caps = (tcfg.get('symbol_market_usd_caps') or tcfg.get('symbol_order_caps') or {})
                    if isinstance(cfg_caps, dict) and str(_sym) in cfg_caps:
                        try:
                            caps[str(_sym)] = float(cfg_caps[str(_sym)])
                        except Exception:
                            pass
                    capv = caps.get(str(_sym))
                    if capv is None:
                        return float(_usd)
                    capf = float(capv)
                    if capf > 0 and float(_usd) > capf:
                        try:
                            log_event(self.data_dir, {"level":"INFO","msg":"order_usd_capped","extra":{"symbol":_sym,"usd":float(_usd),"cap":capf}})
                        except Exception:
                            pass
                        return capf
                except Exception:
                    return float(_usd)
                return float(_usd)

            # SAFETY: не отправляем на OKX "копеечные" остатки — это выглядит как скупка без разбора
            try:
                min_order_usd = float(tcfg.get("min_order_usd", 10.0))
            except Exception:
                min_order_usd = 10.0
            if usd < min_order_usd:
                return {"ok": False, "error": f"Сумма ордера {usd:.2f} USDT меньше min_order_usd={min_order_usd}. Ордер заблокирован."}

            ord_type = ("market" if force else str(tcfg.get("order_type", "market")).lower())
            if ord_type not in ("market", "limit", "post_only"):
                ord_type = "market"

            usd = _cap_usd_for_symbol(symbol, usd, ord_type)

            sz = f"{usd:.2f}".rstrip("0").rstrip(".")
            tgt_ccy = "quote_ccy"
            px = None

            lot_trade_id = self.portfolio._new_trade_id() if hasattr(self.portfolio, '_new_trade_id') else f"lot_{int(time.time()*1000)}"

            
            ok_tr, rsn = self._okx_is_tradeable(symbol)
            if not ok_tr:
                res = {"ok": False, "dry_run": bool(dry), "error": f"SYMBOL_UNTRADEABLE: {rsn}", "response": {"code": "UNTRADEABLE", "msg": str(rsn)}}
                try:
                    self._mark_symbol_untradeable(inst_id=str(symbol), reason=str(rsn), source="order")
                except Exception:
                    pass
                try:
                    self.ui_queue.put({"type":"warn","symbol":"ENGINE","warn":f"BUY пропущен: {symbol} недоступен ({rsn})"})
                except Exception:
                    pass
                log_event(self.data_dir, {"level": "INFO", "msg": "manual_trade", "extra": {"symbol": symbol, "side": side_l, "res": res}})
                return res
            # place order with one safe retry for OKX 51201 (market value cap)
            res = self.trader.place_order(dry_run=dry, inst_id=symbol, side="buy", sz=sz, ord_type=ord_type, px=px, tgt_ccy=tgt_ccy)
            try:
                if (not res.get('ok')) and ('sCode=51201' in str(res.get('error') or '')) and ord_type == 'market':
                    # Парсим "can't exceed 100" из сообщения и сохраняем cap на символ.
                    import re
                    m = re.search(r"exceed\s+([0-9]+(?:\.[0-9]+)?)", str(res.get('error') or ''), re.IGNORECASE)
                    if m:
                        cap_val = float(m.group(1))
                        if cap_val > 0:
                            caps = self.shared_state.get('symbol_max_market_usd')
                            if not isinstance(caps, dict):
                                caps = {}
                                self.shared_state['symbol_max_market_usd'] = caps
                            caps[str(symbol)] = float(cap_val)
                            # повторяем ордер на допустимую сумму
                            usd2 = min(float(usd), float(cap_val))
                            sz2 = f"{usd2:.2f}".rstrip("0").rstrip(".")
                            log_event(self.data_dir, {"level":"WARN","msg":"retry_buy_with_market_cap","extra":{"symbol":symbol,"old_usd":float(usd),"cap":float(cap_val),"new_usd":float(usd2)}})
                            res = self.trader.place_order(dry_run=dry, inst_id=symbol, side="buy", sz=sz2, ord_type=ord_type, px=px, tgt_ccy=tgt_ccy)
            except Exception:
                pass

            # если OKX сообщает что пара недоступна — отключаем её
            try:
                self._maybe_disable_symbol_from_order_error(inst_id=symbol, res=res)
            except Exception:
                pass

            try:
                if not res.get("ok"):
                    # пороги из config
                    try:
                        tcfg0 = (self.config.get("trading", {}) or {})
                        thr = int(tcfg0.get("ban_after_failures", 3) or 3)
                    except Exception:
                        thr = 3
                    try:
                        tcfg0 = (self.config.get("trading", {}) or {})
                        ttl_min = int(tcfg0.get("ban_ttl_min", 60) or 60)
                    except Exception:
                        ttl_min = 60

                    reason = str(res.get("error") or (res.get("response") or {}).get("msg") or "order_failed")[:220]
                    try:
                        self.banlist.bump_failure_and_maybe_ban(
                            symbol,
                            ttl_sec=float(ttl_min) * 60.0,
                            threshold=int(thr),
                            reason=reason,
                            source="order",
                        )
                    except Exception:
                        pass

                    # пер-символьный warmup, чтобы стратегия не долбила ордерами каждую секунду
                    try:
                        swu = self.shared_state.get("symbol_warmup_until")
                        if not isinstance(swu, dict):
                            swu = {}
                            self.shared_state["symbol_warmup_until"] = swu
                        cd = float(tcfg0.get("cooldown_sec", 60) or 60)
                        swu[str(symbol)] = time.time() + max(10.0, float(cd))
                    except Exception:
                        pass

                    # фиксируем cooldown timestamp даже на ошибке
                    try:
                        if isinstance(getattr(self.portfolio, "last_signal_ts", None), dict):
                            self.portfolio.last_signal_ts[str(symbol)] = time.time()
                    except Exception:
                        pass
            except Exception:
                pass


            if res.get("ok"):
                try:
                    # dry-run: сразу применяем локально
                    if dry:
                        filled_qty = (float(usd) / float(last_price)) if float(last_price or 0.0) > 0 else 0.0
                        avg_px = float(last_price or 0.0)
                        notional = float(usd)
                        fee_usd = float(usd) * float(fee_rate)
                        self.portfolio.apply_local_fill(symbol=symbol, side="buy", qty=float(filled_qty), price=float(avg_px), fee=float(fee_usd))
                        self.portfolio.set_open_trade_buy_totals(symbol=symbol, trade_id=lot_trade_id, filled_qty=filled_qty, filled_qty_gross=filled_qty, notional_usd=notional, fee_usd=fee_usd, avg_px=avg_px, ts=time.time(), source=source, ord_id="DRYRUN", buy_score=float(conf or 0.0))
                    else:
                        ord_id = self._extract_ord_id(res)
                        if not ord_id:
                            return {"ok": False, "error": "OKX не вернул ordId. Ордер не подтверждён."}

                        pending = {
                            "side": "buy",
                            "ord_id": ord_id,
                            "created_ts": time.time(),
                            "updated_ts": time.time(),
                            "source": source,
                            "last_px": float(last_price or 0.0),
                            # сохраняем запрошенную сумму в quote (USDT),
                            # чтобы бухгалтерия BUY была стабильной даже если OKX /fills
                            # вернёт неполный/дублированный набор строк.
                            "req_usd": float(usd or 0.0),
                            "filled_qty": 0.0,
                            "notional_usd": 0.0,
                            "fee_usd": 0.0,
                            "avg_px": 0.0,
                            "trade_id": str(lot_trade_id),
                            "meta": dict(meta or {}),
                            "buy_score": float(conf or 0.0),
                        }
                        self.portfolio.set_pending(symbol, pending)
                        self._maybe_snapshot(name="BUY_SENT", payload={"symbol": symbol, "ord_id": ord_id, "res": res, "pending": pending})

                    self.portfolio.last_signal_ts[symbol] = time.time()
                except Exception:
                    pass

            log_event(self.data_dir, {"level": "INFO", "msg": "manual_trade", "extra": {"symbol": symbol, "side": side_l, "res": res}})
            return res

        if side_l == "sell":
            # После рестарта/PRV-reconnect ledger может быть неполным, но позиция на OKX есть.
            # В таком случае создаём "recovered" open_trade по факту баланса.
            tr_open = None
            try:
                tr_open = getattr(self.portfolio, "last_open_trade", lambda _s: None)(symbol)
            except Exception:
                tr_open = None

            pos = self.portfolio.position(symbol)

            if tr_open is None or float(getattr(tr_open, "buy_qty", 0.0) or 0.0) <= 0:
                # попытка восстановить по balances cache
                try:
                    base_ccy = str(symbol.split('-')[0]).upper().strip()
                except Exception:
                    base_ccy = ''
                qty_allowed = 0.0
                try:
                    if base_ccy:
                        qty_allowed = float(self._sell_allowed_qty(base_ccy) or 0.0)
                except Exception:
                    qty_allowed = 0.0
                if qty_allowed > 0:
                    # если локальная позиция пуста — поднимаем qty, чтобы apply_local_fill SELL смог её уменьшать
                    try:
                        if float(getattr(pos, 'qty', 0.0) or 0.0) <= 0:
                            pos.qty = float(qty_allowed)
                            pos.avg_price = float(last_price or 0.0)  # неизвестно, но для SELL допустимо
                            pos.opened_ts = float(time.time())
                            pos.last_price = float(last_price or 0.0)
                    except Exception:
                        pass
                    try:
                        tr_open = self.portfolio.ensure_recovered_open_trade(symbol=symbol, qty=float(qty_allowed), px_ref=float(last_price or 0.0), source='recovered')
                    except Exception:
                        tr_open = None
                if tr_open is None:
                    return {"ok": False, "error": f"NO_OPEN_TRADE: по {symbol} нет открытой сделки и баланс не обнаружен"}

            # FORCE SELL: если висит pending — пытаемся отменить и сбросить локально,
            # чтобы не блокировать экстренный выход.
            # ветка НИКОГДА не выполнялась и ордера зависали навсегда.

            if force:
                # ВАЖНО: force_exit не должен "спамить" SELL и отменять только что выставленный ордер.
                # Отменяем pending SELL только если он "застрял" (старше pending_stale_sec).
                try:
                    pending_stale_sec = float((self.config.get("trading", {}) or {}).get("pending_stale_sec", 60.0) or 60.0)
                except Exception:
                    pending_stale_sec = 60.0

                try:
                    if getattr(self.portfolio, "has_pending", None) is not None and self.portfolio.has_pending(symbol):
                        pending = (self.portfolio.pending_orders or {}).get(symbol) or {}
                        # Если private WS не подключен — НЕ пытаемся отменять/перевыставлять. Иначе будут десятки ордеров и отмен.
                        try:
                            prv = getattr(getattr(self.trader, "private", None), "ws_private", None)
                            prv_ok = False
                            if prv is not None:
                                st = prv.status()
                                prv_ok = str(st.get("connected","0")) == "1" and str(st.get("authed","0")) == "1"
                            if not prv_ok:
                                return {"ok": False, "error": f"PRV OFF: pending SELL по {symbol} уже есть. Ждём fills/reconcile."}
                        except Exception:
                            pass
                        try:
                            created_ts = float(pending.get("created_ts") or 0.0)
                        except Exception:
                            created_ts = 0.0

                        # Если pending свежий — не трогаем, просто ждём fills/reconcile
                        if created_ts and (time.time() - created_ts) < pending_stale_sec:
                            return {"ok": False, "error": f"Уже есть pending SELL по {symbol}. Ждём исполнение/скан fills."}
                        else:
                            ord_id = str(pending.get("ord_id") or "").strip()
                            if (not dry) and ord_id:
                                try:
                                    self.trader.private.cancel_order(inst_id=symbol, ord_id=ord_id)  # best-effort
                                except Exception:
                                    pass

                            try:
                                self.portfolio.clear_pending(symbol)
                            except Exception:
                                try:
                                    if isinstance(self.portfolio.pending_orders, dict) and symbol in self.portfolio.pending_orders:
                                        del self.portfolio.pending_orders[symbol]
                                except Exception:
                                    pass
                except Exception:
                    pass

            if pos.qty <= 0:
                return {"ok": False, "error": "Нет позиции для продажи (qty=0)."}

            # если остаток слишком мал ("пыль"), продажа на OKX обычно невозможна
            # из-за минимального размера ордера. Вместо ошибки API показываем понятную причину
            # и не блокируем последующие покупки (см. BUY-логику).
            try:
                dust_th = float(self.shared_state.get('dust_usd_threshold', 1.0) or 1.0)
            except Exception:
                dust_th = 1.0
            try:
                px_ref = float(last_price or pos.last_price or 0.0)
                if px_ref <= 0:
                    t = self.public.ticker(symbol)
                    px_ref = float(t.get('last') or t.get('lastPx') or 0.0)
            except Exception:
                px_ref = float(last_price or pos.last_price or 0.0)
            try:
                if self.portfolio.is_dust_qty(qty=float(pos.qty or 0.0), px=float(px_ref or 0.0), threshold_usd=float(dust_th)):
                    try:
                        self._dust_clear(symbol=symbol, last_px=float(px_ref or 0.0), reason="SELL_PRECHECK_DUST")
                    except Exception:
                        pass
                    return {"ok": True, "dust_cleared": True, "response": {"code": "0", "msg": "DUST_IGNORED"}}
            except Exception:
                pass

            # Если notional продажи меньше min_order_usd, НЕ шлём ордер на биржу (иначе будет spam ошибок).
            # Позицию считаем "пылью" и разрешаем двигаться дальше.
            try:
                try:
                    min_order_usd = float(tcfg.get('min_order_usd', 10.0) or 10.0)
                except Exception:
                    min_order_usd = 10.0
                notional_est = float(pos.qty or 0.0) * float(px_ref or 0.0)
                if min_order_usd > 0 and notional_est > 0 and notional_est < float(min_order_usd):
                    try:
                        self._dust_clear(symbol=symbol, last_px=float(px_ref or 0.0), reason=f"SELL_PRECHECK_MIN_NOTIONAL<{min_order_usd}")
                    except Exception:
                        pass
                    return {"ok": True, "dust_cleared": True, "response": {"code": "0", "msg": "MIN_NOTIONAL_DUST_IGNORED", "notional": notional_est}}
            except Exception:
                pass

            # pending guard
            try:
                if getattr(self.portfolio, "has_pending", None) is not None and self.portfolio.has_pending(symbol):
                    return {"ok": False, "error": f"По {symbol} уже есть ожидающий ордер (pending)."}
            except Exception as e:
                log_event(self.data_dir, {"level":"WARN","msg":"manual_trade_pending_guard_exception","extra":{"symbol":symbol,"error":str(e)}})
                ui_warn_once(self, "manual_trade_pending_guard", "Внутренняя ошибка проверки pending-ордера (см. logs).", ttl_sec=10.0, extra={"symbol":symbol})

            ord_type = str(tcfg.get("order_type", "market")).lower()
            if ord_type not in ("market", "limit", "post_only"):
                ord_type = "market"

            # чтобы не было путаницы "продали второй, а закрылась первая".
            lot_trade_id = ""
            lot_qty = 0.0
            try:
                last_tr = getattr(self.portfolio, "last_open_trade", lambda _s: None)(symbol)
                if last_tr is not None:
                    lot_trade_id = str(getattr(last_tr, "trade_id", "") or "")
                    lot_qty = float(getattr(last_tr, "buy_qty", 0.0) or 0.0)
            except Exception:
                lot_trade_id = ""
                lot_qty = 0.0

            # никогда не продаём "весь баланс" (в demo это приводит к распродаже демо-остатков).
            if float(lot_qty or 0.0) <= 0.0:
                return {"ok": False, "error": "Нет открытой сделки для продажи по этому символу. Ручной SELL продаёт только сделку (а не баланс)."}

            qty = float(lot_qty)

            # По умолчанию продаём 100% лота.
            try:
                sell_fraction = float((meta or {}).get('sell_fraction') or 1.0)
            except Exception:
                sell_fraction = 1.0
            if sell_fraction <= 0:
                sell_fraction = 1.0
            if sell_fraction > 1.0:
                sell_fraction = 1.0
            if sell_fraction < 0.999:
                qty = float(qty) * float(sell_fraction)
            # продаём по доступному объёму из OKX balances (учёт fee/dust + защита базовых активов)
            try:
                base_ccy = str(symbol).split('-')[0].upper()
            except Exception:
                base_ccy = ''
            try:
                # мягкий refresh (фоновый)
                self.request_balances_refresh()
                self._ensure_baseline_for_protected()
                allowed = float(self._sell_allowed_qty(base_ccy)) if base_ccy else 0.0
            except Exception:
                allowed = 0.0

            # делаем СИНХРОННЫЙ запрос баланса и пересчитываем allowed.
            try:
                intended_qty = float(qty)
                if base_ccy and intended_qty > 0 and allowed > 0 and allowed < (intended_qty * 0.98):
                    if self.private is not None:
                        resp = self.private.balances()
                        try:
                            self._update_balances_cache_trading(resp)
                        except Exception:
                            pass
                        allowed = float(self._sell_allowed_qty(base_ccy)) if base_ccy else allowed
                # если после синхронного обновления всё равно не хватает объёма — блокируем SELL (если не force),
                # иначе будем продавать кусок и UI/история начнут путаться.
                if base_ccy and intended_qty > 0 and allowed >= 0 and allowed < (intended_qty * 0.98):
                    if not force:
                        return {"ok": False, "error": f"SELL заблокирован: доступный объём {base_ccy}={allowed:.8f} меньше требуемого {intended_qty:.8f}. Вероятно, баланс не обновился/актив не в Trading."}
            except Exception:
                pass

            # можно продать вместе с текущим лотом, но ТОЛЬКО если это небольшая добавка.
            # Иначе можно случайно распродать ручные остатки пользователя.
            try:
                sweep_enabled = bool(tcfg.get('sell_sweep_dust_enabled', True))
            except Exception:
                sweep_enabled = True
            try:
                sweep_max_usd = float(tcfg.get('sell_sweep_max_usd', 5.0) or 5.0)
            except Exception:
                sweep_max_usd = 5.0
            try:
                if sweep_enabled and base_ccy and allowed > 0 and float(qty) > 0 and allowed > float(qty):
                    extra_qty = float(allowed) - float(qty)
                    extra_usd = float(extra_qty) * float(px_ref or 0.0)
                    if extra_usd > 0 and extra_usd <= float(sweep_max_usd):
                        qty = float(allowed)
            except Exception:
                pass

            if base_ccy and allowed > 0:
                qty = min(float(qty), float(allowed))
            elif base_ccy and (base_ccy in self._protect_ccy):
                base = float(self._baseline_ccy.get(base_ccy) or 0.0)
                return {"ok": False, "error": f"Продажа запрещена защитой {base_ccy}. База={base:.8f}, доступно={self._trading_avail(base_ccy):.8f}, к продаже={self._sell_allowed_qty(base_ccy):.8f}"}

            if qty <= 0:
                return {"ok": False, "error": "Недостаточно доступного объёма для продажи (учтена комиссия/защита)."}

            sz = f"{qty:.12f}".rstrip("0").rstrip(".")
            tgt_ccy = "base_ccy"
            px = None

            res = self.trader.place_order(dry_run=dry, inst_id=symbol, side="sell", sz=sz, ord_type=ord_type, px=px, tgt_ccy=tgt_ccy)

            # если OKX сообщает что пара недоступна — отключаем её и не пытаемся дальше
            try:
                self._maybe_disable_symbol_from_order_error(inst_id=symbol, res=res)
            except Exception:
                pass

            # считаем, что это "пыль": очищаем локально и НЕ залипаем на SELL/FORCE_EXIT.
            try:
                if not res.get("ok"):
                    resp = res.get("response") or {}
                    s_code = ""
                    if isinstance(resp, dict):
                        data = resp.get("data") or []
                        if isinstance(data, list) and data and isinstance(data[0], dict):
                            s_code = str(data[0].get("sCode") or "")
                    if s_code == "51020":
                        # OKX: "минимальный размер ордера".
                        # Это НЕ всегда "пыль". Если позиция/лот крупные — НЕЛЬЗЯ очищать локально,
                        # иначе мы "закроем" сделку без реального SELL (как было в логах по XLM-USDT).
                        try:
                            px_chk = float(px_ref or last_price or 0.0)
                        except Exception:
                            px_chk = 0.0
                        try:
                            dust_th2 = float(self.shared_state.get('dust_usd_threshold', 1.0) or 1.0)
                        except Exception:
                            dust_th2 = 1.0
                        try:
                            is_dust_now = self.portfolio.is_dust_qty(qty=float(pos.qty or 0.0), px=float(px_chk or 0.0), threshold_usd=float(dust_th2))
                        except Exception:
                            is_dust_now = False
                        if is_dust_now:
                            try:
                                self._dust_clear(symbol=symbol, last_px=float(px_chk or 0.0), reason="OKX_51020_MIN_ORDER")
                            except Exception:
                                pass
                            return {"ok": True, "dust_cleared": True, "response": resp, "warning": "OKX 51020: остаток признан пылью и очищен локально."}
                        # Не пыль: возвращаем понятную ошибку и НЕ очищаем сделку.
                        return {"ok": False, "error": "OKX 51020 (min order): попытка SELL слишком мала/некорректна. Позиция не пыль — локально НЕ очищаем. Требуется ресинхронизация балансов/лотности.", "response": resp}

            except Exception:
                pass

            try:
                if not res.get("ok"):
                    # пороги из config
                    try:
                        tcfg0 = (self.config.get("trading", {}) or {})
                        thr = int(tcfg0.get("ban_after_failures", 3) or 3)
                    except Exception:
                        thr = 3
                    try:
                        tcfg0 = (self.config.get("trading", {}) or {})
                        ttl_min = int(tcfg0.get("ban_ttl_min", 60) or 60)
                    except Exception:
                        ttl_min = 60

                    reason = str(res.get("error") or (res.get("response") or {}).get("msg") or "order_failed")[:220]
                    try:
                        self.banlist.bump_failure_and_maybe_ban(
                            symbol,
                            ttl_sec=float(ttl_min) * 60.0,
                            threshold=int(thr),
                            reason=reason,
                            source="order",
                        )
                    except Exception:
                        pass

                    # пер-символьный warmup, чтобы стратегия не долбила ордерами каждую секунду
                    try:
                        swu = self.shared_state.get("symbol_warmup_until")
                        if not isinstance(swu, dict):
                            swu = {}
                            self.shared_state["symbol_warmup_until"] = swu
                        cd = float(tcfg0.get("cooldown_sec", 60) or 60)
                        swu[str(symbol)] = time.time() + max(10.0, float(cd))
                    except Exception:
                        pass

                    # фиксируем cooldown timestamp даже на ошибке
                    try:
                        if isinstance(getattr(self.portfolio, "last_signal_ts", None), dict):
                            self.portfolio.last_signal_ts[str(symbol)] = time.time()
                    except Exception:
                        pass
            except Exception:
                pass


            if res.get("ok"):
                try:
                    if dry:
                        filled_qty = float(qty)
                        avg_px = float(last_price or 0.0)
                        notional = float(filled_qty) * float(avg_px)
                        fee_usd = float(notional) * float(fee_rate)
                        self.portfolio.apply_local_fill(symbol=symbol, side="sell", qty=float(filled_qty), price=float(avg_px), fee=float(fee_usd))
                        self.portfolio.on_bot_sell(symbol=symbol, qty=float(filled_qty), price=float(avg_px), fee_usd=float(fee_usd), usd_amount=float(notional), ts=time.time(), source=source, ord_id="DRYRUN", trade_id=str(lot_trade_id))
                    else:
                        ord_id = self._extract_ord_id(res)
                        if not ord_id:
                            return {"ok": False, "error": "OKX не вернул ordId. Ордер не подтверждён."}

                        pending = {
                            "side": "sell",
                            "ord_id": ord_id,
                            "created_ts": time.time(),
                            "updated_ts": time.time(),
                            "source": source,
                            "last_px": float(last_price or 0.0),
                            "filled_qty": 0.0,
                            "notional_usd": 0.0,
                            "fee_usd": 0.0,
                            "avg_px": 0.0,
                            "trade_id": str(lot_trade_id),
                            "meta": dict(meta or {}),
                            "buy_score": float(conf or 0.0),
                        }
                        self.portfolio.set_pending(symbol, pending)
                        self._maybe_snapshot(name="SELL_SENT", payload={"symbol": symbol, "ord_id": ord_id, "res": res, "pending": pending})

                    self.portfolio.last_signal_ts[symbol] = time.time()
                except Exception:
                    pass

            log_event(self.data_dir, {"level": "INFO", "msg": "manual_trade", "extra": {"symbol": symbol, "side": side_l, "res": res}})
            return res

        return {"ok": False, "error": f"Unknown side: {side}"}


    def _dust_clear(self, *, symbol: str, last_px: float, reason: str = "DUST_CLEAR") -> bool:
        """жёсткая очистка "пыли" (микро-остатков), которая мешает логике.

        Когда OKX не даёт продать из-за min order (например sCode=51020), или после SELL остаётся
        микроскопический остаток, мы:
        - закрываем открытую сделку синтетически (source="dust_clear"), чтобы UI/PnL не зависали;
        - обнуляем локальную позицию (qty/avg/opened/peak);
        - очищаем pending (best-effort).

        Важно: применяется ТОЛЬКО если остаток реально мал (USD <= dust_usd_threshold) ИЛИ если OKX явно
        вернул код 51020.
        """
        sym = str(symbol or "")
        if not sym:
            return False
        try:
            tcfg = (self.shared_state.get("trade_config") or {})
            dust_th = float(tcfg.get("dust_usd_threshold", 1.0) or 1.0)
        except Exception:
            dust_th = 1.0
        try:
            px = float(last_px or 0.0)
        except Exception:
            px = 0.0
        pos = self.portfolio.position(sym)
        try:
            q = float(getattr(pos, "qty", 0.0) or 0.0)
        except Exception:
            q = 0.0
        if q <= 0:
            # всё уже пусто
            try:
                self.portfolio.clear_pending(sym)
            except Exception:
                pass
            return True
        usd_val = (q * px) if (q > 0 and px > 0) else 0.0
        ok_to_clear = False
        try:
            if "51020" in str(reason):
                ok_to_clear = True
            elif dust_th > 0 and usd_val > 0 and usd_val <= float(dust_th):
                ok_to_clear = True
            elif dust_th > 0 and usd_val <= 0 and q > 0:
                # на демо eqUsd может быть 0 — считаем как пыль
                ok_to_clear = True
        except Exception:
            ok_to_clear = False
        if not ok_to_clear:
            return False

        # 1) best-effort: закрываем открытую сделку, если она есть
        try:
            tr = getattr(self.portfolio, "last_open_trade", lambda _s: None)(sym)
        except Exception:
            tr = None
        if tr is not None:
            try:
                rem = float(getattr(tr, "buy_qty", 0.0) or 0.0)
            except Exception:
                rem = 0.0
            if rem > 0 and px > 0:
                try:
                    self.portfolio.on_bot_sell(
                        symbol=sym,
                        trade_id=str(getattr(tr, "trade_id", "") or ""),
                        qty=rem,
                        price=px,
                        fee_usd=0.0,
                        fee_mode="quote",
                        fee_ccy="USDT",
                        fee_amt=0.0,
                        usd_amount=(rem * px),
                        ts=time.time(),
                        source="dust_clear",
                        ord_id="",
                    )
                except Exception:
                    pass

        # 2) обнуляем локальную позицию
        try:
            pos.qty = 0.0
            pos.avg_price = 0.0
            pos.opened_ts = 0.0
            pos.peak_price = 0.0
            pos.last_price = float(px or 0.0)
        except Exception:
            pass

        # 3) снимаем pending
        try:
            self.portfolio.clear_pending(sym)
        except Exception:
            try:
                if isinstance(self.portfolio.pending_orders, dict) and sym in self.portfolio.pending_orders:
                    del self.portfolio.pending_orders[sym]
            except Exception:
                pass

        # 4) логируем один раз (чтобы пользователь видел, что это было обработано)
        try:
            log_event(self.data_dir, {"level": "WARN", "msg": "dust_cleared", "extra": {"symbol": sym, "reason": str(reason), "qty": q, "usd": usd_val, "px": px}})
        except Exception:
            pass
        return True

    def _top_symbols_candidates(self, *, limit: int) -> List[str]:
            """Auto-TOP candidates ranked best-first.

            We intentionally return MORE than the desired channel count.
            This is critical for runtime refill (dead-swap / reconnect),
            where some candidates may already be in use or temporarily banned.

            Ranking goal: pick symbols that are liquid and volatile.
            "Volatile" here means *range* (high-low) rather than only
            "distance from day open" — this better matches "sinusoid" markets.

            используем ТОЛЬКО зашитый whitelist OKX (см. OKX_FIXED_SYMBOLS_V2365).
            - auto_top_count задаёт количество символов.
            - При Auto-TOP=ON проверяем "живость":
                * символ существует как LIVE SPOT USDT инструмент (public instruments),
                * есть тикер (last>0) в spot_tickers.
            Если живых меньше, чем нужно — возвращаем то, что есть.
            """
            scfg = self.config.get("symbols", {}) or {}
            try:
                limit = int(limit)
            except Exception:
                limit = 0
            limit = max(0, limit)

            try:
                base = load_symbol_universe(self.data_dir, fallback=list(OKX_EMBEDDED_SYMBOLS_V2365))
            except Exception:
                base = [str(x).strip().upper() for x in (OKX_EMBEDDED_SYMBOLS_V2365 or []) if str(x).strip()]

            # общие фильтры
            hard_exclude = {"AXS-USDT", "STRK-USDT"}  # исторически проблемные пары
            base = [x for x in base if x not in hard_exclude]

            # blacklist из конфига
            try:
                bl = scfg.get("symbol_blacklist", []) or []
                blset = set([str(x).strip().upper() for x in bl if str(x).strip()])
                if blset:
                    base = [x for x in base if x not in blset]
            except Exception:
                pass

            # disabled_symbols (runtime)
            try:
                ds = self.shared_state.get("disabled_symbols")
                if isinstance(ds, set) and ds:
                    base = [x for x in base if x not in ds]
            except Exception:
                pass

            # existence filter: LIVE SPOT USDT instruments
            available = None
            try:
                available = self._okx_available_spot_usdt_instids()
            except Exception as e:
                try:
                    self.ui_queue.put({"type": "warn", "symbol": "ENGINE", "warn": f"okx instruments fetch failed: {e}"})
                except Exception:
                    pass

            if available:
                base = [x for x in base if x in available]

            # --- Auto-TOP 2.0: liveness + scoring (volume/spread/volatility) ---
            scored: List[tuple] = []
            try:
                tickers = self.public.spot_tickers() or []
                tmap = {}
                for t in tickers:
                    inst = str(t.get("instId") or "").strip().upper()
                    if inst:
                        tmap[inst] = t

                def _f(x, d=0.0):
                    try:
                        return float(x)
                    except Exception:
                        return float(d)

                for inst in base:
                    t = tmap.get(inst)
                    if not t:
                        continue
                    last = _f(t.get("last"), 0.0)
                    if last <= 0.0:
                        continue

                    bid = _f(t.get("bidPx") or t.get("bid"), 0.0)
                    ask = _f(t.get("askPx") or t.get("ask"), 0.0)
                    spread_pct = 0.0
                    if bid > 0 and ask > 0 and ask >= bid:
                        mid = (bid + ask) / 2.0
                        if mid > 0:
                            spread_pct = (ask - bid) / mid * 100.0

                    # 24h volume in quote currency (USDT) if available
                    vol_usd = _f(t.get("volCcy24h") or t.get("volCcy"), 0.0)
                    # volatility proxy: prefer RANGE (high-low) over just "distance from open"
                    # this selects symbols with "sinusoid" movement.
                    high24 = _f(t.get("high24h") or t.get("high"), 0.0)
                    low24 = _f(t.get("low24h") or t.get("low"), 0.0)
                    range_pct = 0.0
                    if high24 > 0 and low24 > 0 and high24 >= low24:
                        mid = (high24 + low24) / 2.0
                        if mid > 0:
                            range_pct = (high24 - low24) / mid * 100.0

                    # secondary: abs change since day open (can be small in oscillation)
                    sod = _f(t.get("sodUtc0") or t.get("sod"), 0.0)
                    chg_pct = 0.0
                    if sod > 0:
                        chg_pct = abs((last - sod) / sod) * 100.0
                    # clamp
                    if chg_pct > 20.0:
                        chg_pct = 20.0
                    if range_pct > 40.0:
                        range_pct = 40.0

                    # score: prefer high volume, low spread, *range* (sinusoid), and some movement
                    import math
                    score = math.log1p(max(0.0, vol_usd)) + (range_pct * 0.22) + (chg_pct * 0.05) - (spread_pct * 3.0)

                    # penalties for known problematic micro-liquidity
                    if spread_pct > 1.5:
                        score -= 2.0
                    if vol_usd < 50_000:
                        score -= 1.0

                    # avoid flat / near-stable symbols (range too small)
                    try:
                        min_range = float(scfg.get("auto_top_min_range_pct", 0.25) or 0.25)
                    except Exception:
                        min_range = 0.25
                    if range_pct < float(min_range):
                        score -= 1.2

                    scored.append((score, inst))
            except Exception:
                scored = []

            # sort best first
            scored.sort(key=lambda x: x[0], reverse=True)
            ranked = [inst for _, inst in scored]

            out: List[str] = []
            for s in ranked:
                if limit > 0 and len(out) >= limit:
                    break
                try:
                    banned, _, _ = self.banlist.is_banned(s)
                    if banned:
                        continue
                except Exception:
                    pass
                out.append(s)

            # Fallback if tickers are missing or API hiccup: use cached TOP to avoid shrinking.
            # We still filter banned/disabled.
            if limit > 0 and len(out) < limit:
                try:
                    cached = self._load_top_cache(max_age_sec=24*3600.0) or []
                    for s in cached:
                        if len(out) >= limit:
                            break
                        s2 = str(s or "").strip().upper()
                        if not s2 or s2 in out:
                            continue
                        try:
                            ds = self.shared_state.get("disabled_symbols")
                            if isinstance(ds, set) and s2 in ds:
                                continue
                        except Exception:
                            pass
                        try:
                            banned, _, _ = self.banlist.is_banned(s2)
                            if banned:
                                continue
                        except Exception:
                            pass
                        out.append(s2)
                except Exception:
                    pass

            return out


    def _top_symbols_now(self) -> List[str]:
            """Auto-TOP symbols (exactly auto_top_count if possible)."""
            scfg = self.config.get("symbols", {}) or {}
            try:
                count = int(scfg.get("auto_top_count", 20))
            except Exception:
                count = 20
            count = max(0, count)

            # take more candidates to avoid overlap holes
            cand = self._top_symbols_candidates(limit=max(count * 5, count))
            out = list(cand[:count])

            # если не набрали — предупредим
            try:
                if count > 0 and len(out) < count:
                    self.ui_queue.put({"type": "warn", "symbol": "ENGINE", "warn": f"Auto-TOP: доступно {len(out)}/{count} кандидатов. Проверь OKX/сеть/доступность тикеров/бан-лист."})
            except Exception:
                pass

            # cache for future fallback
            try:
                if out:
                    self._save_top_cache(out)
            except Exception:
                pass

            return out


    def _fixed_symbols_now(self, *, count: Optional[int] = None) -> List[str]:
        """Список символов при Auto-TOP=OFF.

        FIX1B: если часть символов из whitelist заблокирована (disabled / banlist / runtime stop / blacklist),
        подставляем следующие из whitelist, чтобы поддерживать заданное количество.
        """
        scfg = self.config.get("symbols", {}) or {}
        if count is None:
            try:
                count = int(scfg.get("auto_top_count", 20))
            except Exception:
                count = 20
        try:
            count = int(count)
        except Exception:
            count = 20
        count = max(0, count)

        try:
            base = load_symbol_universe(self.data_dir, fallback=list(OKX_EMBEDDED_SYMBOLS_V2365))
        except Exception:
            base = [str(x).strip().upper() for x in (OKX_EMBEDDED_SYMBOLS_V2365 or []) if str(x).strip()]

        hard_exclude = {"AXS-USDT", "STRK-USDT"}
        base = [x for x in base if x not in hard_exclude]

        try:
            bl = scfg.get("symbol_blacklist", []) or []
            blset = set([str(x).strip().upper() for x in bl if str(x).strip()])
            if blset:
                base = [x for x in base if x not in blset]
        except Exception:
            pass

        try:
            ds = self.shared_state.get("disabled_symbols")
            if isinstance(ds, set) and ds:
                base = [x for x in base if x not in ds]
        except Exception:
            pass

        try:
            rs = self.shared_state.get("runtime_stop_symbols")
            if isinstance(rs, set) and rs:
                base = [x for x in base if x not in rs]
        except Exception:
            pass

        out: List[str] = []
        for s in base:
            if len(out) >= count:
                break
            try:
                banned, _, _ = self.banlist.is_banned(s)
                if banned:
                    continue
            except Exception:
                pass
            out.append(s)
        return out


    def _fill_symbols_to_count(self, symbols: List[str], *, count: int, auto_top: bool) -> List[str]:
        """Дозаполнить список символов до count, пропуская заблокированные.

        Используется для:
        - старта (чтобы не стартовать с "дырками" при уже заблокированных символах)
        - runtime-супервизора (замена заблокированных символов, поддержание заданного количества)
        """
        try:
            count = int(count)
        except Exception:
            count = 0
        count = max(0, count)

        out: List[str] = []
        seen: Set[str] = set()

        # 1) берём то, что уже есть, но выкидываем заблокированное
        for s in (symbols or []):
            s2 = str(s or "").strip().upper()
            if not s2 or s2 in seen:
                continue
            seen.add(s2)
            try:
                ds = self.shared_state.get("disabled_symbols")
                if isinstance(ds, set) and s2 in ds:
                    continue
            except Exception:
                pass
            try:
                rs = self.shared_state.get("runtime_stop_symbols")
                if isinstance(rs, set) and s2 in rs:
                    continue
            except Exception:
                pass
            try:
                banned, _, _ = self.banlist.is_banned(s2)
                if banned:
                    continue
            except Exception:
                pass
            out.append(s2)
            if len(out) >= count:
                return out

        # 2) дозаполняем из кандидатов (auto-top или fixed whitelist)
        # IMPORTANT: берем кандидатов с запасом, иначе при совпадениях/банах могут появляться "дырки".
        if bool(auto_top):
            cand = self._top_symbols_candidates(limit=max(count * 6, count))
        else:
            cand = self._fixed_symbols_now(count=None)
        for s in (cand or []):
            s2 = str(s or "").strip().upper()
            if not s2 or s2 in seen:
                continue
            seen.add(s2)
            try:
                ds = self.shared_state.get("disabled_symbols")
                if isinstance(ds, set) and s2 in ds:
                    continue
            except Exception:
                pass
            try:
                rs = self.shared_state.get("runtime_stop_symbols")
                if isinstance(rs, set) and s2 in rs:
                    continue
            except Exception:
                pass
            try:
                banned, _, _ = self.banlist.is_banned(s2)
                if banned:
                    continue
            except Exception:
                pass
            out.append(s2)
            if len(out) >= count:
                break
        return out[:count]

    def _top_cache_path(self) -> str:
        try:
            os.makedirs(os.path.join(self.data_dir, "cache"), exist_ok=True)
        except Exception:
            pass
        return os.path.join(self.data_dir, "cache", "top_symbols_cache.json")

    def _load_top_cache(self, max_age_sec: float = 24 * 3600.0) -> List[str]:
        """Load cached TOP symbols to avoid blocking START.

        We intentionally keep this very defensive:
        - If file is missing/corrupted/too old -> return []
        - Validate each symbol (simple format + '-USDT')
        """
        p = self._top_cache_path()
        try:
            if not os.path.exists(p):
                return []
            raw = open(p, "r", encoding="utf-8").read()
            j = json.loads(raw)
            ts = float(j.get("ts", 0.0) or 0.0)
            if ts <= 0 or (time.time() - ts) > float(max_age_sec):
                return []
            arr = j.get("symbols", [])
            if not isinstance(arr, list):
                return []
            out: List[str] = []
            for s in arr:
                s2 = str(s or "").strip().upper()
                if not s2 or "-" not in s2:
                    continue
                if not s2.endswith("-USDT"):
                    continue
                out.append(s2)
            return out
        except Exception:
            return []

    def _save_top_cache(self, symbols: List[str]) -> None:
        p = self._top_cache_path()
        try:
            payload = {"ts": time.time(), "symbols": [str(x).upper() for x in (symbols or []) if str(x).strip()]}
            with open(p, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
        except Exception:
            pass

    def _okx_available_spot_usdt_instids(self, cache_ttl_sec: float = 600.0) -> Set[str]:
            """Return set of OKX SPOT USDT instIds that are in LIVE state.

            Используется для фильтрации auto-top. Кэшируем, чтобы не бить public endpoint слишком часто.
            """
            now = time.time()
            if self._okx_spot_inst_cache is not None and (now - float(self._okx_spot_inst_cache_ts)) < float(cache_ttl_sec):
                return set(self._okx_spot_inst_cache)

            data = self.public.instruments_spot(inst_id=None)
            out: Set[str] = set()
            for d in (data or []):
                try:
                    inst_id = str(d.get('instId', '')).strip()
                    if not inst_id or not inst_id.endswith('-USDT'):
                        continue
                    state = str(d.get('state', '')).lower().strip()
                    # OKX обычно использует state=live для торгуемых инструментов
                    if state and state != 'live':
                        continue
                    # дополнительная защита по quoteCcy, если поле присутствует
                    q = str(d.get('quoteCcy', '')).upper().strip()
                    if q and q != 'USDT':
                        continue
                    out.add(inst_id)
                except Exception:
                    continue

            # учитываем blacklist пользователя (если задан)
            try:
                scfg = self.config.get('symbols', {}) or {}
                bl = set(str(x).strip() for x in (scfg.get('symbol_blacklist', []) or []) if str(x).strip())
                if bl:
                    out = set([x for x in out if x not in bl])
            except Exception:
                pass

            self._okx_spot_inst_cache = set(out)
            self._okx_spot_inst_cache_ts = now
            return out

    
    # ---------------- runtime add symbol (without stopping engine) ----------------
    def add_symbol_runtime(self, symbol: str) -> tuple[bool, str, str]:
        """Add one symbol to monitoring/trading while engine is running.

        Returns (ok, instId, reason).
        """
        inst = str(symbol or "").strip().upper()
        if not inst:
            return False, "", "empty symbol"
        if "-" not in inst:
            inst = inst + "-USDT"
        if not inst.endswith("-USDT"):
            return False, inst, "only -USDT spot supported"

        # existence filter: must be live SPOT instrument
        try:
            avail = self._okx_available_spot_usdt_instids()
            if inst not in avail:
                return False, inst, "pair is not LIVE SPOT-USDT on OKX"
        except Exception:
            # if instruments endpoint failed, we still allow adding (user knows the pair)
            pass

        # do not add disabled
        try:
            ds = self.shared_state.get('disabled_symbols')
            if isinstance(ds, set) and inst in ds:
                return False, inst, "pair is disabled (untradeable)"
        except Exception:
            pass

        # Update config symbols list (so it persists)
        try:
            scfg = self.config.get("symbols", {}) or {}
            lst = scfg.get("list") or []
            if not isinstance(lst, list):
                lst = []
            if inst not in [str(x).upper() for x in lst]:
                lst.append(inst)
            scfg["list"] = lst
            self.config["symbols"] = scfg
        except Exception:
            pass

        # per-symbol warmup
        try:
            warm = float((self.config.get("trading", {}) or {}).get("warmup_sec", 60) or 60)
            wb = self.shared_state.get("warmup_by_symbol", {})
            if not isinstance(wb, dict):
                wb = {}
            wb[inst] = time.time() + warm
            self.shared_state["warmup_by_symbol"] = wb
        except Exception:
            pass

        # start missing channel
        try:
            self._ensure_channels([inst])
        except Exception as e:
            return False, inst, f"start channel failed: {e}"

        try:
            self.ui_queue.put({"type": "info", "symbol": "ENGINE", "info": f"symbol added: {inst}"})
        except Exception:
            pass
        return True, inst, ""

    def apply_runtime_config(self) -> Dict[str, Any]:
        from engine.controller_runtime import apply_runtime_config_impl
        return apply_runtime_config_impl(self)

    def _ensure_channels(self, symbols: List[str]):
        """Start missing symbol channels while engine is running."""
        strat_cfg = (self.config.get("strategy", {}) or {})
        strat_name = str(strat_cfg.get("name", "StrategyV3"))
        strat_params = (strat_cfg.get("params") or {})

        # для Decision Log / Replay: фиксируем текущую стратегию и параметры
        try:
            self.shared_state['strategy_name'] = strat_name
            self.shared_state['strategy_params'] = strat_params
        except Exception:
            pass
        from engine.symbol_channel import SymbolChannel

        with self._lock:
            existing = set(self.channels.keys())

        desired = set(symbols or [])
        try:
            rs = self.shared_state.get('runtime_stop_symbols')
            if isinstance(rs, set):
                for s in list(desired):
                    rs.discard(s)
        except Exception:
            pass

        to_stop = [s for s in (existing - desired)]
        if to_stop:
            for sym in to_stop:
                sym = str(sym or '').strip().upper()
                if not sym:
                    continue
                # безопасность: не трогаем символы с открытой позицией или pending-ордером
                try:
                    posd = self.portfolio.position_dict(sym) or {}
                    qty = float(posd.get('base_qty') or posd.get('qty') or 0.0)
                except Exception:
                    qty = 0.0
                try:
                    pend = bool(self.portfolio.has_pending(sym))
                except Exception:
                    pend = False
                if qty > 0.0 or pend:
                    try:
                        self.ui_queue.put({"type":"warn","symbol":"ENGINE","warn":f"Не отключаю {sym}: есть открытая позиция или pending ордер"})
                    except Exception:
                        pass
                    continue

                try:
                    rs = self.shared_state.get('runtime_stop_symbols')
                    if isinstance(rs, set):
                        rs.add(sym)
                except Exception:
                    pass

                ch = None
                try:
                    with self._lock:
                        ch = self.channels.pop(sym, None)
                except Exception:
                    ch = None
                # Иначе UI «висит», а после разморозки возможны импульсные BUY.
                try:
                    if ch is not None:
                        ch.join(timeout=0.05)
                except Exception:
                    pass
                # LOG: Auto-TOP / reconcile каналов
                try:
                    log_event(self.data_dir, {
                        "level": "INFO",
                        "msg": "channel_removed",
                        "extra": {
                            "symbol": sym,
                            "reason": "reconcile_or_count_change",
                            "had_pos_qty": qty,
                            "had_pending": pend,
                        },
                    })
                except Exception:
                    pass
                try:
                    self.ui_queue.put({"type":"warn","symbol":"ENGINE","warn":f"Отключил канал {sym} (reconcile списка/кол-ва)"})
                except Exception:
                    pass

            # перечитываем existing после остановок
            with self._lock:
                existing = set(self.channels.keys())

        tcfg = (self.config.get("trading", {}) or {})
        economy = bool(tcfg.get("api_economy_mode", False))
        fetch_candles_every = float(tcfg.get("fetch_candles_every", 15.0) or 15.0)
        fetch_book_every = float(tcfg.get("fetch_book_every", 2.0) or 2.0)
        fetch_trades_every = float(tcfg.get("fetch_trades_every", 2.0) or 2.0)
        if economy:
            fetch_candles_every = float(tcfg.get("economy_candles_every", 60.0) or 60.0)
            fetch_book_every = float(tcfg.get("economy_book_every", 30.0) or 30.0)
            fetch_trades_every = float(tcfg.get("economy_trades_every", 30.0) or 30.0)


        try:
            tcfg_local = (self.config.get("trading", {}) or {})
            new_warmup_sec = int(tcfg_local.get("new_symbol_warmup_sec", 60) or 60)
        except Exception:
            new_warmup_sec = 60

        for sym in symbols:
            if sym in existing:
                continue
            # если символ в permanent disable — не запускаем.
            try:
                ds = self.shared_state.get('disabled_symbols')
                if isinstance(ds, set) and sym in ds:
                    continue
            except Exception:
                pass
            # временный бан (TTL)
            try:
                banned, until, reason = self.banlist.is_banned(sym)
                if banned:
                    self.ui_queue.put({"type": "warn", "symbol": sym, "warn": f"Символ временно заблокирован до {time.strftime('%H:%M:%S', time.localtime(until))}: {reason}"})
                    try:
                        log_event(self.data_dir, {"level":"INFO","msg":"channel_skip_banned","extra":{"symbol":sym,"until":float(until or 0.0),"reason":str(reason or "")}})
                    except Exception:
                        pass
                    continue
            except Exception:
                pass
            try:
                inst = SymbolStrategyInstance(self.registry.create(strat_name), strategy_params=strat_params)
                ch = SymbolChannel(
                    symbol=sym,
                    public=self.public,
                    public_ws=self.public_ws,
                    strategy_instance=inst,
                    portfolio=self.portfolio,
                    ui_queue=self.ui_queue,
                    signal_queue=self.signal_queue,
                    shared_state=self.shared_state,
                    stop_event=self.stop_event,
                    fetch_candles_every=fetch_candles_every,
                    fetch_book_every=fetch_book_every,
                    fetch_trades_every=fetch_trades_every,
                )
                with self._lock:
                    self.channels[sym] = ch
                # фиксируем частный разгон: новый символ не должен моментально выдавать BUY.
                try:
                    swu = self.shared_state.get("symbol_warmup_until")
                    if not isinstance(swu, dict):
                        swu = {}
                        self.shared_state["symbol_warmup_until"] = swu
                    swu[sym] = time.time() + max(0, int(new_warmup_sec))
                except Exception:
                    pass
                ch.start()
                try:
                    log_event(self.data_dir, {"level":"INFO","msg":"channel_added","extra":{"symbol":sym,"auto_top":bool(auto_top),"fetch_candles_every":fetch_candles_every,"fetch_book_every":fetch_book_every,"fetch_trades_every":fetch_trades_every}})
                except Exception:
                    pass
            except Exception as e:
                self.ui_queue.put({"type": "warn", "symbol": "ENGINE", "warn": f"start channel {sym}: {e}"})

    def _prune_dead_channels(self) -> int:
        """Удаляет из registry каналы, которые завершились или были отключены.

        Важно: без этого UI продолжает считать "каналы" по длине dict,
        даже если отдельные SymbolChannel уже завершились (например из-за untradeable).
        """
        removed = 0
        try:
            disabled = self.shared_state.get('disabled_symbols')
            disabled_set = disabled if isinstance(disabled, set) else set()
        except Exception:
            disabled_set = set()

        try:
            with self._lock:
                for sym, ch in list(self.channels.items()):
                    try:
                        if sym in disabled_set:
                            self.channels.pop(sym, None)
                            removed += 1
                            continue
                        if hasattr(ch, 'is_alive') and (not ch.is_alive()):
                            self.channels.pop(sym, None)
                            removed += 1
                            continue
                    except Exception:
                        continue
        except Exception:
            return 0
        return removed


    # ---------------- кэш торговых комиссий по инструментам (для точного PnL) ----------------
    def _maybe_refresh_fee_rates(self, *, max_symbols: int = 25, min_age_sec: float = 3600.0) -> None:
        """Подтягивает trade-fee OKX и кэширует по instId.

        Зачем:
        - комиссии на OKX могут отличаться по инструментам/аккаунтам,
        - мгновенный PnL должен считать ожидаемую комиссию продажи.
        Важно: вызываем редко и только при наличии private клиента.
        """
        if self.trader is None or getattr(self.trader, 'private', None) is None:
            return
        try:
            now = time.time()
            fr = self.shared_state.get('fee_rate_by_symbol')
            ts_map = self.shared_state.get('fee_rate_ts')
            if not isinstance(fr, dict):
                fr = {}
                self.shared_state['fee_rate_by_symbol'] = fr
            if not isinstance(ts_map, dict):
                ts_map = {}
                self.shared_state['fee_rate_ts'] = ts_map

            syms: List[str] = []
            try:
                syms.extend(list((self.channels or {}).keys()))
            except Exception:
                pass
            try:
                syms.extend(list((self.portfolio.open_trades or {}).keys()))
            except Exception:
                pass
            # уникализируем, но сохраняем порядок
            seen = set()
            uniq: List[str] = []
            for s in syms:
                s = str(s)
                if s and s not in seen:
                    seen.add(s)
                    uniq.append(s)
            uniq = uniq[:max(0, int(max_symbols))]

            for inst_id in uniq:
                try:
                    last_ts = float(ts_map.get(inst_id) or 0.0)
                except Exception:
                    last_ts = 0.0
                if last_ts > 0 and (now - last_ts) < float(min_age_sec or 0.0):
                    continue
                try:
                    rate = float(self.trader.get_trade_fee_rate(inst_id=inst_id) or 0.0)
                    rate = abs(rate)
                    if rate <= 0:
                        continue
                    fr[inst_id] = rate
                    ts_map[inst_id] = now
                except Exception:
                    continue
        except Exception:
            return


    # ---------------- WS price pump (ATE 2.0 foundation) ----------------

    def _start_ws_price_pump(self, symbols: List[str]):
        """Pump OKX public WS tickers into shared_state['last_prices'].

        До этого патча public WS запускался, но результаты никак не использовались.
        Из-за этого мониторинг/фильтры опирались на REST, появлялись лаги и нагрузка.
        """

        if self._ws_pump_thread and self._ws_pump_thread.is_alive():
            return

        # Default 250ms. Можно менять в data/config.json: ws.public_pump_ms
        wcfg = (self.config.get("ws", {}) or {})
        try:
            interval_ms = int(wcfg.get("public_pump_ms", 250))
        except Exception:
            interval_ms = 250
        interval_ms = max(50, min(2000, interval_ms))

        self._ws_pump_stop.clear()

        def run():
            base_symbols = list(symbols)
            status_last_ui = 0.0
            while not self.stop_event.is_set() and not self._ws_pump_stop.is_set():
                # status for UI/logs
                try:
                    st = self.public_ws.status()
                    ws_status = {
                        "public_connected": st.get("connected") == "1",
                        "public_error": st.get("last_error") or "",
                        "public_prices": st.get("prices") or "0",
                        "public_msgs_per_sec": st.get("msg_per_sec") or "0",
                        "public_last_msg_age_sec": st.get("last_msg_age_sec") or "0",
                    }
                    # Private WS (orders/fills/account) is optional. If enabled and keys exist,
                    # show operator-visible status and use it for state sync acceleration.
                    try:
                        if getattr(self, 'private_ws', None) is not None:
                            pst = self.private_ws.status() or {}
                            ws_status.update({
                                "private_connected": pst.get("connected") == "1",
                                "private_authed": pst.get("authed") == "1",
                                "private_error": pst.get("last_error") or "",
                                "private_msgs_per_sec": pst.get("msg_per_sec") or "0",
                                "private_last_msg_age_sec": pst.get("last_msg_age_sec") or "0",
                            })
                        else:
                            ws_status.update({
                                "private_connected": False,
                                "private_authed": False,
                                "private_error": "",
                                "private_msgs_per_sec": "0",
                                "private_last_msg_age_sec": "0",
                            })
                    except Exception:
                        pass
                    self.shared_state["ws_status"] = ws_status

                    # visible proof for operator: push status to UI every ~2s
                    try:
                        now_ui = time.time()
                        if (now_ui - float(status_last_ui)) >= 2.0:
                            self.ui_queue.put({"type": "ws_status", "ws_public": ws_status, "ws_private": ws_status})
                            status_last_ui = now_ui
                    except Exception:
                        pass
                except Exception:
                    pass

                # runtime symbols can change; prefer shared_state list if exists
                try:
                    cur_syms = list((self.shared_state.get("symbols") or []) or [])
                    syms = cur_syms if cur_syms else base_symbols
                except Exception:
                    syms = base_symbols

                try:
                    lp = self.shared_state.get("last_prices")
                    if not isinstance(lp, dict):
                        lp = {}
                except Exception:
                    lp = {}

                now = time.time()
                for sym in syms:
                    try:
                        q = self.public_ws.get_quote(sym)
                        tsq = float(q.get("ts") or 0.0)
                        if tsq <= 0:
                            continue
                        last = float(q.get("last") or 0.0)
                        bid = float(q.get("bid") or 0.0)
                        ask = float(q.get("ask") or 0.0)
                        if last <= 0 and bid > 0 and ask > 0:
                            last = (bid + ask) / 2.0
                        if last <= 0:
                            continue
                        lp[sym] = {
                            "last": last,
                            "bid": bid,
                            "ask": ask,
                            "ts": tsq or now,
                            "provider": "OKX_WS",
                        }
                    except Exception:
                        continue

                try:
                    self.shared_state["last_prices"] = lp
                except Exception:
                    pass

                # sleep in small chunks to react quickly to STOP
                total_sleep = interval_ms / 1000.0
                step = 0.05
                n = max(1, int(total_sleep / step))
                for _ in range(n):
                    if self.stop_event.is_set() or self._ws_pump_stop.is_set():
                        break
                    time.sleep(step)

        self._ws_pump_thread = threading.Thread(target=run, daemon=True)
        self._ws_pump_thread.start()


    def _start_balance_poll(self):
        if self._balance_thread and self._balance_thread.is_alive():
            return
        if self.private is None:
            return

        def parse_balance(resp: Dict[str, Any]) -> tuple[Optional[float], Optional[float], float, int]:
            try:
                data = (resp.get("data") or [])
                if not data:
                    return None, None, 0.0, 0
                acc = data[0] or {}
                total_eq = acc.get("totalEq") or acc.get("totalEquity")
                total_eq_f = float(total_eq) if total_eq is not None else None

                cash_usdt = None
                assets_usd = 0.0
                assets_cnt = 0
                details = acc.get("details") or []
                for d in details:
                    try:
                        ccy = str(d.get('ccy','')).upper()
                        if ccy and ccy != 'USDT':
                            v = d.get('eqUsd') or d.get('usdEq') or d.get('eq')
                            if v is not None:
                                assets_usd += float(v)
                                assets_cnt += 1
                    except Exception:
                        pass
                for d in details:
                    if str(d.get("ccy","")).upper() == "USDT":
                        # prefer availEq if present, else cashBal
                        v = d.get("availEq") or d.get("cashBal") or d.get("eq")
                        if v is not None:
                            cash_usdt = float(v)
                        break
                return total_eq_f, cash_usdt, float(assets_usd or 0.0), int(assets_cnt or 0)
            except Exception:
                return None, None, 0.0, 0

        def run():
            tick = 0
            while not self.stop_event.is_set():
                try:
                    resp = self.private.balances()
                    total_eq, cash_usdt, assets_usd, assets_cnt = parse_balance(resp)
                    self.portfolio.update_from_okx_balance(total_equity=total_eq, cash_usdt=cash_usdt, assets_usd=assets_usd, assets_count=assets_cnt)

                    try:
                        self._update_session_realized_pnl()
                    except Exception:
                        pass

                    try:
                        self._prv_health_check()
                    except Exception:
                        pass

                    # сохранить детальные балансы Trading
                    try:
                        self._update_balances_cache_trading(resp)
                    except Exception:
                        pass

                    # Funding баланс опрашиваем реже (раз в 30 сек)
                    tick += 1
                    if tick % 6 == 0:
                        try:
                            fr = self.private.asset_balances()
                            self._update_balances_cache_funding(fr)
                        except Exception:
                            pass
                        # периодически обновляем кэш торговых комиссий (для точного PnL)
                        try:
                            self._maybe_refresh_fee_rates()
                        except Exception:
                            pass
                except Exception as e:
                    # не спамим UI каждую секунду
                    self.ui_queue.put({"type":"warn","symbol":"OKX","warn":f"Баланс OKX: {e}"})

                # чистим завершившиеся/отключённые каналы, чтобы UI показывал
                # реальное число активных котировок и auto-top мог "урезать" список.
                try:
                    removed = self._prune_dead_channels()
                    if removed:
                        scfg = self.config.get('symbols', {}) or {}
                        if bool(scfg.get('auto_top')):
                            self.ui_queue.put({"type":"top_symbols", "symbols": list(self.channels.keys())})
                except Exception:
                    pass
                # poll interval
                for _ in range(5):
                    if self.stop_event.is_set():
                        break
                    time.sleep(1)

        self._balance_thread = threading.Thread(target=run, daemon=True)
        self._balance_thread.start()

    # ---------------- balances cache + protection baseline ----------------
    def _update_balances_cache_trading(self, resp: Dict[str, Any]):
        """Парсит /api/v5/account/balance."""
        data = (resp or {}).get('data') or []
        if not data:
            return
        acc = data[0] or {}
        details = acc.get('details') or []
        out: dict = {}
        for d in details:
            try:
                ccy = str(d.get('ccy') or '').upper().strip()
                if not ccy:
                    continue
                total = float(d.get('cashBal') or d.get('bal') or d.get('eq') or 0.0)
                avail = float(d.get('availBal') or d.get('availEq') or d.get('avail') or 0.0)
                usd = float(d.get('eqUsd') or d.get('usdEq') or 0.0)
                out[ccy] = {'total': total, 'avail': avail, 'usd': usd}
            except Exception:
                continue
        with self._lock:
            self._balances_cache['trading'] = out

    def _update_balances_cache_funding(self, resp: Dict[str, Any]):
        """Парсит /api/v5/asset/balances."""
        data = (resp or {}).get('data') or []
        out: dict = {}
        for d in data:
            try:
                ccy = str(d.get('ccy') or '').upper().strip()
                if not ccy:
                    continue
                total = float(d.get('bal') or d.get('cashBal') or 0.0)
                avail = float(d.get('availBal') or total)
                usd = float(d.get('eqUsd') or d.get('usdEq') or 0.0)
                out[ccy] = {'total': total, 'avail': avail, 'usd': usd}
            except Exception:
                continue
        with self._lock:
            self._balances_cache['funding'] = out

    def request_balances_refresh(self):
        """Принудительно обновить balances cache (для UI / Активы)."""
        if self.private is None:
            return
        try:
            r = self.private.balances()
            self._update_balances_cache_trading(r)
        except Exception:
            pass
        try:
            r2 = self.private.asset_balances()
            self._update_balances_cache_funding(r2)
        except Exception:
            pass

    def _ensure_baseline_for_protected(self):
        """Фиксирует базовый остаток для защищённых валют.

        - Для BTC пользователь хочет защитить *ровно* 1 BTC по умолчанию.
          При балансе 1.00015 BTC продаваться может только 0.00015.
        - Поэтому baseline должен быть настраиваемым (trading.sell_protect_baseline),
          а не всегда равным текущему total на момент START.
        """
        if not self._protect_ccy:
            return

        # baseline-значения из конфига (например {"BTC": 1.0})
        try:
            tcfg = self.config.get('trading', {}) or {}
            baseline_cfg = tcfg.get('sell_protect_baseline', {}) or {}
        except Exception:
            baseline_cfg = {}

        with self._lock:
            trading = dict(self._balances_cache.get('trading') or {})

        for ccy in list(self._protect_ccy):
            # если baseline задан в конфиге — используем его (и перезаписываем кэш baseline)
            b = None
            try:
                if ccy in baseline_cfg:
                    b = float(baseline_cfg.get(ccy) or 0.0)
            except Exception:
                b = None

            if b is not None and b > 0:
                self._baseline_ccy[ccy] = b
                continue

            # иначе — фиксируем как было раньше: текущий total на момент START
            if ccy not in self._baseline_ccy:
                try:
                    self._baseline_ccy[ccy] = float((trading.get(ccy) or {}).get('total') or 0.0)
                except Exception:
                    self._baseline_ccy[ccy] = 0.0

    def set_protect_currency(self, ccy: str, enabled: bool):
        ccy = str(ccy or '').upper().strip()
        if not ccy:
            return
        if enabled:
            self._protect_ccy.add(ccy)
            # baseline фиксируем при первом включении (если уже есть балансы)
            self.request_balances_refresh()
            self._ensure_baseline_for_protected()
        else:
            if ccy in self._protect_ccy:
                self._protect_ccy.remove(ccy)

        # сохранить в конфиг
        try:
            tcfg = self.config.setdefault('trading', {})
            tcfg['sell_protect_ccy'] = sorted(list(self._protect_ccy))
        except Exception:
            pass

    def get_protect_ccy(self) -> List[str]:
        return sorted(list(self._protect_ccy))

    def get_balance_baseline(self) -> dict:
        return dict(self._baseline_ccy or {})

    def get_balances_snapshot(self) -> List[dict]:
        """Снимок для UI: объединяем Trading и Funding (как отдельные строки)."""
        with self._lock:
            trading = dict(self._balances_cache.get('trading') or {})
            funding = dict(self._balances_cache.get('funding') or {})

        rows: List[dict] = []

        try:
            dust_th = float(self.shared_state.get('dust_usd_threshold', 1.0) or 1.0)
        except Exception:
            dust_th = 1.0

        def add_rows(src: str, m: dict):
            for ccy, v in (m or {}).items():
                try:
                    total = float((v or {}).get('total') or 0.0)
                    avail = float((v or {}).get('avail') or 0.0)
                    usd = float((v or {}).get('usd') or 0.0)
                except Exception:
                    total, avail, usd = 0.0, 0.0, 0.0
                prot = (ccy in self._protect_ccy)
                base = float(self._baseline_ccy.get(ccy) or 0.0) if prot else 0.0
                sellable = max(0.0, avail - base) if prot else avail
                # подсказки по "пыли": сколько USD-эквивалента можно реально продать
                # и является ли это меньше порога (тогда продавать нельзя / не блокируем BUY).
                sellable_usd = 0.0
                try:
                    if total > 0 and usd > 0 and sellable > 0:
                        sellable_usd = usd * (sellable / total)
                except Exception:
                    sellable_usd = 0.0
                is_dust = False
                try:
                    if (sellable_usd > 0 and sellable_usd < dust_th) or (usd > 0 and usd < dust_th):
                        is_dust = True
                except Exception:
                    is_dust = False
                rows.append({'ccy': ccy, 'total': total, 'avail': avail, 'usd': usd, 'sellable_usd': sellable_usd, 'dust': is_dust, 'source': src, 'protected': prot, 'baseline': base, 'sellable': sellable})

        add_rows('TRADING', trading)
        # Funding добавляем только если отличается (или если в trading нет)
        for ccy in funding.keys():
            if ccy not in trading:
                pass
        add_rows('FUNDING', funding)

        # агрегат (суммарный вид) тоже полезен для USDT — добавим отдельную «TOTAL» строку
        try:
            all_ccy = set(list(trading.keys()) + list(funding.keys()))
            for ccy in all_ccy:
                t = trading.get(ccy) or {}
                f = funding.get(ccy) or {}
                total = float(t.get('total') or 0.0) + float(f.get('total') or 0.0)
                avail = float(t.get('avail') or 0.0) + float(f.get('avail') or 0.0)
                usd = float(t.get('usd') or 0.0) + float(f.get('usd') or 0.0)
                prot = (ccy in self._protect_ccy)
                base = float(self._baseline_ccy.get(ccy) or 0.0) if prot else 0.0
                sellable = max(0.0, avail - base) if prot else avail
                rows.append({'ccy': ccy, 'total': total, 'avail': avail, 'usd': usd, 'source': 'TOTAL', 'protected': prot, 'baseline': base, 'sellable': sellable})
        except Exception:
            pass

        return rows

    def _trading_avail(self, ccy: str) -> float:
        ccy = str(ccy or '').upper().strip()
        if not ccy:
            return 0.0
        with self._lock:
            v = (self._balances_cache.get('trading') or {}).get(ccy) or {}
        try:
            return float(v.get('avail') or 0.0)
        except Exception:
            return 0.0

    def _sell_allowed_qty(self, ccy: str) -> float:
        avail = self._trading_avail(ccy)
        if ccy in self._protect_ccy:
            base = float(self._baseline_ccy.get(ccy) or 0.0)
            return max(0.0, avail - base)
        return max(0.0, avail)

    def sell_currency_from_balance(self, ccy: str, quote: str = 'USDT') -> dict:
        """Продать доступный объём валюты, ориентируясь на OKX balance."""
        try:
            if self.trader is None or self.private is None:
                return {'ok': False, 'error': 'Private API не подключён'}
            ccy = str(ccy or '').upper().strip()
            quote = str(quote or 'USDT').upper().strip()
            if not ccy or ccy == quote:
                return {'ok': False, 'error': 'Некорректная валюта'}
            self.request_balances_refresh()
            qty = self._sell_allowed_qty(ccy)
            if qty <= 0:
                return {'ok': False, 'error': f'Нет доступного объёма к продаже (ccy={ccy})'}
            inst_id = f"{ccy}-{quote}"
            # минимальный safety: округление вниз, чтобы не упереться в fee/dust
            qty_str = f"{max(0.0, qty - 1e-12):.12f}".rstrip('0').rstrip('.')
            # dry_run обязателен (keyword-only), иначе кнопка продажи из вкладки «Активы» падает.
            dry = bool((self.config.get('trading', {}) or {}).get('dry_run', False))
            res = self.trader.place_order(dry_run=dry, inst_id=inst_id, side='sell', sz=qty_str, ord_type='market', tgt_ccy=None)
            return res
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    def sell_all_assets_from_balance(self, quote: str = 'USDT') -> dict:
        """Продать все доступные валюты (кроме USDT и защищённых)."""
        try:
            self.request_balances_refresh()
            snap = self.get_balances_snapshot()
            # берём TOTAL строки
            totals = [r for r in snap if str(r.get('source') or '') == 'TOTAL']
            sent = 0
            errs = []
            for r in totals:
                ccy = str(r.get('ccy') or '').upper()
                if not ccy or ccy in ('USDT', 'USD', quote):
                    continue
                if bool(r.get('protected')):
                    continue
                sellable = float(r.get('sellable') or 0.0)
                if sellable <= 0:
                    continue
                res = self.sell_currency_from_balance(ccy=ccy, quote=quote)
                if res.get('ok'):
                    sent += 1
                else:
                    errs.append(f"{ccy}: {res.get('error')}")
            return {'ok': True, 'sent': sent, 'errors': errs}
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    
    def _preflight_symbol_tradeable(self, inst_id: str, *, td_mode: str = "cash") -> tuple[bool, str]:
        """Быстрая проверка символа перед стартом.

        Цель: не пускать в мониторинг пары, которые явно не торгуются (instrument doesn't exist / compliance).
        Возвращает (ok, reason). Не считаем временные/сетевые ошибки фатальными.
        """
        inst = str(inst_id or '').strip().upper()
        if not inst:
            return False, "empty"
        ok, rsn = self._okx_is_tradeable(inst)
        if not ok:
            return False, f"preflight_untradeable {rsn}".strip()

        if self.private is None:
            return True, ""
        try:
            j = self.private.max_size(inst_id=inst, td_mode=td_mode)
        except Exception as e:
            return True, f"preflight_exc:{e}"
        try:
            code0 = str((j or {}).get("code", "") or "")
            msg0 = str((j or {}).get("msg", "") or "")
            s_code = ""
            s_msg = ""
            data = (j or {}).get("data") or []
            if isinstance(data, list) and data:
                r0 = data[0] or {}
                s_code = str(r0.get("sCode", "") or "")
                s_msg = str(r0.get("sMsg", "") or "")
            if code0 == "0" and (not s_code or s_code == "0"):
                return True, ""
            ban_codes = {"51001", "51155", "51087"}
            if code0 in ban_codes or s_code in ban_codes:
                return False, f"preflight_ban code={code0} sCode={s_code} {s_msg or msg0}".strip()
            low = f"{msg0} {s_msg}".lower()
            if ("compliance" in low) or ("not exist" in low) or ("instrument" in low and "exist" in low) or ("listing canceled" in low):
                return False, f"preflight_ban code={code0} sCode={s_code} {s_msg or msg0}".strip()
            # другие ошибки считаем нефактическими (не блокируем старт)
            return True, f"preflight_warn code={code0} sCode={s_code} {s_msg or msg0}".strip()
        except Exception:
            return True, ""

    def _validate_start_symbols(self, symbols: List[str], *, auto_top: bool) -> List[str]:
        """Валидируем список котировок ПЕРЕД стартом мониторинга.

        Требование пользователя:
        - нерабочие пары не должны попадать в мониторинг вообще (особенно при auto-top).
        - используем комбинацию: live instruments OKX + сохранённые untradeable + быстрый preflight (max-size).
        """
        out: List[str] = []
        seen: Set[str] = set()
        # normalize + uniq preserve order
        for s in (symbols or []):
            inst = str(s or '').strip().upper()
            if not inst or inst in seen:
                continue
            seen.add(inst)
            out.append(inst)

        # blacklist
        try:
            scfg = self.config.get("symbols", {}) or {}
            bl = scfg.get("symbol_blacklist", []) or []
            blset = set([str(x).strip().upper() for x in bl if str(x).strip()])
            out = [x for x in out if x not in blset]
        except Exception:
            pass

        # disabled/untradeable from previous runs
        try:
            ds = self.shared_state.get("disabled_symbols")
            if isinstance(ds, set) and ds:
                out = [x for x in out if x not in ds]
        except Exception:
            pass
        try:
            bad = set([str(k).strip().upper() for k, v in (self._okx_tradeable_cache or {}).items() if isinstance(v, dict) and v.get("ok") is False])
            if bad:
                out = [x for x in out if x not in bad]
        except Exception:
            pass

        # instruments existence check (fast, one request)
        if auto_top:
            try:
                avail = self._okx_available_spot_usdt_instids() or set()
                if avail:
                    out = [x for x in out if x in avail]
            except Exception:
                pass

        # Если авто-топ в режиме core (стабильный whitelist) — не блокируем старт preflight-ом:
        try:
            scfg = self.config.get("symbols", {}) or {}
            mode = str(scfg.get("auto_top_mode", "blend") or "blend").strip().lower()
            if auto_top and mode == "core":
                return out
        except Exception:
            pass

        # preflight tradeable (parallel, bounded). Не добавляем символ пока не проверен.
        # Чтобы не было лага на 20+ символов, делаем мягкий лимит по времени.
        try:
            t0 = time.time()
            max_total_sec = (max(4.0, 0.35*len(out))) if auto_top else 2.5
            max_workers = 12
            # td_mode
            td_mode = "cash"
            try:
                okx_cfg = self.config.get("okx", {}) or {}
                if bool(okx_cfg.get("simulated_trading", False)):
                    td_mode = "cash"
            except Exception:
                td_mode = "cash"

            results: Dict[str, tuple[bool, str]] = {}
            q = list(out)

            # worker function
            def _w(sym: str):
                ok, reason = self._preflight_symbol_tradeable(sym, td_mode=td_mode)
                results[sym] = (ok, reason)

            threads: List[threading.Thread] = []
            for sym in q:
                # time budget
                if time.time() - t0 > max_total_sec:
                    break
                th = threading.Thread(target=_w, args=(sym,), daemon=True)
                threads.append(th)
                th.start()
                # ограничение параллелизма
                while True:
                    alive = sum(1 for t in threads if t.is_alive())
                    if alive < max_workers:
                        break
                    if time.time() - t0 > max_total_sec:
                        break
                    time.sleep(0.01)

            # join within budget
            for th in threads:
                remain = max(0.0, max_total_sec - (time.time() - t0))
                if remain <= 0:
                    break
                th.join(timeout=remain)

            # filter: only those checked ok or unchecked? Requirement: unchecked should not be included.
            validated: List[str] = []
            for sym in out:
                if sym in results:
                    ok, reason = results.get(sym) or (True, "")
                    if ok:
                        validated.append(sym)
                    else:
                        self._mark_symbol_untradeable(sym, reason=reason, source="preflight")
                        try:
                            ds = self.shared_state.get("disabled_symbols")
                            if isinstance(ds, set):
                                ds.add(sym)
                        except Exception:
                            pass
                else:
                    # Не успели проверить в preflight — ВКЛЮЧАЕМ в мониторинг,
                    # но помечаем как "tradeable pending" (автоторговля по нему начнётся
                    # только после первого успешного ордера или отдельной проверки).
                    try:
                        self._okx_tradeable_cache[sym] = {"ok": None, "reason": "preflight pending", "ts": time.time()}
                    except Exception:
                        pass
                    try:
                        self.ui_queue.put({"type": "warn", "symbol": sym, "warn": "symbol preflight pending; monitoring enabled"})
                    except Exception:
                        pass
                    validated.append(sym)
            out = validated
        except Exception:
            pass

        return out

    def start(self, symbols: List[str]):
        # Безопасный перезапуск: никогда не очищаем старый stop_event,
        # иначе старые потоки могут "ожить" и засорить UI/крашить приложение.
        self.stop()

        # новый run: новый stop_event, чистые каналы/потоки
        self.stop_event = threading.Event()
        self._is_running = True
        with self._lock:
            self.channels = {}
        self._top_refresh_thread = None
        self._balance_thread = None
        self._order_tracker_thread = None

        try:

            try:
                # на каждый START перезаписываем heartbeat сразу
                os.makedirs(self.data_dir, exist_ok=True)
                with open(self._heartbeat_path, 'w', encoding='utf-8') as f:
                    f.write(str(time.time()))
            except Exception:
                pass
            try:
                def _hb_loop():
                    while True:
                        try:
                            if (not self._is_running) or self.stop_event.is_set():
                                break
                            with open(self._heartbeat_path, 'w', encoding='utf-8') as f:
                                f.write(str(time.time()))
                        except Exception:
                            pass
                        time.sleep(5.0)

                self._heartbeat_thread = threading.Thread(target=_hb_loop, name='heartbeat', daemon=True)
                self._heartbeat_thread.start()
            except Exception:
                self._heartbeat_thread = None

            # обновляем порог "пыли" на каждый START (на случай, если пользователь
            # изменил настройку и нажал «Сохранить» без перезапуска приложения).
            try:
                tcfg = self.config.get('trading', {}) or {}
                dust_usd = float(tcfg.get('dust_usd_threshold', 1.0))
            except Exception:
                dust_usd = 1.0
            os.environ['ATE_DUST_USD'] = str(dust_usd)
            self.shared_state['dust_usd_threshold'] = float(dust_usd)

            # политика загрузки/очистки локальной истории сделок
            self._apply_ledger_start_policy()

            # фиксируем момент START (для ограничения импортируемых fills)
            self._run_started_at = time.time()

            # UI показывает прибыль сессии, а AutoTrader использует это же значение для дневного лимита убытка.
            try:
                base = 0.0
                for tr in self.portfolio.trade_rows():
                    try:
                        if float(getattr(tr, 'sell_ts', 0.0) or 0.0) > 0.0:
                            p, _ = tr.realized_pnl()
                            base += float(p or 0.0)
                    except Exception:
                        continue
                self.shared_state['session_realized_baseline'] = float(base)
                self.shared_state['session_realized_pnl'] = 0.0
            except Exception:
                self.shared_state['session_realized_baseline'] = 0.0
                self.shared_state['session_realized_pnl'] = 0.0

            # сброс таймеров PRV при новом START
            try:
                self._prv_first_start_ts = time.time()
                self._prv_last_ok_ts = 0.0
            except Exception:
                pass

            # автоторговля: параметры (плавный старт + пороги)
            try:
                tcfg = self.config.get('trading', {}) or {}

                # Требование пользователя:
                #  - если в «Ключи OKX» включена галка «Демо-торговля OKX» (okx.simulated_trading=True),
                #    то стартовый демо-набор BTC/ETH/OKB должен быть НЕпродаваемым.
                #    (BTC=1, ETH=1, OKB=100 по умолчанию)
                #  - если галка выключена — это боевой ключ, защита отключена полностью.
                sim = False
                try:
                    okx_cfg = self.config.get('okx', {}) or {}
                    sim = bool(okx_cfg.get('simulated_trading', False))
                except Exception:
                    sim = False

                if sim:
                    # В демо всегда включаем защиту базовых активов.
                    self._protect_ccy = set(['BTC', 'ETH', 'OKB'])
                    try:
                        tcfg['sell_protect_ccy'] = sorted(list(self._protect_ccy))
                    except Exception:
                        pass
                else:
                    # В бою защита отключена (полный доступ к торговле).
                    self._protect_ccy = set()
                    try:
                        tcfg['sell_protect_ccy'] = []
                    except Exception:
                        pass
                # разрешаем автоторговлю даже в Dry‑run, если включён paper_trade.
                dry_run = bool(tcfg.get('dry_run', False))
                paper_trade = bool(tcfg.get('paper_trade', False))
                self.shared_state['auto_trade'] = bool(tcfg.get('auto_trade', False)) and (not dry_run or paper_trade)
                # сбрасываем режим плавного стопа при новом START
                self.shared_state['smooth_stop'] = False
                self.shared_state['smooth_stop_deadline_ts'] = 0.0
                self.shared_state['smooth_stop_max_time'] = False
                warmup_sec = int(tcfg.get('warmup_sec', 60))
                self.shared_state['warmup_until'] = time.time() + max(0, warmup_sec)

                try:
                    per_thr = tcfg.get('per_symbol_thresholds', {}) or {}
                    if isinstance(per_thr, dict):
                        self.shared_state['per_symbol_thresholds'] = per_thr
                    else:
                        self.shared_state['per_symbol_thresholds'] = {}
                except Exception:
                    self.shared_state['per_symbol_thresholds'] = {}
                try:
                    self.shared_state['ban_after_failures'] = int(tcfg.get('ban_after_failures', 3))
                    self.shared_state['ban_ttl_min'] = int(tcfg.get('ban_ttl_min', 60))
                    self.shared_state['winrate_min'] = float(tcfg.get('winrate_min', 0.40))
                    self.shared_state['winrate_min_trades'] = int(tcfg.get('winrate_min_trades', 10))
                    self.shared_state['winrate_ban_hours'] = int(tcfg.get('winrate_ban_hours', 24))
                except Exception:
                    self.shared_state['ban_after_failures'] = 3
                    self.shared_state['ban_ttl_min'] = 60
                    self.shared_state['winrate_min'] = 0.40
                    self.shared_state['winrate_min_trades'] = 10
                    self.shared_state['winrate_ban_hours'] = 24
                # список запрещённых символов для автоторговли (например stable-stable)
                try:
                    scfg = self.config.get('symbols', {}) or {}
                    bl = scfg.get('symbol_blacklist', []) or []
                    self.shared_state['symbol_blacklist'] = [str(x) for x in bl]
                except Exception:
                    self.shared_state['symbol_blacklist'] = []
            except Exception:
                # безопасные дефолты
                self.shared_state['auto_trade'] = False
                self.shared_state['warmup_until'] = time.time() + 60
            
            # запуск автотрейдера (если включён)
            try:
                if self.shared_state.get('auto_trade', False):
                    self.auto_trader = AutoTrader(data_dir=self.data_dir, controller=self, signal_q=self.signal_queue, shared_state=self.shared_state)
                    self.auto_trader.start()
            except Exception:
                self.auto_trader = None


            # Если ключей нет (или включён тестовый режим) — инициализируем бумажный капитал,
            # чтобы стратегия не упиралась в total_equity=0 и могла считать уверенность.
            try:
                tcfg = self.config.get('trading', {}) or {}
                dry = bool(tcfg.get('dry_run', False))
                paper_eq = float(tcfg.get('paper_equity_usd', 1000.0))
                if (self.private is None or dry) and (self.portfolio.equity_usd <= 0):
                    self.portfolio.update_from_okx_balance(total_equity=paper_eq, cash_usdt=paper_eq)
            except Exception:
                pass

            scfg = self.config.get("symbols", {}) or {}
            auto_top = bool(scfg.get("auto_top"))

            try:
                self.shared_state["lag_swap_sec"] = float(scfg.get("lag_swap_sec", 5.0) or 5.0)
            except Exception:
                self.shared_state["lag_swap_sec"] = 5.0
            try:
                self.shared_state["lag_swap_hits"] = int(scfg.get("lag_swap_hits", 3) or 3)
            except Exception:
                self.shared_state["lag_swap_hits"] = 3
            try:
                self.shared_state["lag_swap_window_sec"] = float(scfg.get("lag_swap_window_sec", 30.0) or 30.0)
            except Exception:
                self.shared_state["lag_swap_window_sec"] = 30.0

            # Кол-во котировок берём из auto_top_count даже если Auto-TOP выключен.
            try:
                cnt_fixed = int(scfg.get("auto_top_count", 20))
            except Exception:
                cnt_fixed = 20
            cnt_fixed = max(0, cnt_fixed)

            # Важно: не блокируем START сетевыми проверками.
            # На старте берём первые N из whitelist, а при Auto-TOP=ON фоновой нитью
            # обновим список с учётом "живости".
            try:
                try:
                    symbols = list(load_symbol_universe(self.data_dir, fallback=list(OKX_EMBEDDED_SYMBOLS_V2365)))[:cnt_fixed]
                except Exception:
                    symbols = list(OKX_EMBEDDED_SYMBOLS_V2365)[:cnt_fixed]
                # FIX1B: если часть символов уже заблокирована, сразу дозаполним,
                # чтобы мониторинг стартовал с нужным количеством.
                symbols = self._fill_symbols_to_count(list(symbols), count=cnt_fixed, auto_top=False)
                # UI подсветка: показываем, какие символы реально будут мониториться
                self.ui_queue.put({"type": "top_symbols", "symbols": list(symbols), "cached": False, "fixed": True, "auto_top": bool(auto_top)})
            except Exception:
                pass

            # auto-top не должен блокировать START. Раньше _top_symbols_now()
            # мог занимать заметное время (сеть/таймауты) и пользователь видел «лаг».
            # Теперь:
            #  - на START мгновенно подхватываем кэш (если есть)
            #  - запуск мониторинга/WS идёт сразу
            #  - реальный TOP подтягиваем в фоне потоком _start_top_refresh()
            if auto_top:
                try:
                    scfg = self.config.get("symbols", {}) or {}
                    cnt = int(scfg.get("auto_top_count", 20))
                    cached = self._load_top_cache(max_age_sec=24 * 3600.0)
                    if cached:
                        # кэш допускается только в рамках зашитого whitelist
                        try:
                            cached_f = [str(s).strip().upper() for s in list(cached) if str(s).strip()]
                            wl = set(load_symbol_universe(self.data_dir, fallback=list(OKX_EMBEDDED_SYMBOLS_V2365)) or [])
                            cached_f = [s for s in cached_f if s in wl]
                        except Exception:
                            cached_f = list(cached)
                        symbols = list(cached_f)[:max(0, cnt)]
                        symbols = self._fill_symbols_to_count(list(symbols), count=max(0, cnt), auto_top=True)
                        self.ui_queue.put({"type": "top_symbols", "symbols": symbols, "cached": True, "fixed": True})
                except Exception as e:
                    try:
                        self.ui_queue.put({"type": "warn", "symbol": "ENGINE", "warn": f"top cache: {e}"})
                    except Exception:
                        pass

        
            # финальная валидация списка котировок перед запуском WS/каналов
            try:
                # FIX1B: дозаполняем до нужного кол-ва до/после валидации,
                # чтобы не было "дыр" при заблокированных символах.
                desired_count = cnt_fixed
                if auto_top:
                    try:
                        desired_count = int(scfg.get("auto_top_count", cnt_fixed))
                    except Exception:
                        desired_count = cnt_fixed
                desired_count = max(0, int(desired_count))
                symbols = self._fill_symbols_to_count(list(symbols), count=desired_count, auto_top=auto_top)
                symbols = self._validate_start_symbols(list(symbols), auto_top=auto_top)
                symbols = self._fill_symbols_to_count(list(symbols), count=desired_count, auto_top=auto_top)
            except Exception:
                pass
    # Start WS (prices) and subscribe to symbols
            try:
                self.public_ws.set_symbols(set(symbols))
                self.public_ws.start()
                self._start_ws_price_pump(symbols)
                self._start_okx_health_supervisor()
            except Exception:
                pass

            self._ensure_channels(symbols)

            # приватные данные (баланс)
            self._start_balance_poll()

            # разово подтянуть балансы и зафиксировать базу для защиты
            try:
                self.request_balances_refresh()
                self._ensure_baseline_for_protected()
            except Exception:
                pass

            # трекер pending ордеров (fills → обновление портфеля/истории)
            self._start_order_tracker()

            # optional auto top refresh
            if auto_top:
                self._start_top_refresh()
            # FIX1B: супервизор поддержания количества/замены заблокированных символов
            # работает и при Auto-TOP=OFF (если включена "Автозамена мёртвых символов").
            try:
                if bool(scfg.get("auto_dead_swap", False)):
                    self._start_dead_symbol_supervisor()
            except Exception:
                pass

            log_event(self.data_dir, {"level":"INFO","msg":"engine_start", "extra":{"symbols":symbols, "auto_top":auto_top}})


        except Exception as e:
            # Гарантируем, что START не 'ломает' UI: ошибка отдаётся в уведомления.
            try:
                self._is_running = False
            except Exception:
                pass
            try:
                self.ui_queue.put({"type":"error","error":f"ENGINE START FAILED: {e}","symbol":"ENGINE"})
            except Exception:
                pass
            try:
                log_event(self.data_dir, {"level":"ERROR","msg":"engine_start_failed","extra":{"error":str(e)}})
            except Exception:
                pass
            try:
                self.stop()
            except Exception:
                pass

    def _start_okx_health_supervisor(self) -> None:
        """Фоновая проверка доступности OKX Public REST.

        Важно:
        - НЕ останавливаемся только из-за падения public WS (он может быть заблокирован, а REST работать).
        - Останавливаемся ТОЛЬКО если REST реально недоступен несколько раз подряд.

        Это убирает ситуацию "сайт открывается, но API/WS режется" и пользователь видит пустые цены без причины.
        """
        try:
            if self._okx_health_thread is not None and self._okx_health_thread.is_alive():
                return
        except Exception:
            pass

        # сброс состояния на каждый START
        try:
            self._okx_health_fail_count = 0
            self._okx_health_last_ok_ts = 0.0
        except Exception:
            pass

        def run():
            # первые секунды после START: дадим подняться WS и каналам
            start_ts = time.time()
            while not self.stop_event.is_set():
                try:
                    if (not self._is_running):
                        break
                except Exception:
                    pass

                now = time.time()
                if now - start_ts < 3.0:
                    time.sleep(1.0)
                    continue

                ok = False
                err = ""
                try:
                    _ = self.public.ticker("BTC-USDT")
                    ok = True
                except Exception as e:
                    ok = False
                    err = str(e)[:160]

                if ok:
                    self._okx_health_fail_count = 0
                    self._okx_health_last_ok_ts = now
                    try:
                        self.shared_state["okx_api_last_ok_ts"] = float(now)
                    except Exception:
                        pass
                else:
                    self._okx_health_fail_count = int(self._okx_health_fail_count or 0) + 1

                if self._okx_health_fail_count >= 3:
                    # доп. эвристика: если WS жив и цены реально идут, то не стопаем (редкий ложный фейл).
                    ws = {}
                    try:
                        ws = self.public_ws.status() or {}
                    except Exception:
                        ws = {}
                    try:
                        age = float(ws.get("last_msg_age_sec") or 9999.0)
                    except Exception:
                        age = 9999.0
                    ws_has_data = age <= 3.0

                    if not ws_has_data:
                        msg = (
                            "OKX НЕДОСТУПНО: нет ответа Public API (REST). Движок остановлен для безопасности.\n"
                            "Причина: таймаут/ошибка сети или блокировка /api/v5 корпоративной сетью.\n"
                            f"Детали: {err}"
                        )
                        try:
                            self.ui_queue.put({"type": "error", "symbol": "ENGINE", "error": msg})
                        except Exception:
                            pass
                        try:
                            self.stop()
                        except Exception:
                            pass
                        return

                time.sleep(5.0)

        self._okx_health_thread = threading.Thread(target=run, name="okx_health", daemon=True)
        self._okx_health_thread.start()

    def _start_top_refresh(self):
        if self._top_refresh_thread and self._top_refresh_thread.is_alive():
            return

        def run():
            scfg = self.config.get("symbols", {}) or {}
            refresh_min = float(scfg.get("auto_top_refresh_min", 60))
            while not self.stop_event.is_set():
                # при плавном стопе прекращаем Auto-TOP обновление,
                # чтобы список мониторинга не менялся во время распродажи.
                try:
                    if bool(self.shared_state.get("smooth_stop", False)):
                        break
                except Exception:
                    pass
                try:
                    top = self._top_symbols_now()
                    try:
                        if top:
                            self._save_top_cache(list(top))
                    except Exception:
                        pass
                    self.ui_queue.put({"type": "top_symbols", "symbols": top})

                    try:
                        self.public_ws.set_symbols(set(top))
                    except Exception:
                        pass
                    self._ensure_channels(top)
                except Exception as e:
                    self.ui_queue.put({"type":"warn","symbol":"ENGINE","warn":f"top refresh: {e}"})

                # sleep with STOP sensitivity
                for _ in range(int(refresh_min*60)):
                    if self.stop_event.is_set():
                        break
                    try:
                        if bool(self.shared_state.get("smooth_stop", False)):
                            break
                    except Exception:
                        pass
                    time.sleep(1)

        self._top_refresh_thread = threading.Thread(target=run, daemon=True)
        self._top_refresh_thread.start()




    def _start_symbols_change_ramp(self, *, reason: str) -> None:
        """разгон после изменения списка символов.

        Блокируем только новые BUY на короткое время, чтобы:
        - UI не «зависал» на сохранении,
        - новые каналы успели прогреть метрики,
        - не было серии импульсных покупок.

        SELL/сопровождение открытых позиций не блокируется.
        """
        try:
            tcfg = (self.config.get("trading", {}) or {})
            sec = int(tcfg.get("symbols_change_warmup_sec", 60) or 60)
        except Exception:
            sec = 60
        try:
            until = time.time() + max(0, sec)
            prev = float(self.shared_state.get("symbols_change_pause_until", 0.0) or 0.0)
            # продлеваем, но не укорачиваем
            self.shared_state["symbols_change_pause_until"] = max(prev, until)
            self.ui_queue.put({"type": "warn", "symbol": "ENGINE", "warn": f"Разгон после изменения списка: BUY-пауза {sec} сек ({reason})"})
        except Exception:
            pass

    def refresh_top_now_runtime(self, *, user_trigger: bool = False) -> list:
        """Принудительно обновить Auto-TOP прямо сейчас (без ожидания следующего refresh_min).

        Возвращает актуальный список TOP-символов.
        """
        try:
            top = self._top_symbols_now() or []
        except Exception as e:
            try:
                self.ui_queue.put({"type":"warn","symbol":"ENGINE","warn":f"refresh_top_now: {e}"})
            except Exception:
                pass
            return []

        try:
            self.ui_queue.put({"type":"top_symbols", "symbols": list(top)})
        except Exception:
            pass
        try:
            self.public_ws.set_symbols(set(top))
        except Exception:
            pass
        try:
            if user_trigger:
                self._start_symbols_change_ramp(reason="user auto-top refresh")
            self._ensure_channels(list(top))
        except Exception as e:
            try:
                self.ui_queue.put({"type":"warn","symbol":"ENGINE","warn":f"ensure_channels(top): {e}"})
            except Exception:
                pass
        return list(top)

    def reconcile_symbols_runtime(self, symbols: list) -> None:
        """Синхронизировать каналы под заданный список символов (добавить и отключить лишние)."""
        try:
            self._start_symbols_change_ramp(reason="user symbols reconcile")
            self._ensure_channels(list(symbols or []))
        except Exception as e:
            try:
                self.ui_queue.put({"type":"warn","symbol":"ENGINE","warn":f"reconcile_symbols_runtime: {e}"})
            except Exception:
                pass

    def _start_dead_symbol_supervisor(self) -> None:
        """Фоновый супервизор: заменяет "мёртвые" символы без STOP/START.

        Работает только когда включён Auto-TOP.
        """
        try:
            if getattr(self, "_dead_sup_thread", None) is not None and self._dead_sup_thread.is_alive():
                return
        except Exception:
            pass

        def run():
            last_swap_ts = 0.0
            while not self.stop_event.is_set():
                try:
                    scfg = (self.config.get("symbols", {}) or {})
                    auto_top = bool(scfg.get("auto_top", False))
                    # FIX1B: автозамена/поддержание количества работает и при Auto-TOP=OFF
                    # (например, если символы попали в TTL-ban/disabled).
                    if not bool(scfg.get("auto_dead_swap", False)):
                        time.sleep(2.0); continue

                    # пороги
                    try:
                        no_tick_sec = float(scfg.get("dead_no_tick_sec", 120) or 120)
                    except Exception:
                        no_tick_sec = 120.0
                    try:
                        zero_conf_sec = 0.0
                    except Exception:
                        zero_conf_sec = 0.0
                    try:
                        cooldown_sec = float(scfg.get("dead_swap_cooldown_sec", 45) or 45)
                    except Exception:
                        cooldown_sec = 45.0
                    try:
                        ban_min = float(scfg.get("dead_ban_min", 30) or 30)
                    except Exception:
                        ban_min = 30.0

                    now = time.time()
                    if now - last_swap_ts < max(1.0, cooldown_sec):
                        time.sleep(1.0); continue

                    # целевое количество
                    try:
                        desired_count = int(scfg.get("auto_top_count", 20))
                    except Exception:
                        desired_count = 20
                    desired_count = max(0, desired_count)

                    # копируем список каналов
                    with self._lock:
                        items = list(self.channels.items())

                    # FIX1B: если каналов меньше, чем нужно (например, из-за банов) —
                    # быстро дозаполним без ожидания "dead" детекта.
                    try:
                        if desired_count > 0 and len(items) < desired_count:
                            target = self._top_symbols_now() if auto_top else self._fixed_symbols_now(count=desired_count)
                            target = self._fill_symbols_to_count(list(target), count=desired_count, auto_top=auto_top)
                            try:
                                self.ui_queue.put({"type":"top_symbols", "symbols": list(target), "cached": False, "fixed": True, "auto_top": bool(auto_top)})
                            except Exception:
                                pass
                            try:
                                self.public_ws.set_symbols(set(target))
                            except Exception:
                                pass
                            self._ensure_channels(list(target))
                            last_swap_ts = now
                            time.sleep(2.0)
                            continue
                    except Exception:
                        pass

                    dead_sym = None
                    reason = ""
                    for sym, ch in items:
                        # FIX1B: если символ заблокирован (TTL-ban) — заменяем его,
                        # чтобы поддерживать заданное количество.
                        try:
                            banned, until_ts, breason = self.banlist.is_banned(sym)
                            if banned:
                                dead_sym = sym
                                reason = f"заблокирован (ban до {time.strftime('%H:%M:%S', time.localtime(until_ts))}): {breason}"
                                break
                        except Exception:
                            pass
                        # runtime/disabled
                        try:
                            ds = self.shared_state.get("disabled_symbols")
                            if isinstance(ds, set) and sym in ds:
                                dead_sym = sym
                                reason = "отключён (disabled_symbols)"
                                break
                        except Exception:
                            pass
                        try:
                            rs = self.shared_state.get("runtime_stop_symbols")
                            if isinstance(rs, set) and sym in rs:
                                dead_sym = sym
                                reason = "остановлен (runtime_stop_symbols)"
                                break
                        except Exception:
                            pass
                        try:
                            lt = float(getattr(ch, "last_tick_ts", 0.0) or 0.0)
                            lz = float(getattr(ch, "last_nonzero_conf_ts", 0.0) or 0.0)
                            st = float(getattr(ch, "started_ts", 0.0) or 0.0)
                            # если цена так и не появилась после старта канала
                            if lt <= 0 and st > 0 and (now - st) > no_tick_sec:
                                dead_sym = sym
                                reason = f"нет цены с старта > {int(no_tick_sec)}с"
                                break

                            # если lt=0 — канал ещё не прогрелся
                            if lt > 0 and (now - lt) > no_tick_sec:
                                dead_sym = sym
                                reason = f"нет цены > {int(no_tick_sec)}с"
                                break

                            try:
                                lag_hits = int(getattr(ch, "lag_over_window_hits", 0) or 0)
                                lag_need = int(getattr(ch, "lag_over_window_target", 3) or 3)
                                if lag_need > 0 and lag_hits >= lag_need:
                                    dead_sym = sym
                                    # lag threshold is configured in shared state (default 5s)
                                    try:
                                        lag_thr = float(self.shared_state.get("lag_swap_sec", 5.0) or 5.0)
                                    except Exception:
                                        lag_thr = 5.0
                                    dead_sym = sym
                                    reason = f"лаг данных > {int(lag_thr)}с ({lag_hits}/{lag_need})"
                                    break
                            except Exception:
                                pass
                            # v3: старая защита по '0 уверенности' удалена (у Score=0 это нормальный HOLD).
                            # Оставлено пустым намеренно.
                                break
                        except Exception:
                            continue

                    if not dead_sym:
                        time.sleep(2.0); continue

                    # безопасность: не меняем если есть позиция/pending
                    try:
                        posd = self.portfolio.position_dict(dead_sym) or {}
                        qty = float(posd.get("base_qty") or posd.get("qty") or 0.0)
                    except Exception:
                        qty = 0.0
                    try:
                        pend = bool(self.portfolio.has_pending(dead_sym))
                    except Exception:
                        pend = False
                    if qty > 0.0 or pend:
                        time.sleep(2.0); continue

                    before = set([s for s, _ in items])

                    # баним чтобы не вернулся сразу
                    try:
                        self.banlist.ban(dead_sym, ttl_sec=float(ban_min)*60.0, reason=f"dead swap: {reason}", source="dead_swap")
                    except Exception:
                        pass

                    # мягко останавливаем канал
                    try:
                        rs = self.shared_state.get("runtime_stop_symbols")
                        if isinstance(rs, set):
                            rs.add(dead_sym)
                    except Exception:
                        pass
                    try:
                        with self._lock:
                            ch = self.channels.pop(dead_sym, None)
                        if ch is not None:
                            ch.join(timeout=1.5)
                    except Exception:
                        pass

                    # FIX1B: обновляем целевой список и каналы, поддерживая заданное количество.
                    try:
                        target = self._top_symbols_now() if auto_top else self._fixed_symbols_now(count=desired_count)
                        target = self._fill_symbols_to_count(list(target), count=desired_count, auto_top=auto_top)
                    except Exception:
                        target = []

                    try:
                        self.ui_queue.put({"type":"top_symbols", "symbols": list(target), "cached": False, "fixed": True, "auto_top": bool(auto_top)})
                    except Exception:
                        pass
                    try:
                        self.public_ws.set_symbols(set(target))
                    except Exception:
                        pass
                    try:
                        self._ensure_channels(list(target))
                    except Exception:
                        pass

                    after = set(target or [])
                    added = list(after - before)
                    new_sym = added[0] if added else ""
                    try:
                        msg = f"Автозамена мёртвого символа: {dead_sym} → {new_sym} ({reason})" if new_sym else f"Автозамена: отключил {dead_sym} ({reason})"
                        self.ui_queue.put({"type":"warn","symbol":"ENGINE","warn":msg})
                    except Exception:
                        pass

                    last_swap_ts = now

                except Exception as e:
                    try:
                        self.ui_queue.put({"type":"warn","symbol":"ENGINE","warn":f"dead swap: {e}"})
                    except Exception:
                        pass
                    time.sleep(2.0)

        self._dead_sup_thread = threading.Thread(target=run, daemon=True)
        self._dead_sup_thread.start()


    def request_smooth_stop(self, *, minutes: Optional[int] = 15, max_time: bool = False) -> Dict[str, Any]:
        from engine.controller_lifecycle import request_smooth_stop_impl
        return request_smooth_stop_impl(self, minutes=minutes, max_time=max_time)

    def stop(self):
        # Остановка текущего запуска (потоки должны корректно завершиться)
        try:
            self.stop_event.set()
        except Exception:
            pass
        try:
            self._ws_pump_stop.set()
        except Exception:
            pass
        # runtime flag
        self._is_running = False

        try:
            if self._heartbeat_thread is not None:
                self._heartbeat_thread.join(timeout=1.0)
        except Exception:
            pass
        self._heartbeat_thread = None
        try:
            self.public_ws.stop()
        except Exception:
            pass

        try:
            if self.private_ws is not None:
                self.private_ws.stop()
        except Exception:
            pass

        # stop autotrader
        try:
            if self.auto_trader is not None:
                self.auto_trader.stop()
                self.auto_trader.join(timeout=2.0)
                self.auto_trader = None
        except Exception:
            self.auto_trader = None


        # stop order tracker
        try:
            if self._order_tracker_thread is not None:
                self._order_tracker_thread.join(timeout=2.0)
        except Exception:
            pass

        # drain signal queue
        try:
            while True:
                self.signal_queue.get_nowait()
        except Exception:
            pass

        chans = []
        with self._lock:
            chans = list(self.channels.values())
        for ch in chans:
            try:
                ch.join(timeout=2.0)
            except Exception:
                pass
        with self._lock:
            self.channels = {}

        # финализируем decision-логи и пакуем последний файл в .gz при STOP/плавном STOP.
        # Это НЕ влияет на торговлю и работает только если включены снапшоты/отладка.
        try:
            if self.decision_logger is not None:
                self.decision_logger.finalize_session(reason="stop")
        except Exception:
            pass

        log_event(self.data_dir, {"level":"INFO","msg":"engine_stop"})

    def is_running(self) -> bool:
        """True, если движок реально запущен (а не просто stop_event ещё не выставлен)."""
        try:
            return bool(self._is_running) and (not self.stop_event.is_set())
        except Exception:
            return False


    def status(self) -> Dict[str, Any]:
        return {"channels": list(self.channels.keys()), "running": self.is_running()}