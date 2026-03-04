from __future__ import annotations

import os
import json
import threading
import time
import sys
import traceback

from engine.logging_utils import MSK_TZ

# Когда приложение собрано в EXE (PyInstaller), любые относительные пути
# должны резолвиться относительно папки с EXE, а не относительно профиля пользователя.
def _get_app_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    # режим запуска из исходников: корень проекта (папка, где лежат app/, engine/, data/)
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

APP_DIR = _get_app_dir()
try:
    os.chdir(APP_DIR)
except Exception:
    pass


def _install_crash_hook(app_dir: str) -> None:
    """Ловим непойманные исключения и пишем в ./data/logs/crash_YYYY-MM-DD.log.

    Важно для диагностики внезапных остановок .
    """
    try:
        logs_dir = os.path.join(app_dir, "data", "logs")
        os.makedirs(logs_dir, exist_ok=True)
    except Exception:
        logs_dir = None

    def _hook(exc_type, exc, tb):
        try:
            if logs_dir:
                from datetime import datetime
                ts = datetime.now(MSK_TZ).strftime("%Y-%m-%d")
                path = os.path.join(logs_dir, f"crash_{ts}.log")
                with open(path, "a", encoding="utf-8") as f:
                    f.write("\n" + "="*80 + "\n")
                    f.write(f"CRASH {datetime.now(MSK_TZ).isoformat()}\n")
                    f.write("".join(traceback.format_exception(exc_type, exc, tb)))
                    try:
                        f.flush()
                    except Exception:
                        pass
        except Exception:
            pass
        # Печатаем в stderr тоже (если запущено из консоли)
        try:
            sys.__excepthook__(exc_type, exc, tb)
        except Exception:
            pass

    try:
        sys.excepthook = _hook
    except Exception:
        pass


_install_crash_hook(APP_DIR)

# создаём его с согласованными "боевыми" настройками, чтобы freshly-built EXE
# всегда стартовал одинаково.
DEFAULT_BOOT_CONFIG_JSON = r"""{
  "version": "ATE 6PRO v3.1.4.0",
  "okx": {
    "api_key": "",
    "api_secret": "",
    "passphrase": "",
    "save_keys": false,
    "simulated_trading": false
  },
  "trading": {
    "dry_run": false,
    "quote_ccy": "USDT",
    "max_total_position_pct": 5,
    "max_single_position_pct": 12.0,
    "risk_per_trade_pct": 0.8,
    "signal_cooldown_sec": 90,
    "order_type": "market",
    "default_order_usd": 500.0,
    "order_size_mode": "fixed",
    "order_size_pct": 5.0,
    "min_cash_reserve_pct": 10.0,
    "paper_equity_usd": 5000.0,
    "fee_rate": 0.001,
    "snapshots_enabled": true,
    "auto_trade": true,
    "warmup_sec": 60,
    "v3_buy_score_min": 0.75,
    "v3_sell_score_min": 0.70,
    "min_exit_hold_sec": 30.0,
    "hard_stop_loss_pct": 1.20,
    "cooldown_sec": 10,
    "max_positions": 8,
    "min_order_usd": 10.0,
    "dust_usd_threshold": 1.0,
    "check_old_orders": false,
    "check_old_orders_hours": 5,
    "sell_protect_ccy": [],
    "sell_protect_baseline": {},
    "sell_sweep_dust_enabled": true,
    "sell_sweep_max_usd": 5.0,
    "paper_trade": false,
    "ban_after_failures": 3,
    "ban_ttl_min": 60,
    "winrate_min": 0.4,
    "winrate_min_trades": 10,
    "winrate_ban_hours": 24,
    "max_positions_per_symbol": 1,
    "allow_exceed_max_positions": false,
    "exceed_max_positions_score": 0.95,
    "api_economy_mode": false,
    "auto_top_enabled": true,
    "auto_top_count": 30,
    "auto_top_update_min": 60,
    "dead_swap_enabled": true,
    "dead_swap_no_price_sec": 120,
    "dead_swap_pause_sec": 45,
    "dead_swap_ban_min": 30,
    "dust_threshold_usd": 1.0,
    "economy_candles_every": 60.0,
    "economy_book_every": 30.0,
    "economy_trades_every": 30.0,
    "fetch_candles_every": 15.0,
    "fetch_book_every": 2.0,
    "fetch_trades_every": 2.0,
    "symbols_change_warmup_sec": 60,
    "new_symbol_warmup_sec": 60,
    "buy_cooldown_sec": 90,
    "sell_cooldown_sec": 10,
    "signal_ttl_sec": 3,
    "global_buy_throttle_sec": 4,
    "max_spread_buy_pct": 0.22,
    "max_lag_buy_sec": 1.5
  },
  "symbols": {
    "list": [],
    "auto_top": true,
    "auto_top_refresh_min": 60,
    "auto_top_count": 30,
    "lag_swap_sec": 5,
    "lag_swap_hits": 3,
    "lag_swap_window_sec": 30,
    "symbol_blacklist": [
      "DAI-USDT",
      "EUR-USDT",
      "FDUSD-USDT",
      "GBP-USDT",
      "IP-USDT",
      "OKB-USDT",
      "PENGU-USDT",
      "PYUSD-USDT",
      "TUSD-USDT",
      "USDC-USDT",
      "USDE-USDT",
      "USDG-USDT",
      "USDT-USDC",
      "XPL-USDT",
      "ZRO-USDT"
    ],
    "auto_top_mode": "core",
    "core_symbols": [
      "BTC-USDT",
      "ETH-USDT",
      "SOL-USDT",
      "BNB-USDT",
      "XRP-USDT",
      "DOGE-USDT",
      "ADA-USDT",
      "TRX-USDT",
      "TON-USDT",
      "AVAX-USDT",
      "LINK-USDT",
      "DOT-USDT",
      "MATIC-USDT",
      "ATOM-USDT",
      "LTC-USDT",
      "BCH-USDT",
      "ETC-USDT",
      "XLM-USDT",
      "UNI-USDT",
      "FIL-USDT",
      "APT-USDT",
      "ARB-USDT",
      "OP-USDT",
      "INJ-USDT",
      "NEAR-USDT",
      "ICP-USDT",
      "IMX-USDT",
      "AAVE-USDT",
      "SUI-USDT",
      "SEI-USDT",
      "PEPE-USDT",
      "SHIB-USDT",
      "WIF-USDT",
      "BONK-USDT",
      "JUP-USDT",
      "RUNE-USDT",
      "HBAR-USDT",
      "VET-USDT",
      "ALGO-USDT",
      "EGLD-USDT",
      "RNDR-USDT",
      "TIA-USDT",
      "STX-USDT",
      "KAS-USDT",
      "FET-USDT",
      "GRT-USDT",
      "SAND-USDT",
      "MANA-USDT",
      "FLOW-USDT",
      "THETA-USDT",
      "FTM-USDT",
      "NEO-USDT",
      "KAVA-USDT",
      "ZEC-USDT",
      "XMR-USDT",
      "EOS-USDT",
      "IOTA-USDT",
      "XTZ-USDT",
      "CRV-USDT",
      "DYDX-USDT",
      "GMX-USDT",
      "ENJ-USDT",
      "CHZ-USDT",
      "1INCH-USDT",
      "COMP-USDT",
      "SNX-USDT",
      "BAT-USDT",
      "ZIL-USDT",
      "ONT-USDT",
      "WLD-USDT",
      "PYTH-USDT",
      "JTO-USDT",
      "ENA-USDT",
      "ONDO-USDT",
      "TAO-USDT",
      "ARKM-USDT",
      "FLOKI-USDT",
      "GALA-USDT",
      "LDO-USDT",
      "AR-USDT"
    ],
    "auto_dead_swap": true,
    "dead_no_tick_sec": 120,
    "dead_swap_cooldown_sec": 45,
    "dead_ban_min": 30,
    "auto_top_enabled": true
  },
  "strategy": {
    "name": "StrategyV3",
    "params": {
      "buy_threshold": 0.78,
      "sell_threshold": 0.72,
      "conf_scale": 2.0,
      "take_profit_base_pct": 0.01,
      "stop_loss_base_pct": 0.012,
      "atr_tp_mult": 1.2,
      "atr_sl_mult": 1.3,
      "min_hold_sec": 60,
      "lock_start_pct": 0.006,
      "lock_gap_pct": 0.003,
      "max_atr_pct": 1.1,
      "trend_filter": true,
      "cooldown_sec": 60,
      "min_exit_net_pct": 0.0002,
      "spread_limit_pct": 0.06,
      "spread_soft_pct": 0.035,
      "spread_buy_thr_add": 0.005,
      "profit_protect_enabled": true,
      "profit_tp_hard_pct": 0.01,
      "profit_tp_hard_strong_pct": 0.018,
      "profit_hold_strong_buy_conf": 0.84,
      "profit_be_start_pct": 0.001,
      "profit_be_floor_pct": 0.0002,
      "profit_ladder": [
        {
          "start": 0.01,
          "floor": 0.008
        },
        {
          "start": 0.007,
          "floor": 0.006
        },
        {
          "start": 0.004,
          "floor": 0.003
        },
        {
          "start": 0.002,
          "floor": 0.0012
        }
      ],
      "market_filter_enabled": true,
      "market_ref_symbol": "BTC-USDT",
      "market_block_btc_ret15": -0.8,
      "market_block_btc_slope30": -0.25,
      "market_danger_vol_1h": 3.0,
      "market_block_seconds": 180,
      "market_buy_thr_add": 0.03
      ,
      "staged_entry_enabled": true,
      "staged_entry_prearm_delta": 0.03,
      "staged_entry_confirm_break_pct": 0.0015,
      "staged_entry_max_chase_pct": 0.0035,
      "early_failfast_min_hold_sec": 25,
      "peak_det_enabled": true,
      "peak_det_min_hold_sec": 45,
      "peak_det_min_net": 0.002,
      "peak_det_dd_base": 0.0015,
      "peak_det_dd_spread_mult": 5.0,
      "peak_det_dd_atr_mult": 0.35,
      "peak_det_rev_thr": 0.65,
      "peak_det_fs_exit": 0.45,
      "neg_peak_enabled": true,
      "neg_peak_time_sec": 120,
      "neg_peak_peak_max_net": 0.0,
      "neg_peak_current_net_le": -0.0035,
      "neg_peak_require_edge_bad": true
    }
  },
  "logging": {
    "decision_log_enabled": true,
    "decision_log_mode": "signals",
    "decision_log_tick_sec": 15.0,
    "decision_log_max_bytes": 52428800
  },
  "app": {
    "name": "ATE 6PRO v3",
    "version": "3"
  },
  "app_version": "3"
}"""


# фиксируем рабочую папку на директорию исполняемого файла,
# чтобы ВСЕ файлы (data, логи, кэш) жили рядом с программой, а не в профиле пользователя.
if getattr(sys, 'frozen', False):
    try:
        os.chdir(os.path.dirname(sys.executable))
    except Exception:
        pass

from queue import Queue, Empty

import tkinter as tk
import tkinter.font as tkfont
from tkinter import ttk, messagebox

from engine.config import ConfigManager, obfuscate, deobfuscate
from engine.controller import EngineController
from engine.cache import clear_cache
from engine.logging_utils import log_event
from engine.version import APP_NAME, STRATEGY_VERSION, APP_VERSION, APP_TITLE
from engine.migration import migrate_blocked_symbols

def data_dir(base_path: str) -> str:
    return os.path.join(base_path, "data")

class App(tk.Tk):
    def __init__(self, base_path: str):
        super().__init__()
        # при запуске окно сразу в полноэкранном режиме (Windows zoomed)
        try:
            self.state('zoomed')
        except Exception:
            try:
                self.attributes('-fullscreen', True)
            except Exception:
                pass
        self.base_path = base_path
        self.data_path = data_dir(base_path)
        self.cfg_path = os.path.join(self.data_path, "config.json")
        # Если data\config.json не попал в dist (или удалён), приложение стартует с "внутренними" дефолтами.
        # Чтобы дефолты всегда были теми, что мы согласовали, гарантируем создание config.json рядом с программой.
        try:
            os.makedirs(self.data_path, exist_ok=True)
            if not os.path.exists(self.cfg_path):
                with open(self.cfg_path, "w", encoding="utf-8") as f:
                    json.dump(json.loads(DEFAULT_BOOT_CONFIG_JSON), f, ensure_ascii=False, indent=2)
        except Exception:
            pass

        self.cfg_mgr = ConfigManager(self.cfg_path)
        try:
            self.cfg = self.cfg_mgr.load()
        except Exception:
            # повреждённый JSON не должен валить UI
            self.cfg = {}
        # соль для шифрования ключей не должна зависеть от версии сборки.
        # Храним её отдельно (crypto_salt) и используем ВЕЗДЕ для (де)шифрования.
        self.cfg.setdefault("crypto_salt", "ATE")
        # --- дефолты (не затирают пользовательские значения) ---
        self.cfg.setdefault("okx", {})
        # MultiSymbol по умолчанию (боевой профиль — Auto‑TOP включён).
        self.cfg.setdefault(
            "symbols",
            {
                # Ручной список по умолчанию пустой: наполняется Auto‑TOP.
                "list": [],
                "auto_top": True,
                "auto_top_count": 30,
                "auto_top_refresh_min": 60,
                "symbol_blacklist": ["USDC-USDT", "USDT-USDC"],
                "auto_dead_swap": True,
                "dead_no_tick_sec": 120,
                "dead_swap_cooldown_sec": 45,
                "dead_ban_min": 30,
            },
        )
        tcfg = self.cfg.setdefault("trading", {})
        # Боевые дефолты (всё можно менять в UI, но стартовые значения — как договорились)
        tcfg.setdefault("dry_run", False)  # dry-run удалён
        tcfg.setdefault("auto_trade", True)
        tcfg.setdefault("snapshots_enabled", True)
        tcfg.setdefault("api_economy_mode", False)
        tcfg.setdefault("economy_candles_every", 60.0)
        tcfg.setdefault("economy_book_every", 30.0)
        tcfg.setdefault("economy_trades_every", 30.0)
        tcfg.setdefault("check_old_orders", False)
        tcfg.setdefault("check_old_orders_hours", 5)
        tcfg.setdefault("warmup_sec", 60)
        tcfg.setdefault("v3_buy_score_min", 1.0)
        tcfg.setdefault("v3_sell_score_min", 1.0)
        # Общий cooldown (для UI) — оставляем коротким. Реальные BUY/SELL cooldown регулируются отдельно.
        tcfg.setdefault("cooldown_sec", 10.0)
        tcfg.setdefault("order_size_mode", "fixed")  # fixed|percent
        tcfg.setdefault("default_order_usd", 500.0)
        tcfg.setdefault("order_size_pct", 5.0)
        tcfg.setdefault("min_cash_reserve_pct", 10.0)
        tcfg.setdefault("max_positions", 8)
        tcfg.setdefault("max_positions_per_symbol", 1)
        tcfg.setdefault("allow_exceed_max_positions", False)
        tcfg.setdefault("exceed_max_positions_score", 0.95)
        tcfg.setdefault("min_order_usd", 10.0)
        # порог "пыли" (USD-эквивалент), остатки меньше него:
        # - не блокируют BUY в режиме "1 символ = 1 позиция",
        # - могут оставаться после SELL из-за округлений/минималок OKX.
        tcfg.setdefault("dust_usd_threshold", 1.0)

        # Paper trading отключён: dry‑run вырезан, работаем в реальном режиме при наличии ключей.
        tcfg.setdefault("paper_trade", False)

        # Логи решений (Decision Log) для последующего Replay/моделирования.
        # Важно: всё хранится только внутри папки программы: ./data/decision_logs/
        lcfg = self.cfg.setdefault("logging", {})
        lcfg.setdefault("decision_log_enabled", True)
        # режим: "signals" (только BUY/SELL), "ticks" (все тики), "signals+ticks" (оба)
        lcfg.setdefault("decision_log_mode", "signals+ticks")
        # если включены ticks, пишем не чаще, чем раз в N секунд на символ
        lcfg.setdefault("decision_log_tick_sec", 3.0)

        # Шумовая зона (по умолчанию привязываем к порогам):
        # Шумовая зона синхронизирована с порогами (нижняя/верхняя границы для подсветки в UI).
        tcfg.setdefault("ban_after_failures", 3)
        tcfg.setdefault("ban_ttl_min", 60)
        tcfg.setdefault("winrate_min", 0.40)
        tcfg.setdefault("winrate_min_trades", 10)
        tcfg.setdefault("winrate_ban_hours", 24)
        # защита стартового BTC: по умолчанию защищаем *ровно* 1 BTC.
        # Если на балансе 1.00015 BTC — продаваться может только 0.00015.
        # При необходимости можно включить через UI/конфиг.
        tcfg.setdefault("sell_protect_ccy", [])
        tcfg.setdefault("sell_protect_baseline", {})

        # Единый формат версий: всегда держим одинаково во всех местах.
        self.cfg["version"] = APP_TITLE

        # если пользователь ставит новую версию в новую папку.
        try:
            mig = migrate_blocked_symbols(base_path=self.base_path, data_dir=self.data_path, cfg=self.cfg)
            if mig.get("did"):
                # сохраняем конфиг, если в него добавили symbol_blacklist
                try:
                    self.cfg_mgr.data = self.cfg
                    self.cfg_mgr.save()
                except Exception:
                    pass
                # и пишем событие в лог для прозрачности
                try:
                    log_event(self.data_path, {"level":"INFO","msg":"MIGRATE_BLOCKED_SYMBOLS", "extra": mig})
                except Exception:
                    pass
        except Exception:
            pass

        self.ui_queue: Queue = Queue(maxsize=5000)
        self.engine = EngineController(data_dir=self.data_path, config=self.cfg, ui_queue=self.ui_queue)

        self.title(APP_TITLE)
        self.geometry("1250x780")
        self.minsize(1100, 700)

        # при запуске открываем окно сразу в развернутом (maximized) режиме
        self.after(80, self._apply_startup_zoom)

        self._build_style()
        self._build_ui()
        self._load_cfg_to_ui()

        self._iid_map = {}
        # Кэш для ускорения Treeview (иначе при подсветке/тегах можно получить задержки UI)
        self._row_cache_values = {}  # symbol -> tuple(values)
        self._row_cache_tags = {}    # iid -> tuple(tags)
        self._active_symbols = []
        self._last_bank_update = 0.0

        # UI-only: время старта и PnL с момента запуска
        self._run_started_ts = 0.0
        self._pnl_baseline = 0.0
        self._session_running = False
        self._session_elapsed_hold = 0  # фиксируем, когда нажали STOP/Плавный STOP

        # UI-only: плавный стоп
        self._smooth_stop_active = False

        # UI-only: фильтры вкладки "Активы"
        self._hide_dust_assets = False
        self._assets_last_snapshot = None

        # UI-only: фиксация ширины правой панели мониторинга
        self._sash_lock = False

        self.after(200, self._poll_ui_queue)
        # более частое обновление блока "Торговля" (PnL/статусы) — 3-4 раза в секунду.
        self._trade_panel_refresh_ms = 300
        self.after(self._trade_panel_refresh_ms, self._refresh_trade_panel)
        self.after(500, self._tick_session_timer)
        # авто-обновление вкладки "Активы" 1-2 раза в секунду (без доп. запросов к OKX,
        # т.к. UI читает кэш движка).
        self._assets_refresh_ms = 700
        self.after(self._assets_refresh_ms, self._assets_auto_refresh_loop)

    
    def _reason_ru(self, code: str) -> str:
        """Перевод и компактное отображение причин для мониторинга.

        Правила:
        - НИКОГДА не показываем пользователю *_ERROR (это тех.след от парсинга).
        - Если строка уже на русском — только аккуратно укорачиваем.
        - Английские/технические причины маппим на русский и сжимаем параметры.
        """
        raw = (code or "").strip()
        if not raw:
            return "—"

        # 1) убираем любые следы ERROR (суффиксы/префиксы/внутренние маркеры)
        try:
            raw = raw.replace("_ERROR", "").replace("ERROR_", "")
            raw = raw.replace("[ERROR]", "").replace("(ERROR)", "").strip()
        except Exception:
            pass

        # 2) если уже есть кириллица — просто укорачиваем (без усложнений)
        try:
            if any(('А' <= ch <= 'я') or ch in ('ё','Ё') for ch in raw):
                return (raw[:32] + '…') if len(raw) > 33 else raw
        except Exception:
            pass

        up = raw.upper()

        # BLOCK_* — показываем что именно блокирует
        try:
            if up.startswith("BLOCK_"):
                tail = raw[6:].strip()
                if not tail:
                    return "Блок"
                return ("Блок: " + (tail[:28] + "…" if len(tail) > 29 else tail))
        except Exception:
            pass

        # 3) компактные шаблоны с параметрами
        # ANTI_CHASE: r5=0.18%, r15=0.63%
        m = None
        try:
            m = __import__('re').search(r"ANTI[_-]?CHASE.*?R5=([\-0-9\.]+)%.*?R15=([\-0-9\.]+)%", raw, __import__('re').I)
        except Exception:
            m = None
        if m:
            return f"Анти-погоня r5={m.group(1)} r15={m.group(2)}"

        # SPREAD too high (0.121%) / SPREAD>0.12%
        try:
            m = __import__('re').search(r"SPREA[D]?[^0-9]*([0-9\.]+)%", raw, __import__('re').I)
        except Exception:
            m = None
        if m:
            return f"Спред высокий {m.group(1)}%"[:33] if len(f"Спред высокий {m.group(1)}%")>33 else f"Спред высокий {m.group(1)}%"  # noqa

        # ENTRY_GATE score=7 ...
        try:
            m = __import__('re').search(r"ENTRY[_-]?GATE\s*[:=]?\s*SCORE\s*=\s*([0-9]+)", raw, __import__('re').I)
        except Exception:
            m = None
        if m:
            return f"Фильтр входа score={m.group(1)}"

        # PROFIT_FLOOR:0.20->0.12 / PROFIT_FLOOR_0.20->0.12
        try:
            m = __import__('re').search(r"PROFIT[_-]?FLOOR.*?([0-9\.]+).*?([0-9\.]+)", raw, __import__('re').I)
        except Exception:
            m = None
        if m:
            return f"Профит-стоп {m.group(1)}→{m.group(2)}"

        # ACTIVE_EXIT: edge_lost / ACTIVE_EXIT: ....
        if "ACTIVE_EXIT" in up:
            # берём хвост после двоеточия
            tail = raw.split(":", 1)[1].strip() if ":" in raw else raw
            tail = tail.replace("edge_lost", "край потерян").replace("edge", "край")
            out = f"Активный выход: {tail}"
            return (out[:32] + '…') if len(out) > 33 else out

        # BUY: ...
        if up.startswith("BUY") or "BUY:" in up:
            out = raw.replace("BUY:", "Покупка:").replace("BUY", "Покупка")
            return (out[:32] + '…') if len(out) > 33 else out

        if up.startswith("SELL") or "SELL:" in up:
            out = raw.replace("SELL:", "Продажа:").replace("SELL", "Продажа")
            return (out[:32] + '…') if len(out) > 33 else out

        # 4) простой словарь (короткие причины)
        mapping = {
            "OK": "ОК",
            "NO_SIGNAL": "Нет сигнала",
            "WARMUP": "Прогрев",
            "COOLDOWN": "Пауза",
            "MIN_HOLD_ACTIVE": "Мин.холд",
            "MIN-HOLD": "Мин.холд",
            "MARKET_BLOCK_BUY": "Режим рынка: блок",
            "BLOCK_MAX_POS": "Лимит позиций",
            "MAX_POS": "Лимит позиций",
            "BLOCK_PENDING": "Есть ордер",
            "PENDING": "Есть ордер",
            "BLOCK_COOLDOWN": "Пауза",
            "DUST": "Пыль",
            "ANTI_CHASE": "Анти-погоня",
            "TREND_DOWN": "Тренд вниз",
        }
        for k, v in mapping.items():
            if up == k or up.startswith(k):
                return v

        # 5) фолбэк: компактная "как есть"
        out = raw.strip()
        return (out[:32] + '…') if len(out) > 33 else out

    def _apply_startup_zoom(self):
        try:
            # Windows: 'zoomed' = maximize
            self.state("zoomed")
        except Exception:
            try:
                # fallback: quick fullscreen toggle
                self.attributes("-fullscreen", True)
                self.after(50, lambda: self.attributes("-fullscreen", False))
            except Exception:
                pass

    def _build_style(self):
        style = ttk.Style(self)
        # Тёмно-синяя (читабельная) тема как на примере пользователя.
        try:
            style.theme_use("clam")
        except Exception:
            pass

        # Palette (тёмная, нейтральные границы, мягкие акценты)
        bg = "#0B1220"        # окно
        panel = "#0E1829"     # панели
        card = "#101C31"      # карточки/таблицы
        accent = "#152441"    # кнопки/вкладки
        accent2 = "#1A2A47"   # hover/headers
        fg = "#E6EEF8"        # основной текст
        muted = "#A7B4C7"     # вторичный
        select = "#2B6CF6"    # выделение
        border = "#1A2740"    # нейтральные разделители

        self.configure(background=bg)

        style.configure("TFrame", background=bg)
        style.configure("Card.TFrame", background=card, padding=10)

        style.configure("TLabel", background=bg, foreground=fg)
        style.configure("Title.TLabel", font=("Segoe UI", 14, "bold"), background=bg, foreground=fg)
        style.configure("Sub.TLabel", font=("Segoe UI", 10), background=bg, foreground=muted)

        style.configure("TNotebook", background=bg, borderwidth=0)
        style.configure("TNotebook.Tab", background=accent, foreground=fg, padding=(10, 6))
        style.map("TNotebook.Tab",
                  background=[("selected", card)],
                  foreground=[("selected", fg)])

        # Кнопки: мягкий объём (raised/sunken), чуть более "живые" нажатия
        style.configure("TButton", padding=(10, 7), background=accent, foreground=fg, relief="raised", borderwidth=1)
        style.map("TButton",
                  background=[("active", accent2), ("pressed", "#223A66")],
                  foreground=[("disabled", muted)])

        style.configure("Primary.TButton", padding=(12, 7), relief="raised", borderwidth=1)
        style.configure("Danger.TButton", padding=(12, 7), relief="raised", borderwidth=1)
        style.map("Primary.TButton", background=[("active", accent2), ("pressed", "#223A66")])
        style.map("Danger.TButton", background=[("active", "#3A1B2B"), ("pressed", "#5A223A")])

        style.configure("Treeview", background=card, foreground=fg, fieldbackground=card, rowheight=24, bordercolor=border, lightcolor=border, darkcolor=border)
        style.map("Treeview", background=[("selected", select)], foreground=[("selected", "#ffffff")])
        style.configure("Treeview.Heading", background=accent2, foreground=fg, relief="flat", bordercolor=border, lightcolor=border, darkcolor=border)

        # Поля ввода/чекбоксы/фреймы: убираем "серые" плашки, приводим к единой гамме
        style.configure("TLabelframe", background=card, foreground=fg, bordercolor=border, relief="solid")
        style.configure("TLabelframe.Label", background=card, foreground=muted)
        style.configure("TEntry", fieldbackground=card, foreground=fg, insertcolor=fg, bordercolor=border)
        style.configure("TCheckbutton", background=bg, foreground=fg)
        style.map("TCheckbutton", foreground=[("disabled", muted)])
        style.configure("TRadiobutton", background=bg, foreground=fg)

        # Панель-разделитель: нейтральные границы
        style.configure("TPanedwindow", background=bg)

        # Tk widgets (Listbox/Text) are not themed by ttk
        self._tk_bg = bg
        self._tk_panel = panel
        self._tk_card = card
        self._tk_fg = fg
        self._tk_muted = muted
        self._tk_select = select

        self._tk_border = border

    def _build_ui(self):
        # top header
        header = ttk.Frame(self, padding=10)
        header.pack(fill="x")
        ttk.Label(header, text=APP_TITLE, style="Title.TLabel").pack(side="left")

        # справа: статус + время старта (МСК) + прибыль
        rightbox = ttk.Frame(header)
        rightbox.pack(side="right", anchor="e")
        self.status_var = tk.StringVar(value="ОСТАНОВЛЕНО")
        self.start_time_var = tk.StringVar(value="Время старта — —")
        self.elapsed_var = tk.StringVar(value="Сессия: 00:00:00")
        self.profit_var = tk.StringVar(value="Прибыль: —")
        self.ws_var = tk.StringVar(value="WS: —")
        ttk.Label(rightbox, textvariable=self.status_var, style="Sub.TLabel").pack(anchor="e")
        ttk.Label(rightbox, textvariable=self.start_time_var, style="Sub.TLabel").pack(anchor="e")
        ttk.Label(rightbox, textvariable=self.elapsed_var, style="Sub.TLabel").pack(anchor="e")
        ttk.Label(rightbox, textvariable=self.profit_var, style="Sub.TLabel").pack(anchor="e")
        ttk.Label(rightbox, textvariable=self.ws_var, style="Sub.TLabel").pack(anchor="e")

        # main body split
        body = ttk.Panedwindow(self, orient="horizontal")
        body.pack(fill="both", expand=True, padx=10, pady=(0,10))

        left = ttk.Frame(body)
        right = ttk.Frame(body)
        body.add(left, weight=1)
        body.add(right, weight=0)

        # Фиксируем ширину правой части под мониторинг (760px)
        try:
            body.paneconfigure(right, width=760, minsize=760)
        except Exception:
            pass
        self._body_paned = body
        self._left_pane = left
        self._right_pane = right

        def _enforce_right_width(_evt=None):
            # Держим правую часть ~760px даже при ресайзе окна.
            try:
                if self._sash_lock:
                    return
                self._sash_lock = True
                w = body.winfo_width()
                if w and w > 200:
                    newpos = max(260, int(w) - 760)
                    try:
                        body.sashpos(0, newpos)
                    except Exception:
                        pass
            finally:
                self._sash_lock = False

        body.bind("<Configure>", _enforce_right_width)
        self.after(150, _enforce_right_width)

        self._build_left(left)
        self._build_right(right)

    def _build_left(self, parent):
        nb = ttk.Notebook(parent)
        nb.pack(fill="both", expand=True)

        self.tab_symbols = ttk.Frame(nb, padding=10)
        self.tab_trade = ttk.Frame(nb, padding=10)
        # История сделок по монетам за сессию (агрегация по закрытым сделкам)
        # Важно: вкладка должна существовать ДО вызова self._build_history_tab(...)
        self.tab_history = ttk.Frame(nb, padding=10)
        self.tab_assets = ttk.Frame(nb, padding=10)
        self.tab_settings = ttk.Frame(nb, padding=10)
        self.tab_logs = ttk.Frame(nb, padding=10)

        nb.add(self.tab_symbols, text="Настройки")
        nb.add(self.tab_trade, text="Торговля")
        nb.add(self.tab_history, text="История")
        nb.add(self.tab_assets, text="Активы")
        nb.add(self.tab_settings, text="Ключи OKX")
        nb.add(self.tab_logs, text="Логи")

        # Symbols tab
        top = ttk.Frame(self.tab_symbols)
        top.pack(fill="x")
        ttk.Label(top, text="Список котировок (OKX instId, напр. BTC-USDT)").pack(side="left")
        ttk.Button(top, text="Добавить", command=self._add_symbol).pack(side="right")
        ttk.Button(top, text="Удалить", command=self._remove_symbol).pack(side="right", padx=(0,6))

        self.symbol_entry = ttk.Entry(self.tab_symbols)
        self.symbol_entry.pack(fill="x", pady=6)

        self.symbols_list = tk.Listbox(
            self.tab_symbols,
            height=8,
            bg=self._tk_card,
            fg=self._tk_fg,
            selectbackground="#0078d4",
            selectforeground="#ffffff",
            relief="flat",
            highlightthickness=0,
        )
        self.symbols_list.pack(fill="x")

        opts = ttk.Frame(self.tab_symbols)
        opts.pack(fill="x", pady=10)
        self.auto_top_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(opts, text="Авто‑TOP (обновлять список каждый час)", variable=self.auto_top_var).pack(side="left")
        ttk.Label(opts, text="Кол-во:").pack(side="left", padx=(10,2))
        self.auto_top_count = ttk.Entry(opts, width=6)
        self.auto_top_count.pack(side="left")
        ttk.Label(opts, text="Обновление (мин):").pack(side="left", padx=(10,2))
        self.auto_top_refresh = ttk.Entry(opts, width=6)
        self.auto_top_refresh.pack(side="left")


        dead = ttk.Frame(self.tab_symbols)
        dead.pack(fill="x", pady=(8,0))
        self.dead_swap_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(dead, text="Автозамена мёртвых символов (без STOP/START)", variable=self.dead_swap_var).pack(side="left")
        ttk.Label(dead, text="Нет цены (сек):").pack(side="left", padx=(10,2))
        self.dead_no_tick = ttk.Entry(dead, width=6); self.dead_no_tick.pack(side="left")
        ttk.Label(dead, text="0 уверенности (сек):").pack(side="left", padx=(10,2))
        ttk.Label(dead, text="Пауза (сек):").pack(side="left", padx=(10,2))
        self.dead_cooldown = ttk.Entry(dead, width=6); self.dead_cooldown.pack(side="left")
        ttk.Label(dead, text="Бан (мин):").pack(side="left", padx=(10,2))
        self.dead_ban = ttk.Entry(dead, width=6); self.dead_ban.pack(side="left")

        btns = ttk.Frame(self.tab_symbols)
        btns.pack(fill="x", pady=(10,0))
        ttk.Button(btns, text="СТАРТ", style="Primary.TButton", command=self._start).pack(side="left")
        ttk.Button(btns, text="СТОП", style="Danger.TButton", command=self._stop).pack(side="left", padx=8)

        # Плавный стоп: останавливаем мониторинг, запрещаем новые BUY,
        # распродаём монеты купленные в этой сессии и только затем делаем STOP.
        self.smooth_stop_minutes_var = tk.StringVar(value="15")
        self.smooth_stop_max_var = tk.BooleanVar(value=False)

        ttk.Button(btns, text="ПЛАВНЫЙ СТОП", command=self._smooth_stop).pack(side="left", padx=(0, 6))
        self.smooth_stop_minutes_entry = ttk.Entry(btns, width=6, textvariable=self.smooth_stop_minutes_var)
        self.smooth_stop_minutes_entry.pack(side="left")
        ttk.Label(btns, text="мин", style="Sub.TLabel").pack(side="left", padx=(4, 10))
        ttk.Checkbutton(btns, text="Максимальное время", variable=self.smooth_stop_max_var, command=self._on_smooth_max_toggle).pack(side="left")
        ttk.Button(btns, text="Сохранить", command=self._save_cfg_from_ui).pack(side="right")

        # --- Настройки торговли: переносим во вкладку "Настройки" ниже START/STOP ---
        self._build_trading_settings_blocks(self.tab_symbols)

        # Trade tab: только торговля (ручные BUY/SELL + таблица сделок)
        trade_top = ttk.Frame(self.tab_trade)
        trade_top.pack(fill="x", pady=(0, 6))

        self.trade_symbol_var = tk.StringVar(value="")
        ttk.Label(trade_top, text="Выбранный символ:", style="Sub.TLabel").pack(side="left")
        ttk.Label(trade_top, textvariable=self.trade_symbol_var).pack(side="left", padx=(6, 0))

        ttk.Button(trade_top, text="КУПИТЬ", command=lambda: self._manual_order("buy")).pack(side="right")
        ttk.Button(trade_top, text="ПРОДАТЬ", command=lambda: self._manual_order("sell")).pack(side="right", padx=(0, 8))

        # Таблица сделок (как в расширении): хранит покупку/продажу и профит
        self._build_trade_panel(self.tab_trade)

        # Assets tab (баланс/активы OKX + защита от продажи базовых активов)
        self._build_history_tab(self.tab_history)
        self._build_assets_tab(self.tab_assets)

        # Settings tab (OKX keys etc.)
        keys = ttk.LabelFrame(self.tab_settings, text="Ключи OKX", padding=10)
        keys.pack(fill="x")
        ttk.Label(keys, text="API‑ключ:").grid(row=0, column=0, sticky="w")
        ttk.Label(keys, text="API‑секрет:").grid(row=1, column=0, sticky="w", pady=(6,0))
        ttk.Label(keys, text="Passphrase (парольная фраза):").grid(row=2, column=0, sticky="w", pady=(6,0))

        self.api_key = ttk.Entry(keys)
        self.api_secret = ttk.Entry(keys, show="*")
        self.passphrase = ttk.Entry(keys, show="*")
        self.api_key.grid(row=0, column=1, sticky="ew")
        self.api_secret.grid(row=1, column=1, sticky="ew", pady=(6,0))
        self.passphrase.grid(row=2, column=1, sticky="ew", pady=(6,0))
        keys.columnconfigure(1, weight=1)

        # Важное: среда OKX (реал/демо). Если ключи созданы в DEMO (simulated),
        # OKX иначе вернёт 401 code=50101 "APIKey does not match current environment".
        self.okx_sim_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(keys, text="Демо‑торговля OKX (симулятор)", variable=self.okx_sim_var).grid(
            row=3, column=1, sticky="w", pady=(6,0)
        )

        self.save_keys_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(keys, text="Сохранять ключи в config.json (обфусцировано)", variable=self.save_keys_var).grid(row=4, column=1, sticky="w", pady=(6,0))

        # Удобство: контекстное меню (вставка/копирование) для полей ключей
        for w in (self.api_key, self.api_secret, self.passphrase):
            self._attach_entry_context_menu(w)

        strat = ttk.LabelFrame(self.tab_settings, text="Стратегия", padding=10)
        strat.pack(fill="x", pady=10)
        # Отображение стратегии берём из config.json (если не найдено — показываем дефолт).
        current_strat = (self.cfg.get("strategy", {}) or {}).get("name") or "StrategyV3"
        ttk.Label(strat, text=f"Текущая стратегия: {current_strat}").pack(anchor="w")
        ttk.Label(
            strat,
            text="(Strategy V3 — rule-based Score, per-symbol thresholds, risk filters, без 'липовой уверенности')",
        ).pack(anchor="w")

        sysf = ttk.LabelFrame(self.tab_settings, text="Система", padding=10)
        sysf.pack(fill="x", pady=10)
        ttk.Button(sysf, text="Очистка кэша", command=self._clear_cache).pack(side="left")
        ttk.Button(sysf, text="Сохранить настройки", command=self._save_cfg_from_ui).pack(side="right")

        logs_wrap = ttk.Frame(self.tab_logs)
        logs_wrap.pack(fill="both", expand=True)

        self.logs_text = tk.Text(
            logs_wrap,
            height=24,
            wrap="word",
            bg=self._tk_card,
            fg=self._tk_fg,
            insertbackground=self._tk_fg,
            relief="flat",
            highlightthickness=0,
        )
        sb = ttk.Scrollbar(logs_wrap, orient="vertical", command=self.logs_text.yview)
        self.logs_text.configure(yscrollcommand=sb.set)

        self.logs_text.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        # Тег для жирного времени в квадратных скобках: [YYYY-MM-DD ...]
        try:
            base_font = tkfont.Font(font=self.logs_text.cget("font"))
            ts_font = tkfont.Font(font=base_font)
            ts_font.configure(weight="bold")
            self._logs_ts_font = ts_font
            self.logs_text.tag_configure("ts", font=ts_font)
            self.logs_text.tag_configure("buy_bg", background="#0b3b1a")
            self.logs_text.tag_configure("sell_bg", background="#3b0b0b")
            self.logs_text.tag_configure("sep", foreground=self._tk_muted)
        except Exception:
            pass

        self._make_text_readonly_copyable(self.logs_text)

        btnlog = ttk.Frame(self.tab_logs)
        btnlog.pack(fill="x", pady=6)
        ttk.Button(btnlog, text="Выгрузить в TXT", command=self._export_logs).pack(side="right")
        ttk.Button(btnlog, text="Очистить", command=self._clear_logs_view).pack(side="right", padx=(0,6))
        ttk.Button(btnlog, text="Обновить", command=self._refresh_log).pack(side="right", padx=(0,6))


    def _build_right(self, parent):
        # Right panel: dashboard table
        dash = ttk.Frame(parent, padding=10)
        dash.pack(fill="both", expand=True)

        ttk.Label(dash, text="Мониторинг (мульти‑символы)", style="Title.TLabel").pack(anchor="w")

        # освобождаем место под "Причина".
        # v3: мониторинг показывает Score и ключевые метрики.
        # Вместо этого показываем RuleScore (0..1) и ключевые метрики, на которых строится решение.
        # PRG (прогрев): показывает готовность канала к трейдингу (история загружена, окно данных готово)
        cols = ("symbol","prg","entry","last","lag_ms","action","reason","rsi14","macd_h","atr14","spread","volr","slope30","buy_ratio")
        self.tree = ttk.Treeview(dash, columns=cols, show="headings", height=18, style="Grid.Treeview")
        # Подгоняем ширины так, чтобы ВСЕ столбцы помещались в 760px.
        headings = {
            "symbol": "Сим",
            "prg": "ПРГ",
            "entry": "ENTRY",
            "last": "Цена",
            "lag_ms": "Лаг",
            "action": "Действ.",
            "reason": "Причина",
            "rsi14": "RSI",
            "macd_h": "MACDh",
            "atr14": "ATR%",
            "spread": "Spr%",
            "volr": "VolR",
            "slope30": "Slope30",
            "buy_ratio": "BUY%",
        }
        widths = {
            # Общая ширина НЕ увеличивается: ужимаем "Причина", расширяем числовые поля.
            "symbol": 70,
            "prg": 40,
            "entry": 85,
            "last": 70,
            "lag_ms": 45,
            "action": 55,
            "reason": 170,   # допускается обрезка
            "rsi14": 50,
            "macd_h": 60,
            "atr14": 55,
            "spread": 55,
            "volr": 55,
            "slope30": 70,
            "buy_ratio": 55,
        }
        for c in cols:
            self.tree.heading(c, text=headings.get(c, c))
            anchor = 'w' if c in ("symbol", "reason") else 'center'
            self.tree.column(c, width=widths.get(c, 60), anchor=anchor)
        self.tree.pack(fill="both", expand=True, pady=8)
        self.tree.bind("<<TreeviewSelect>>", self._on_select_symbol)

        # "Гирлянда" действий: BUY=зелёный, SELL=красный
        try:
            self.tree.tag_configure("row_buy", background="#166534")
            self.tree.tag_configure("row_sell", background="#7f1d1d")
            # BUY сигнал, но исполнение заблокировано (pending/max_pos/PRV stale/...)
            self.tree.tag_configure("row_buy_blocked", foreground="#f59e0b")
            # Entry визуал (только подсветка строки при 3+ прямоугольниках):
            # - если позиции НЕТ -> BUY readiness (жёлтый фон)
            # - если позиция ЕСТЬ -> SELL readiness (красный фон)
            self.tree.tag_configure("row_entry3_fg", foreground="#facc15")
            self.tree.tag_configure("row_entry4_fg", foreground="#fde047")
            self.tree.tag_configure("row_exit3_bg", background="#fecaca")
            self.tree.tag_configure("row_exit4_bg", background="#fca5a5")
        except Exception:
            pass

        # bottom summary
        self.bank_var = tk.StringVar(value="Equity: — | Cash: — | PosNotional: —")
        ttk.Label(dash, textvariable=self.bank_var, style="Sub.TLabel").pack(anchor="w")
        # Временные предупреждения: жёлтые, обновляются и исчезают если не актуальны (TTL 10s)
        # Критичные ошибки: красные, не исчезают в течение сессии.
        self.alert_var = tk.StringVar(value="")
        self.alert_label = ttk.Label(dash, textvariable=self.alert_var, foreground="#ef4444")
        self.alert_label.pack(anchor="w")
        self._alerts: dict[str, dict[str, float]] = {"critical": {}, "warn": {}}
        self._warn_ttl_sec = 10.0
        self._alert_max_critical = 6
        self.after(1000, self._refresh_alerts)

        # авто-обновление "Активы" 1-2 раза в секунду (UI обновляет кэш движка,
        # не дергает OKX каждую итерацию).
        self._assets_auto_refresh_ms = 700
        self.after(self._assets_auto_refresh_ms, self._assets_auto_refresh_loop)


    def _build_trading_settings_blocks(self, parent):
        """UI: компактные блоки настроек торговли (перенесены из вкладки "Торговля")."""
        wrap = ttk.Frame(parent)
        wrap.pack(fill="x", pady=(12, 0))

        ttl = ttk.Label(wrap, text="Настройки торговли", style="Sub.TLabel")
        ttl.pack(anchor="w", pady=(0, 6))

        grid = ttk.Frame(wrap)
        grid.pack(fill="x")
        grid.columnconfigure(0, weight=1)
        grid.columnconfigure(1, weight=1)

        # --- Блок 1: режим ---
        f_mode = ttk.LabelFrame(grid, text="Режим", padding=8)
        f_mode.grid(row=0, column=0, sticky="nsew", padx=(0, 8), pady=(0, 8))
        # Dry-run удалён (только реальная торговля при наличии OKX private ключей)
        self.auto_trade_var = tk.BooleanVar(value=True)
        self.snapshots_var = tk.BooleanVar(value=False)
        self.api_economy_var = tk.BooleanVar(value=False)

        ttk.Checkbutton(f_mode, text="Автоторговля по стратегии", variable=self.auto_trade_var).pack(anchor="w", pady=(4, 0))
        ttk.Checkbutton(f_mode, text="Снапшоты BUY/SELL (отладка)", variable=self.snapshots_var).pack(anchor="w", pady=(4, 0))
        ttk.Checkbutton(f_mode, text="Экономия API (для 50+ символов)", variable=self.api_economy_var).pack(anchor="w", pady=(4, 0))

        # --- Блок 2: время / ордера ---
        f_time = ttk.LabelFrame(grid, text="Время и ордера", padding=8)
        f_time.grid(row=0, column=1, sticky="nsew", pady=(0, 8))

        self.check_old_orders_var = tk.BooleanVar(value=False)
        chk = ttk.Checkbutton(f_time, text="Проверять старые ордера при старте", variable=self.check_old_orders_var, command=self._toggle_old_orders_hours)
        chk.grid(row=0, column=0, columnspan=4, sticky="w")

        ttk.Label(f_time, text="Период (ч):").grid(row=1, column=0, sticky="w", pady=(6, 0))
        self.check_old_orders_hours = ttk.Entry(f_time, width=8)
        self.check_old_orders_hours.grid(row=1, column=1, sticky="w", pady=(6, 0), padx=(6, 12))

        ttk.Label(f_time, text="Разгон (с):").grid(row=1, column=2, sticky="w", pady=(6, 0))
        self.warmup_sec = ttk.Entry(f_time, width=8)
        self.warmup_sec.grid(row=1, column=3, sticky="w", pady=(6, 0), padx=(6, 0))

        ttk.Label(f_time, text="Cooldown (с):").grid(row=2, column=0, sticky="w", pady=(6, 0))
        self.cooldown_sec = ttk.Entry(f_time, width=8)
        self.cooldown_sec.grid(row=2, column=1, sticky="w", pady=(6, 0), padx=(6, 12))

        # --- Блок 3: пороги ---
        # ВАЖНО: именные пороги по каждой монете зашиты (data/per_symbol_thresholds.json),
        # чтобы не терялись между версиями и сборками. В UI они НЕ редактируются.
        f_thr = ttk.LabelFrame(grid, text="Пороги стратегии (V3)", padding=8)
        f_thr.grid(row=1, column=0, sticky="nsew")

        ttk.Label(
            f_thr,
            text="Именные пороги по монетам зашиты в data/per_symbol_thresholds.json (UI не меняет их)",
            wraplength=360,
        ).grid(row=0, column=0, columnspan=4, sticky="w")

        self.allow_exceed_max_positions_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            f_thr,
            text="Разрешить превышать лимит позиций при Score ≥ порога",
            variable=self.allow_exceed_max_positions_var,
        ).grid(row=1, column=0, columnspan=4, sticky="w", pady=(8, 0))

        ttk.Label(f_thr, text="Порог превышения лимита (rule_score ≥):").grid(row=2, column=0, sticky="w", pady=(6,0))
        self.exceed_max_positions_score = ttk.Entry(f_thr, width=8)
        self.exceed_max_positions_score.grid(row=2, column=1, sticky="w", padx=(6,12), pady=(6,0))
        ttk.Label(f_thr, text="(пример: 0.92 / 0.95)").grid(row=2, column=2, columnspan=2, sticky="w", pady=(6,0))

        self.micro_profit_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            f_thr,
            text="Фиксация микро профита (net +0.07% → защита, +0.5–0.6% → фикс)",
            variable=self.micro_profit_var,
        ).grid(row=3, column=0, columnspan=4, sticky="w", pady=(8, 0))

        # Пользователь вводит 0.3 -> продаём при net>=0.3%.
        ttk.Label(f_thr, text="Микро‑профит: фикс при net ≥ (%):").grid(row=4, column=0, sticky="w", pady=(6, 0))
        self.micro_take_pct = ttk.Entry(f_thr, width=8)
        self.micro_take_pct.grid(row=4, column=1, sticky="w", padx=(6, 12), pady=(6, 0))
        ttk.Label(f_thr, text="(пример: 0.3 / 0.5 / 1.2)").grid(row=4, column=2, columnspan=2, sticky="w", pady=(6, 0))

        # --- Блок 4: размер ордера / ограничения ---
        f_limits = ttk.LabelFrame(grid, text="Размер и ограничения", padding=8)
        f_limits.grid(row=1, column=1, sticky="nsew", padx=(0, 0))

        self.order_size_mode = tk.StringVar(value="fixed")
        ttk.Label(f_limits, text="Режим размера:").grid(row=0, column=0, sticky="w")
        mode_wrap = ttk.Frame(f_limits)
        mode_wrap.grid(row=0, column=1, columnspan=3, sticky="w", padx=(6, 0))
        ttk.Radiobutton(mode_wrap, text="Фикс (USDT)", variable=self.order_size_mode, value="fixed").pack(side="left")
        ttk.Radiobutton(mode_wrap, text="% от USDT", variable=self.order_size_mode, value="percent").pack(side="left", padx=(10, 0))

        ttk.Label(f_limits, text="Фикс (USDT):").grid(row=1, column=0, sticky="w", pady=(6, 0))
        self.default_order_usd = ttk.Entry(f_limits, width=8)
        self.default_order_usd.grid(row=1, column=1, sticky="w", pady=(6, 0), padx=(6, 12))

        ttk.Label(f_limits, text="Процент (%):").grid(row=1, column=2, sticky="w", pady=(6, 0))
        self.order_size_pct = ttk.Entry(f_limits, width=8)
        self.order_size_pct.grid(row=1, column=3, sticky="w", pady=(6, 0), padx=(6, 0))

        ttk.Label(f_limits, text="Резерв USDT (%):").grid(row=2, column=0, sticky="w", pady=(6, 0))
        self.reserve_pct = ttk.Entry(f_limits, width=8)
        self.reserve_pct.grid(row=2, column=1, sticky="w", pady=(6, 0), padx=(6, 12))

        ttk.Label(f_limits, text="Лимит позиций:").grid(row=2, column=2, sticky="w", pady=(6, 0))
        self.max_positions = ttk.Entry(f_limits, width=8)
        self.max_positions.grid(row=2, column=3, sticky="w", pady=(6, 0), padx=(6, 0))

        ttk.Label(f_limits, text="Повторов на символ:").grid(row=3, column=0, sticky="w", pady=(6, 0))
        self.max_positions_per_symbol = ttk.Entry(f_limits, width=8)
        self.max_positions_per_symbol.grid(row=3, column=1, sticky="w", pady=(6, 0), padx=(6, 12))

        ttk.Label(f_limits, text="Мин. ордер (USDT):").grid(row=4, column=0, sticky="w", pady=(6, 0))
        self.min_order_usd = ttk.Entry(f_limits, width=8)
        self.min_order_usd.grid(row=4, column=1, sticky="w", pady=(6, 0), padx=(6, 12))

        ttk.Label(f_limits, text='Порог "пыль" (USD):').grid(row=4, column=2, sticky="w", pady=(6, 0))
        self.dust_usd_threshold = ttk.Entry(f_limits, width=8)
        self.dust_usd_threshold.grid(row=4, column=3, sticky="w", pady=(6, 0), padx=(6, 0))

        # на всякий случай, чтобы поле часов реагировало сразу
        self._toggle_old_orders_hours()

    def _toggle_old_orders_hours(self):
        """Включает/выключает поле периода проверки старых ордеров."""
        try:
            enabled = bool(self.check_old_orders_var.get())
        except Exception:
            enabled = False
        try:
            state = "normal" if enabled else "disabled"
            self.check_old_orders_hours.configure(state=state)
        except Exception:
            pass


    def _load_cfg_to_ui(self):
        okx = self.cfg.get("okx", {}) or {}
        salt = self.cfg.get("crypto_salt", "ATE")

        # ключи: показываем расшифрованные значения (если сохранены)
        self.api_key.delete(0, "end"); self.api_key.insert(0, deobfuscate(okx.get("api_key",""), salt))
        self.api_secret.delete(0, "end"); self.api_secret.insert(0, deobfuscate(okx.get("api_secret",""), salt))
        self.passphrase.delete(0, "end"); self.passphrase.insert(0, deobfuscate(okx.get("passphrase",""), salt))
        self.okx_sim_var.set(bool(okx.get("simulated_trading", False)))
        self.save_keys_var.set(bool(okx.get("save_keys", False)))

        tcfg = self.cfg.get("trading", {}) or {}
        pass  # dry-run removed
        self.auto_trade_var.set(bool(tcfg.get("auto_trade", True)))
        self.snapshots_var.set(bool(tcfg.get("snapshots_enabled", False)))
        self.api_economy_var.set(bool(tcfg.get("api_economy_mode", False)))

        self.check_old_orders_var.set(bool(tcfg.get("check_old_orders", False)))
        self.check_old_orders_hours.delete(0,"end"); self.check_old_orders_hours.insert(0, str(tcfg.get("check_old_orders_hours", 5)))
        self._toggle_old_orders_hours()

        self.warmup_sec.delete(0,"end"); self.warmup_sec.insert(0, str(tcfg.get("warmup_sec", 60)))
        # ВАЖНО: v3_buy_score_min / v3_sell_score_min не редактируются в UI.
        # Они остаются фиксированными (1.0) и используются только для внутренних проверок.
        tcfg["v3_buy_score_min"] = float(tcfg.get("v3_buy_score_min", 1.0) or 1.0)
        tcfg["v3_sell_score_min"] = float(tcfg.get("v3_sell_score_min", 1.0) or 1.0)
        self.allow_exceed_max_positions_var.set(bool(tcfg.get("allow_exceed_max_positions", False)))
        try:
            self.exceed_max_positions_score.delete(0, "end")
            self.exceed_max_positions_score.insert(0, str(float(tcfg.get("exceed_max_positions_score", 0.95) or 0.95)))
        except Exception:
            pass
        self.micro_profit_var.set(bool(tcfg.get("micro_profit_enabled", True)))
        try:
            v = float(tcfg.get("micro_profit_take_net_pct", 0.005) or 0.005)
            self.micro_take_pct.delete(0, "end")
            self.micro_take_pct.insert(0, str(round(v * 100.0, 3)))
        except Exception:
            try:
                self.micro_take_pct.delete(0, "end")
                self.micro_take_pct.insert(0, "0.55")
            except Exception:
                pass
        self.cooldown_sec.delete(0,"end"); self.cooldown_sec.insert(0, str(tcfg.get("cooldown_sec", 10.0)))

        self.order_size_mode.set(str(tcfg.get("order_size_mode", "fixed")))
        self.default_order_usd.delete(0,"end"); self.default_order_usd.insert(0, str(tcfg.get("default_order_usd", 500.0)))
        self.order_size_pct.delete(0,"end"); self.order_size_pct.insert(0, str(tcfg.get("order_size_pct", 5.0)))
        self.reserve_pct.delete(0,"end"); self.reserve_pct.insert(0, str(tcfg.get("min_cash_reserve_pct", 10.0)))

        self.max_positions.delete(0,"end"); self.max_positions.insert(0, str(tcfg.get("max_positions", 8)))
        self.max_positions_per_symbol.delete(0,"end"); self.max_positions_per_symbol.insert(0, str(tcfg.get("max_positions_per_symbol", 1)))
        self.min_order_usd.delete(0,"end"); self.min_order_usd.insert(0, str(tcfg.get("min_order_usd", 10.0)))
        self.dust_usd_threshold.delete(0,"end"); self.dust_usd_threshold.insert(0, str(tcfg.get("dust_usd_threshold", 1.0)))

        scfg = self.cfg.get("symbols", {}) or {}
        self.auto_top_var.set(bool(scfg.get("auto_top", False)))
        self.auto_top_count.delete(0,"end"); self.auto_top_count.insert(0, str(scfg.get("auto_top_count", 30)))
        self.auto_top_refresh.delete(0,"end"); self.auto_top_refresh.insert(0, str(scfg.get("auto_top_refresh_min", 60)))
        self.dead_swap_var.set(bool(scfg.get("auto_dead_swap", True)))
        self.dead_no_tick.delete(0,"end"); self.dead_no_tick.insert(0, str(scfg.get("dead_no_tick_sec", 120)))
        self.dead_cooldown.delete(0,"end"); self.dead_cooldown.insert(0, str(scfg.get("dead_swap_cooldown_sec", 45)))
        self.dead_ban.delete(0,"end"); self.dead_ban.insert(0, str(scfg.get("dead_ban_min", 30)))

        self.symbols_list.delete(0,"end")
        for sym in (scfg.get("list") or []):
            self.symbols_list.insert("end", sym)

    def _save_cfg_from_ui(self):
        # Сохраняем настройки из UI в data/config.json
        salt = self.cfg.get("crypto_salt", "ATE")

        okx = self.cfg.setdefault("okx", {})
        okx["simulated_trading"] = bool(self.okx_sim_var.get())
        okx["save_keys"] = bool(self.save_keys_var.get())
        if okx["save_keys"]:
            okx["api_key"] = obfuscate(self.api_key.get().strip(), salt)
            okx["api_secret"] = obfuscate(self.api_secret.get().strip(), salt)
            okx["passphrase"] = obfuscate(self.passphrase.get().strip(), salt)
        else:
            okx["api_key"] = ""
            okx["api_secret"] = ""
            okx["passphrase"] = ""

        tcfg = self.cfg.setdefault("trading", {})
        tcfg["dry_run"] = False  # dry-run removed
        tcfg["auto_trade"] = bool(self.auto_trade_var.get())
        tcfg["snapshots_enabled"] = bool(self.snapshots_var.get())
        tcfg["api_economy_mode"] = bool(self.api_economy_var.get())
        tcfg["check_old_orders"] = bool(self.check_old_orders_var.get())
        try:
            tcfg["check_old_orders_hours"] = float(self.check_old_orders_hours.get())
        except Exception:
            tcfg["check_old_orders_hours"] = 5


        # автоторговля/пороги
        try:
            tcfg["warmup_sec"] = int(self.warmup_sec.get())
        except Exception:
            tcfg["warmup_sec"] = 60
        # ВАЖНО: именные пороги по монетам зашиты, UI не предлагает менять BUY/SELL пороги.
        # Эти значения фиксируем, чтобы при сохранении конфигурации они не "уплывали".
        tcfg["v3_buy_score_min"] = 1.0
        tcfg["v3_sell_score_min"] = 1.0
        tcfg["allow_exceed_max_positions"] = bool(self.allow_exceed_max_positions_var.get())
        try:
            tcfg["micro_profit_enabled"] = bool(self.micro_profit_var.get())
        except Exception:
            tcfg["micro_profit_enabled"] = False

        # В конфиге храним в долях (0.003 = 0.3%), чтобы стратегия не гадала.
        try:
            _pct = float(self.micro_take_pct.get())
            if _pct < 0:
                _pct = 0.0
            # защитим от случайного ввода "30" вместо "0.30" —
            # если пользователь ввёл > 20, считаем, что это проценты (30% — слишком),
            # но всё равно ограничим 10%.
            if _pct > 10.0:
                _pct = 10.0
            tcfg["micro_profit_take_net_pct"] = float(_pct) / 100.0
        except Exception:
            tcfg["micro_profit_take_net_pct"] = float(tcfg.get("micro_profit_take_net_pct", 0.005) or 0.005)
        # порог для превышения оставляем фиксированным (≥0.95)
        try:
            tcfg["exceed_max_positions_score"] = float(self.exceed_max_positions_score.get() or 0.95)
        except Exception:
            tcfg["exceed_max_positions_score"] = 0.95
        try:
            tcfg["cooldown_sec"] = float(self.cooldown_sec.get())
        except Exception:
            tcfg["cooldown_sec"] = 10.0

        # размер ордера / резерв
        tcfg["order_size_mode"] = str(self.order_size_mode.get() or "fixed")
        try:
            tcfg["default_order_usd"] = float(self.default_order_usd.get())
        except Exception:
            tcfg["default_order_usd"] = 500.0
        try:
            tcfg["order_size_pct"] = float(self.order_size_pct.get())
        except Exception:
            tcfg["order_size_pct"] = 5.0
        try:
            tcfg["min_cash_reserve_pct"] = float(self.reserve_pct.get())
        except Exception:
            tcfg["min_cash_reserve_pct"] = 10.0

        # лимиты безопасности
        try:
            tcfg["max_positions"] = int(float(self.max_positions.get()))
        except Exception:
            tcfg["max_positions"] = 8
        try:
            tcfg["max_positions_per_symbol"] = int(float(self.max_positions_per_symbol.get()))
        except Exception:
            tcfg["max_positions_per_symbol"] = 1
        try:
            tcfg["min_order_usd"] = float(self.min_order_usd.get())
        except Exception:
            tcfg["min_order_usd"] = 10.0

        # порог "пыль"
        try:
            tcfg["dust_usd_threshold"] = float(self.dust_usd_threshold.get())
        except Exception:
            tcfg["dust_usd_threshold"] = 1.0

        scfg = self.cfg.setdefault("symbols", {})
        scfg["auto_top"] = bool(self.auto_top_var.get())
        try:
            scfg["auto_top_count"] = int(self.auto_top_count.get().strip())
        except Exception:
            scfg["auto_top_count"] = 30
        try:
            scfg["auto_top_refresh_min"] = int(self.auto_top_refresh.get().strip())
        except Exception:
            scfg["auto_top_refresh_min"] = 60
        scfg["auto_dead_swap"] = bool(self.dead_swap_var.get())
        try:
            scfg["dead_no_tick_sec"] = int(self.dead_no_tick.get().strip())
        except Exception:
            scfg["dead_no_tick_sec"] = 120
        try:
            scfg["dead_swap_cooldown_sec"] = int(self.dead_cooldown.get().strip())
        except Exception:
            scfg["dead_swap_cooldown_sec"] = 45
        try:
            scfg["dead_ban_min"] = int(self.dead_ban.get().strip())
        except Exception:
            scfg["dead_ban_min"] = 30
        scfg["list"] = list(self.symbols_list.get(0, "end"))

        self.cfg_mgr.data = self.cfg
        self.cfg_mgr.save()
        self._apply_runtime_settings_after_save()
        messagebox.showinfo(APP_NAME, "Настройки сохранены.")

    
    def _apply_runtime_settings_after_save(self):
        """Применить настройки к работающему движку без STOP/START.

        - при Auto‑TOP: сразу триггерим refresh_top_now_runtime(), чтобы изменение количества
          вступало в силу мгновенно (и лишние каналы отключались).
        - при ручном списке: синхронизируем каналы под текущий список (добавить/убрать).
        """
        if not getattr(self, "_session_running", False):
            return
        if self.engine is None:
            return

        # поэтому выполняем в фоновом потоке, чтобы UI не «вис» и мониторинг не казался
        # остановленным. Движок при этом продолжает работать.
        import threading

        def worker():
            # 1) применяем параметры торговли/порогов/автоторговли прямо во время работы
            try:
                self.engine.apply_runtime_config()
            except Exception:
                pass

            # 2) синхронизация символов (добавить и отключить лишние) + моментальный Auto‑TOP refresh
            try:
                scfg = self.cfg.get("symbols", {}) or {}
                if bool(scfg.get("auto_top", False)):
                    # моментально обновляем TOP и приводим каналы в соответствие (без ожидания таймера)
                    try:
                        self.engine.refresh_top_now_runtime(user_trigger=True)
                    except Exception:
                        pass
                else:
                    syms = list(self.symbols_list.get(0, "end"))
                    # нормализация
                    norm = []
                    for s in syms:
                        s = str(s or "").strip().upper()
                        if not s:
                            continue
                        if '-' not in s:
                            s = s + '-USDT'
                        norm.append(s)
                    # приводим каналы под список
                    try:
                        self.engine.reconcile_symbols_runtime(norm)
                    except Exception:
                        # fallback: старый режим — добавляем новые
                        for s in norm:
                            if hasattr(self, "_iid_map") and isinstance(getattr(self, "_iid_map", None), dict) and s in self._iid_map:
                                continue
                            ok, inst, reason = self.engine.add_symbol_runtime(s)
                            if ok:
                                # ВНИМАНИЕ: UI-операции из потока не делаем.
                                pass
                            else:
                                # UI-переменные не трогаем из фонового потока.
                                pass
            except Exception:
                pass

        threading.Thread(target=worker, daemon=True).start()


    def _add_symbol(self):
        s = self.symbol_entry.get().strip()
        if not s:
            return
        s_norm = str(s).strip().upper()
        if '-' not in s_norm:
            s_norm = s_norm + "-USDT"
        # 1) update UI list (persist)
        items = set(self.symbols_list.get(0, "end"))
        if s_norm not in items:
            self.symbols_list.insert("end", s_norm)
        self.symbol_entry.delete(0, "end")

        # 2) если сессия уже запущена — добавляем канал на лету
        try:
            if getattr(self, "_session_running", False) and self.engine is not None:
                ok, inst, reason = self.engine.add_symbol_runtime(s_norm)
                if ok:
                    self._activate_monitoring_symbol(inst)
                else:
                    messagebox.showwarning(APP_NAME, f"Не удалось добавить {inst}: {reason}")
        except Exception:
            pass

    def _remove_symbol(self):
        sel = list(self.symbols_list.curselection())
        if not sel:
            return
        for i in reversed(sel):
            self.symbols_list.delete(i)

    def _on_select_symbol(self, _evt=None):
        sel = self.tree.selection()
        if not sel:
            return
        iid = sel[0]
        sym = self.tree.set(iid, "symbol")
        self.trade_symbol_var.set(sym)

    def _start(self):
        self._save_cfg_from_ui_silent()
        syms = list(self.symbols_list.get(0, "end"))
        if not syms and not self.auto_top_var.get():
            messagebox.showwarning(APP_NAME, "Добавь хотя бы один символ.")
            return

        # private client
        okx = self.cfg.get("okx", {}) or {}
        salt = self.cfg.get("crypto_salt", "ATE")
        api_key = deobfuscate(okx.get("api_key",""), salt) if okx.get("save_keys") else self.api_key.get().strip()
        api_secret = deobfuscate(okx.get("api_secret",""), salt) if okx.get("save_keys") else self.api_secret.get().strip()
        passphrase = deobfuscate(okx.get("passphrase",""), salt) if okx.get("save_keys") else self.passphrase.get().strip()
        self.engine.set_private(api_key, api_secret, passphrase, simulated_trading=bool(okx.get("simulated_trading", False)))

        # UI-only: новая сессия
        self._smooth_stop_active = False
        self._session_running = True
        self._session_elapsed_hold = 0
        try:
            self.elapsed_var.set("Сессия: 00:00:00")
        except Exception:
            pass

        # UI-only: фиксируем время старта (МСК) и базовый PnL по закрытым сделкам
        try:
            import datetime
            self._run_started_ts = time.time()
            dt_msk = datetime.datetime.utcnow() + datetime.timedelta(hours=3)
            self.start_time_var.set(f"Время старта — {dt_msk.strftime('%H.%M')} (МСК)")
        except Exception:
            self._run_started_ts = time.time()
            self.start_time_var.set("Время старта — —")

        try:
            base = 0.0
            for tr in self.engine.portfolio.trade_rows():
                if getattr(tr, 'sell_ts', 0) and float(getattr(tr, 'sell_ts', 0) or 0) > 0:
                    p, _ = tr.realized_pnl()
                    base += float(p or 0.0)
            self._pnl_baseline = float(base)
            self.profit_var.set("Прибыль: $0.00")
        except Exception:
            self._pnl_baseline = 0.0
            self.profit_var.set("Прибыль: —")

        # сброс таблицы мониторинга под текущий набор символов
        self._sync_monitoring_table(syms)
        try:
            self.engine.start(syms)
            self.status_var.set("РАБОТАЕТ — запуск...")
        except Exception as e:
            # Не допускаем 'тихого' нестарта: показываем критическую причину.
            self.status_var.set("ОШИБКА — запуск не удался")
            try:
                self._ingest_alert('critical', f"ENGINE START FAILED: {e}")
            except Exception:
                pass
            try:
                messagebox.showerror("ATE 6PRO", f"Не удалось запустить движок:\n{e}")
            except Exception:
                pass

        log_event(self.data_path, {"level":"INFO","msg":"START", "extra":{"symbols":syms}})

    def _stop(self, call_engine: bool = True):
        """Жёсткий STOP.

        Важно по требованиям:
        - после STOP/ПЛАВНОГО STOP значения "Время старта", "Сессия" и "Прибыль" НЕ исчезают;
        - они сбрасываются только при закрытии программы.
        """
        # фиксируем таймер
        try:
            if self._session_running and float(self._run_started_ts or 0.0) > 0:
                self._session_elapsed_hold = int(max(0.0, time.time() - float(self._run_started_ts)))
        except Exception:
            pass
        self._session_running = False        # финализируем прибыль (ТОЛЬКО реализованный PnL по закрытым сделкам)
        try:
            trades = self.engine.portfolio.trade_rows()
            realized = 0.0
            for tr in trades:
                if getattr(tr, 'sell_ts', 0) and float(getattr(tr, 'sell_ts', 0) or 0) > 0:
                    p, _ = tr.realized_pnl()
                    realized += float(p or 0.0)
            prof = float(realized) - float(self._pnl_baseline or 0.0)
            self.profit_var.set(f"Прибыль: ${prof:.2f}")
        except Exception:
            pass

        if call_engine:
            self.engine.stop()

        self.status_var.set("ОСТАНОВЛЕНО")
        # замораживаем дальнейший авто‑пересчёт прибыли
        self._run_started_ts = 0.0
        self._smooth_stop_active = False
        log_event(self.data_path, {"level":"INFO","msg":"STOP", "extra":{"call_engine":call_engine}})

    def _tick_session_timer(self):
        """Таймер с момента старта.

        Требование: после STOP/ПЛАВНОГО STOP таймер останавливается,
        но значение не пропадает до закрытия программы.
        """
        try:
            if self._session_running and float(self._run_started_ts or 0.0) > 0:
                self._session_elapsed_hold = int(max(0.0, time.time() - float(self._run_started_ts)))
            sec = int(self._session_elapsed_hold or 0)
            h = sec // 3600
            m = (sec % 3600) // 60
            s = sec % 60
            self.elapsed_var.set(f"Сессия: {h:02d}:{m:02d}:{s:02d}")
        except Exception:
            pass
        self.after(500, self._tick_session_timer)

    def _on_smooth_max_toggle(self):
        try:
            if self.smooth_stop_max_var.get():
                self.smooth_stop_minutes_entry.configure(state="disabled")
            else:
                self.smooth_stop_minutes_entry.configure(state="normal")
        except Exception:
            pass

    def _smooth_stop(self):
        """Плавный STOP.

        - замораживает мониторинг (правая таблица)
        - запрещает новые BUY
        - распродаёт монеты, купленные в текущей сессии
        - после полного выхода делает STOP (как обычный)
        """
        try:
            if not self.engine.is_running():
                messagebox.showwarning(APP_NAME, "Движок не запущен. Нажми СТАРТ.")
                return
        except Exception:
            pass

        max_time = bool(self.smooth_stop_max_var.get())
        minutes = None
        if not max_time:
            try:
                minutes = int(str(self.smooth_stop_minutes_var.get() or "").strip() or "0")
            except Exception:
                minutes = 0
            if minutes <= 0:
                minutes = 15

        # фиксируем флаг плавного стопа (для статуса), но НЕ замораживаем мониторинг/таймер:
        # требование: всё обновляется до момента продажи последней позиции.
        self._smooth_stop_active = True

        # запускаем плавный стоп в движке

        try:
            res = self.engine.request_smooth_stop(minutes=minutes, max_time=max_time)
            if isinstance(res, dict) and not res.get("ok", True):
                messagebox.showerror(APP_NAME, f"Плавный стоп не запущен: {res.get('error')}")
                self._smooth_stop_active = False
                return
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Плавный стоп не запущен: {e}")
            self._smooth_stop_active = False
            return

        # UI статус
        if max_time:
            self.status_var.set("ПЛАВНЫЙ СТОП — максимальное время...")
        else:
            self.status_var.set(f"ПЛАВНЫЙ СТОП — до {minutes} мин...")

    def _quick_sell(self, symbol: str):
        """Быстрый SELL прямо в строке карточки покупки."""
        try:
            sym = str(symbol).strip()
            if not sym:
                return
            last = 0.0
            try:
                row = self.latest_ticks.get(sym, {})
                last = float(row.get("last") or 0.0)
            except Exception:
                last = 0.0
            # Во время smooth_stop могут висеть pending-ордера от автозакрытия,
            # поэтому разрешаем force SELL.
            try:
                ss = bool(getattr(self.engine, "shared_state", {}) and self.engine.shared_state.get("smooth_stop", False))
            except Exception:
                ss = False
            force = bool(getattr(self, "_smooth_stop_active", False) or ss)
            res = self.engine.manual_trade(symbol=sym, side="sell", last_price=last or 0.0, source="manual", force=force)
            if not res.get("ok"):
                messagebox.showerror(APP_NAME, f"SELL не выполнен: {res.get('error')}")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"SELL не выполнен: {e}")

    def _manual_order(self, side: str):
        sym = self.trade_symbol_var.get().strip()
        if not sym:
            messagebox.showwarning(APP_NAME, "Выбери символ в таблице справа.")
            return

        # Берём последнюю цену из кэша тиков (не зависим от UI таблицы)
        last = 0.0
        try:
            row = self.latest_ticks.get(sym, {})
            last = float(row.get("last") or 0.0)
        except Exception:
            last = 0.0

        # SAFETY: если Реальная торговля — разрешаем реальный ордер только после START,
        # когда подключён приватный клиент OKX (EngineController.set_private).
        if True:  # dry-run removed; require private client always
            if getattr(self.engine, 'private', None) is None:
                messagebox.showwarning(APP_NAME,
                    "Реальная торговля, но приватный клиент OKX не подключён.\n\n"
                    "1) Заполни ключи OKX во вкладке 'Настройки' (и выбери Демо/Реал).\n"
                    "2) Нажми СТАРТ.\n"
                    "3) Повтори BUY/SELL."
                )
                return

        force = False
        try:
            if str(side).lower().strip() == "sell":
                ss = bool(getattr(self.engine, "shared_state", {}) and self.engine.shared_state.get("smooth_stop", False))
                force = bool(getattr(self, "_smooth_stop_active", False) or ss)
        except Exception:
            force = False

        res = self.engine.manual_trade(symbol=sym, side=side, last_price=last, source="manual", force=force)
        action_txt = "КУПИТЬ" if side.lower() == "buy" else "ПРОДАТЬ"

        # Короткий статус для пользователя (умные уведомления)
        try:
            if isinstance(res, dict):
                if res.get("error"):
                    self._ingest_alert("critical", str(res.get("error") or ""))
                elif res.get("warn"):
                    self._ingest_alert("warn", str(res.get("warn") or ""))
                elif res.get("msg"):
                    # сообщения ручной торговли считаем временными
                    self._ingest_alert("warn", str(res.get("msg") or ""))
            else:
                s = str(res)
                if s.strip():
                    self._ingest_alert("warn", s)
        except Exception:
            pass

        # Лог в UI
        try:
            self.logs_text.insert("end", f"\n{action_txt} {sym} -> {res}\n")
            self.logs_text.see("end")

            # highlight BUY/SELL lines and add vertical separator feel
            end_index2 = self.logs_text.index("end-1c")
            end_line2 = int(end_index2.split('.')[0])
            for ln in range(1, end_line2 + 1):
                a = f"{ln}.0"
                b = f"{ln}.end"
                s = self.logs_text.get(a, b)
                if not s:
                    continue
                if 'КУПИТЬ' in s or ' BUY ' in s or '"type": "BUY"' in s:
                    self.logs_text.tag_add('buy_bg', a, b)
                elif 'ПРОДАТЬ' in s or ' SELL ' in s or '"type": "SELL"' in s:
                    self.logs_text.tag_add('sell_bg', a, b)
                # add a subtle separator symbol if line starts with '['
                if s.lstrip().startswith('['):
                    # ensure first char is a thin separator
                    if not s.startswith('│'):
                        try:
                            self.logs_text.insert(a, '│ ')
                            self.logs_text.tag_add('sep', a, f"{ln}.2")
                        except Exception:
                            pass

        except Exception:
            pass

        try:
            log_event(self.data_path, {"level":"INFO","msg":"manual_order_ui","extra":{"action":action_txt,"symbol":sym,"res":res}})
        except Exception:
            pass

    def _clear_cache(self):
        if messagebox.askyesno(APP_NAME, "Удалить кэш/логи/историю/ордера dry-run?"):
            clear_cache(self.data_path)
            messagebox.showinfo(APP_NAME, "Кэш очищен.")


    def _make_text_readonly_copyable(self, widget: tk.Text):
        """Делаем поле копируемым, но не редактируемым (Ctrl+C/Ctrl+A работают)."""
        widget.configure(state="normal")
        def on_key(e):
            # Ctrl+C / Ctrl+A разрешаем
            if (e.state & 0x4) and e.keysym.lower() in ("c","a"):
                return None
            return "break"
        widget.bind("<Key>", on_key)

    def _attach_entry_context_menu(self, entry: ttk.Entry):
        """Контекстное меню (ПКМ) для Entry (вставка/копирование/вырезать/всё)."""
        m = tk.Menu(self, tearoff=0)
        m.add_command(label="Вставить", command=lambda: entry.event_generate("<<Paste>>"))
        m.add_command(label="Копировать", command=lambda: entry.event_generate("<<Copy>>"))
        m.add_command(label="Вырезать", command=lambda: entry.event_generate("<<Cut>>"))
        m.add_separator()
        m.add_command(label="Выделить всё", command=lambda: (entry.select_range(0, "end"), entry.icursor("end")))

        def popup(ev):
            try:
                entry.focus_set()
                m.tk_popup(ev.x_root, ev.y_root)
            finally:
                try:
                    m.grab_release()
                except Exception:
                    pass

        # Button-3 = right click
        entry.bind("<Button-3>", popup)

        # Explicit Ctrl+V fallback (некоторые окружения ломают стандартную привязку)
        entry.bind("<Control-v>", lambda e: (entry.event_generate("<<Paste>>"), "break")[1])
        entry.bind("<Control-V>", lambda e: (entry.event_generate("<<Paste>>"), "break")[1])
        entry.bind("<Shift-Insert>", lambda e: (entry.event_generate("<<Paste>>"), "break")[1])

    def _format_logs_view(self):
        """Форматирование блока логов:
        - выделяем таймстемпы
        - подсвечиваем BUY/SELL строки
        - добавляем тонкий вертикальный разделитель слева (│)
        """
        import re
        try:
            self.logs_text.tag_remove("ts", "1.0", "end")
            self.logs_text.tag_remove("buy_bg", "1.0", "end")
            self.logs_text.tag_remove("sell_bg", "1.0", "end")
            self.logs_text.tag_remove("sep", "1.0", "end")
        except Exception:
            return

        ts_pat = re.compile(r"\[[0-9]{4}-[0-9]{2}-[0-9]{2}[^\]]*\]")
        try:
            end_index = self.logs_text.index("end-1c")
            line_count = int(end_index.split(".")[0])
        except Exception:
            line_count = 0

        for i in range(1, line_count + 1):
            a = f"{i}.0"
            b = f"{i}.end"
            try:
                line = self.logs_text.get(a, b)
            except Exception:
                continue

            # timestamp bold
            for mm in ts_pat.finditer(line):
                try:
                    self.logs_text.tag_add("ts", f"{i}.{mm.start()}", f"{i}.{mm.end()}")
                except Exception:
                    pass

            # BUY/SELL background highlight
            try:
                if "КУПИТЬ" in line or '"type": "BUY"' in line or " BUY " in line:
                    self.logs_text.tag_add("buy_bg", a, b)
                elif "ПРОДАТЬ" in line or '"type": "SELL"' in line or " SELL " in line:
                    self.logs_text.tag_add("sell_bg", a, b)
            except Exception:
                pass

            # add left separator for readability
            try:
                if line.lstrip().startswith("[") and not line.startswith("│"):
                    self.logs_text.insert(a, "│ ")
                    self.logs_text.tag_add("sep", a, f"{i}.2")
            except Exception:
                pass


    def _clear_logs_view(self):
        try:
            self.logs_text.delete("1.0", "end")
        except Exception:
            pass

    def _export_logs(self):
        import datetime
        from tkinter import filedialog, messagebox

        txt = ""
        try:
            txt = self.logs_text.get("1.0", "end-1c")
        except Exception:
            txt = ""
        if not txt.strip():
            messagebox.showinfo("Логи", "Логи пустые — нечего выгружать.")
            return
        ts = datetime.datetime.now(MSK_TZ).strftime("%Y-%m-%d_%H-%M-%S")
        default_name = f"logs_export_{ts}.txt"
        path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            initialfile=default_name,
            filetypes=[("Text", "*.txt")]
        )
        if not path:
            return
        with open(path, "w", encoding="utf-8") as f:
            f.write(txt)
        messagebox.showinfo("Логи", f"Выгружено:\n{path}")

    def _refresh_log(self):
        # read today's log if exists
        import datetime
        log_path = os.path.join(self.data_path, "logs", f"app_{datetime.datetime.now(MSK_TZ).date().isoformat()}.log")
        try:
            self.logs_text.delete("1.0", "end")
        except Exception:
            return
        if os.path.exists(log_path):
            with open(log_path, "r", encoding="utf-8") as f:
                self.logs_text.insert("end", f.read())
        else:
            self.logs_text.insert("end", "(логов пока нет)")

        # подсветка времени + прокрутка вниз к последним событиям
        try:
            self._format_logs_view()
        except Exception:
            pass
        try:
            self.logs_text.see("end")

            # highlight BUY/SELL lines and add vertical separator feel
            end_index2 = self.logs_text.index("end-1c")
            end_line2 = int(end_index2.split('.')[0])
            for ln in range(1, end_line2 + 1):
                a = f"{ln}.0"
                b = f"{ln}.end"
                s = self.logs_text.get(a, b)
                if not s:
                    continue
                if 'КУПИТЬ' in s or ' BUY ' in s or '"type": "BUY"' in s:
                    self.logs_text.tag_add('buy_bg', a, b)
                elif 'ПРОДАТЬ' in s or ' SELL ' in s or '"type": "SELL"' in s:
                    self.logs_text.tag_add('sell_bg', a, b)
                # add a subtle separator symbol if line starts with '['
                if s.lstrip().startswith('['):
                    # ensure first char is a thin separator
                    if not s.startswith('│'):
                        try:
                            self.logs_text.insert(a, '│ ')
                            self.logs_text.tag_add('sep', a, f"{ln}.2")
                        except Exception:
                            pass

            self.logs_text.yview_moveto(1.0)
        except Exception:
            pass
    def _save_cfg_from_ui_silent(self):
        """То же самое, что _save_cfg_from_ui, но без messagebox."""
        salt = self.cfg.get("crypto_salt", "ATE")
        okx = self.cfg.setdefault("okx", {})
        okx["simulated_trading"] = bool(self.okx_sim_var.get())
        okx["save_keys"] = bool(self.save_keys_var.get())
        if okx["save_keys"]:
            okx["api_key"] = obfuscate(self.api_key.get().strip(), salt)
            okx["api_secret"] = obfuscate(self.api_secret.get().strip(), salt)
            okx["passphrase"] = obfuscate(self.passphrase.get().strip(), salt)

        tcfg = self.cfg.setdefault("trading", {})
        tcfg["dry_run"] = False  # dry-run removed
        tcfg["auto_trade"] = bool(self.auto_trade_var.get())
        tcfg["snapshots_enabled"] = bool(self.snapshots_var.get())
        tcfg["api_economy_mode"] = bool(self.api_economy_var.get())
        tcfg["check_old_orders"] = bool(self.check_old_orders_var.get())
        try:
            tcfg["check_old_orders_hours"] = float(self.check_old_orders_hours.get())
        except Exception:
            tcfg["check_old_orders_hours"] = 5

        # ВАЖНО: v3_buy_score_min / v3_sell_score_min не редактируются в UI.
        tcfg["v3_buy_score_min"] = 1.0
        tcfg["v3_sell_score_min"] = 1.0

        for key, default, cast in [
            ("warmup_sec", 60, int),
            ("cooldown_sec", 10.0, float),
            ("default_order_usd", 500.0, float),
            ("order_size_pct", 5.0, float),
            ("min_cash_reserve_pct", 10.0, float),
            ("min_order_usd", 10.0, float),
            ("dust_usd_threshold", 1.0, float),
        ]:
            try:
                widget = {
                    "warmup_sec": self.warmup_sec,
                    "cooldown_sec": self.cooldown_sec,
                    "default_order_usd": self.default_order_usd,
                    "order_size_pct": self.order_size_pct,
                    "min_cash_reserve_pct": self.reserve_pct,
                    "min_order_usd": self.min_order_usd,
                    "dust_usd_threshold": self.dust_usd_threshold,
                }[key]
                tcfg[key] = cast(widget.get())
            except Exception:
                tcfg[key] = default

        try:
            tcfg["max_positions"] = int(float(self.max_positions.get()))
        except Exception:
            tcfg["max_positions"] = 8
        try:
            tcfg["max_positions_per_symbol"] = int(float(self.max_positions_per_symbol.get()))
        except Exception:
            tcfg["max_positions_per_symbol"] = 1

        tcfg["order_size_mode"] = str(self.order_size_mode.get() or "fixed")

        scfg = self.cfg.setdefault("symbols", {})
        scfg["auto_top"] = bool(self.auto_top_var.get())
        try:
            scfg["auto_top_count"] = int(self.auto_top_count.get().strip())
        except Exception:
            scfg["auto_top_count"] = 30
        try:
            scfg["auto_top_refresh_min"] = int(self.auto_top_refresh.get().strip())
        except Exception:
            scfg["auto_top_refresh_min"] = 60
        scfg["auto_dead_swap"] = bool(self.dead_swap_var.get())
        try:
            scfg["dead_no_tick_sec"] = int(self.dead_no_tick.get().strip())
        except Exception:
            scfg["dead_no_tick_sec"] = 120
        try:
            scfg["dead_swap_cooldown_sec"] = int(self.dead_cooldown.get().strip())
        except Exception:
            scfg["dead_swap_cooldown_sec"] = 45
        try:
            scfg["dead_ban_min"] = int(self.dead_ban.get().strip())
        except Exception:
            scfg["dead_ban_min"] = 30
        scfg["list"] = list(self.symbols_list.get(0, "end"))

        self.cfg_mgr.data = self.cfg
        self.cfg_mgr.save()

    def _poll_ui_queue(self):
        drained = 0
        try:
            while drained < 200:
                msg = self.ui_queue.get_nowait()
                drained += 1
                self._handle_msg(msg)
        except Empty:
            pass
        self.after(200, self._poll_ui_queue)

    def _handle_msg(self, msg: dict):
        t = msg.get("type")
        if t == "tick":
            # ПЛАВНЫЙ СТОП не должен останавливать мониторинг/обновления PnL/баланса.
            # Он запрещает новые BUY и запускает ликвидацию в фоне, но UI продолжает обновляться.
            sym = msg.get("symbol")
            last = msg.get("last", 0.0)
            dec = msg.get("decision") or {}
            met = msg.get("metrics") or {}
            pos = msg.get("position") or {}

            # update tree row (O(1) по карте symbol→iid)
            if sym and (sym not in (self._iid_map or {})):
                self._activate_monitoring_symbol(sym)
            iid = (self._iid_map or {}).get(sym)
            # lag
            lag_ms = "—"
            try:
                last_ts = float(msg.get("last_ts") or 0.0)
                if last_ts:
                    lag_ms = str(int(max(0.0, (time.time() - last_ts)) * 1000))
            except Exception:
                lag_ms = "—"

            action_raw = str(dec.get('action_ui', dec.get('action','HOLD'))).upper()
            action_ru = {'BUY':'КУПИТЬ','SELL':'ПРОДАТЬ','HOLD':'ЖДАТЬ','BUY_BLOCKED':'КУПИТЬ⛔'}.get(action_raw, action_raw)

            # v3: Визуальная «степень подходящности» момента.
            # Это НЕ влияет на BUY/SELL (торговые решения определяет стратегия + защиты движка),
            # а лишь показывает, сколько условий (из всех метрик) сейчас совпадает.
            buy_ok = 0
            buy_total = 0
            sell_ok = 0
            sell_total = 0
            try:
                m = (dec.get('meta') or {})
                if isinstance(m, dict):
                    buy_ok = int(m.get('buy_ok') or m.get('buy_passed_checks') or 0)
                    buy_total = int(m.get('buy_total') or m.get('buy_total_checks') or 0)
                    sell_ok = int(m.get('sell_ok') or m.get('sell_passed_checks') or 0)
                    sell_total = int(m.get('sell_total') or m.get('sell_total_checks') or 0)
            except Exception:
                buy_ok, buy_total, sell_ok, sell_total = 0, 0, 0, 0

            
            def _bar_level(ok: int, total: int) -> int:
                # 0..4 по доле ok/total (используем floor, не round)
                try:
                    ok_i = int(ok)
                    tot_i = int(total)
                except Exception:
                    return 0
                if tot_i <= 0:
                    return 0
                if ok_i < 0:
                    ok_i = 0
                if ok_i > tot_i:
                    ok_i = tot_i
                lvl = int((ok_i / float(tot_i)) * 4.0)  # floor
                if lvl < 0:
                    lvl = 0
                if lvl > 4:
                    lvl = 4
                return lvl

            # ВНИМАНИЕ по требованиям пользователя:
            # - кубики показывают близость к ПОКУПКЕ/ПРОДАЖЕ (прогресс условий), а не факт действия
            # - 4-й кубик ДОЛЖЕН загораться только в момент сигнала BUY/SELL, а не просто при 100% совпадении условий
            # Поэтому: если action != BUY (или != SELL), то максимум 3 кубика.
            dec_action = str((dec or {}).get("action") or "HOLD").upper()

            # предпочитаем специальные поля прогресса, если стратегия их отдаёт
            try:
                m = (dec.get('meta') or {})
                if isinstance(m, dict):
                    buy_ok = int(m.get('entry_ok') or m.get('buy_ok') or m.get('buy_passed_checks') or 0)
                    buy_total = int(m.get('entry_total') or m.get('buy_total') or m.get('buy_total_checks') or 0)
                    sell_ok = int(m.get('exit_ok') or m.get('sell_ok') or m.get('sell_passed_checks') or 0)
                    sell_total = int(m.get('exit_total') or m.get('sell_total') or m.get('sell_total_checks') or 0)
            except Exception:
                buy_ok, buy_total, sell_ok, sell_total = 0, 0, 0, 0

            buy_lvl = _bar_level(buy_ok, buy_total)
            sell_lvl = _bar_level(sell_ok, sell_total)

            # ограничение 4-го кубика по действию (моментальному сигналу)
            if dec_action != "BUY":
                buy_lvl = min(3, buy_lvl)
            if dec_action != "SELL":
                sell_lvl = min(3, sell_lvl)

            # Кубики одинакового размера (эмодзи квадраты)
            BUY_ON, SELL_ON, OFF = "🟩", "🟥", "⬜"

            # Прямоугольники/кубики: если позиции НЕТ -> прогресс BUY, если позиция ЕСТЬ -> прогресс SELL
            has_pos = False
            try:
                _q = float((pos or {}).get("base_qty") or (pos or {}).get("qty") or 0.0)
                has_pos = _q > 0.0
            except Exception:
                has_pos = False

            if has_pos:
                entry_disp = (SELL_ON * sell_lvl + OFF * (4 - sell_lvl))
                entry_stage = sell_lvl
                entry_mode = "sell"
            else:
                entry_disp = (BUY_ON * buy_lvl + OFF * (4 - buy_lvl))
                entry_stage = buy_lvl
                entry_mode = "buy"

            # Прогрев канала (история и окно данных готовы)
            prg_ready = bool(msg.get("warmup_ready", False))
            prg_stage = str(msg.get("warmup_stage", ""))
            if prg_ready:
                prg_disp = "OK"
            else:
                # пока прогревается — показываем "…"
                prg_disp = "…" if prg_stage else "—"

            # v3: расширенные метрики
            try:
                vol = float(met.get('volume', 0.0) or 0.0)
                vol_sma = float(met.get('vol_sma20', 0.0) or 0.0)
                volr = (vol / vol_sma) if vol_sma > 0 else 1.0
            except Exception:
                volr = 1.0

            values = (
                sym,
                prg_disp,
                entry_disp,
                f"{float(last):.6f}".rstrip("0").rstrip("."),
                lag_ms,
                action_ru,
                self._reason_ru(str((dec.get('reason') or dec.get('reason_ui') or ''))),
                f"{float(met.get('rsi14',0.0)):.1f}",
                f"{float(met.get('macd_hist',0.0)):.4f}",
                f"{float(met.get('atr14_pct',0.0)):.2f}",
                f"{float(met.get('spread_pct',0.0)):.4f}",
                f"{float(volr):.2f}",
                f"{float(met.get('slope30_pct',0.0)):.3f}",
                f"{float(met.get('buy_ratio',0.5)):.2f}",
            )
            if iid is None:
                iid = self.tree.insert("", "end", values=values)
                self._iid_map[sym] = iid
                try:
                    self._row_cache_values[sym] = values
                except Exception:
                    pass
            else:
                # PERF: не дёргаем Treeview если значения не изменились (иначе ловим лаги при подсветке)
                try:
                    prev_vals = self._row_cache_values.get(sym)
                except Exception:
                    prev_vals = None
                if prev_vals != values:
                    for i, c in enumerate(self.tree["columns"]):
                        self.tree.set(iid, c, values[i])
                    try:
                        self._row_cache_values[sym] = values
                    except Exception:
                        pass

            # UI: гирлянда по действию (строка полностью меняет фон)
            try:
                if action_ru == "КУПИТЬ":
                    desired_tags = ("row_buy",)
                elif action_ru == "КУПИТЬ⛔":
                    desired_tags = ("row_buy_blocked",)
                elif action_ru == "ПРОДАТЬ":
                    desired_tags = ("row_sell",)
                else:
                    # HOLD: подсветка строки только если 3+ прямоугольника.
                    if entry_stage >= 3:
                        if entry_mode == "sell":
                            desired_tags = ("row_exit4_bg",) if entry_stage >= 4 else ("row_exit3_bg",)
                        else:
                            desired_tags = ("row_entry4_fg",) if entry_stage >= 4 else ("row_entry3_fg",)
                    else:
                        desired_tags = ()

                prev_tags = self._row_cache_tags.get(iid)
                if prev_tags != desired_tags:
                    self.tree.item(iid, tags=desired_tags)
                    self._row_cache_tags[iid] = desired_tags
            except Exception:
                pass

            port = msg.get("portfolio") or {}
            try:
                total_eq = float(port.get("total_equity") or 0.0)
                cash = float(port.get("cash") or 0.0)
                posn = float(port.get("positions_notional") or 0.0)
                cnt = int(port.get("positions_count") or 0)
            except Exception:
                total_eq, cash, posn, cnt = 0.0, 0.0, 0.0, 0

            sync_age = ""
            try:
                sync_ts = float(port.get("okx_sync_ts") or 0.0)
                if sync_ts:
                    sync_age = f" | OKX синхр.: {int(max(0.0, time.time()-sync_ts))}с назад"
            except Exception:
                sync_age = ""

            # обновляем банк не чаще 1 раза/сек (чтобы не дергать UI)
            now_ts = time.time()
            if now_ts - self._last_bank_update >= 1.0:
                self._last_bank_update = now_ts
                assets_usd = 0.0
                assets_cnt = 0
                try:
                    assets_usd = float(port.get('assets_usd') or 0.0)
                    assets_cnt = int(port.get('assets_count') or 0)
                except Exception:
                    assets_usd, assets_cnt = 0.0, 0
                self.bank_var.set(
                    f"OKX: Капитал {total_eq:.2f} | USDT {cash:.2f} | Активы {assets_usd:.2f} ({assets_cnt})"
                    f"  ||  БОТ: В позиции {posn:.2f} | Позиций {cnt}{sync_age}"
                )

            # dynamic status
            try:
                ch = len(self.engine.channels)
                self.status_var.set(f"РАБОТАЕТ — каналов: {ch}")
            except Exception:
                pass

        elif t in ("error","warn"):
            txt = msg.get("error") or msg.get("warn") or ""
            sev = "critical" if t == "error" else "warn"
            self._ingest_alert(sev, txt)
        elif t == "ws_status":
            # Показать оператору реальный статус WebSocket (жизнь/скорость/задержка)
            st = msg.get("ws_public") or {}
            stp = msg.get("ws_private") or st  # backward compatible
            try:
                on = "ON" if bool(st.get("public_connected")) else "OFF"
                mps = str(st.get("public_msgs_per_sec") or "0")
                age = str(st.get("public_last_msg_age_sec") or "0")
                pc = str(st.get("public_prices") or "0")
                err = str(st.get("public_error") or "").strip()
                if err:
                    err = " | err: " + err[:80]
                # private stream (orders/fills/account)
                pon = "ON" if bool(stp.get("private_connected")) else "OFF"
                pps = str(stp.get("private_msgs_per_sec") or "0")
                page = str(stp.get("private_last_msg_age_sec") or "0")
                perr = str(stp.get("private_error") or "").strip()
                if perr:
                    perr = " | perr: " + perr[:60]
                self.ws_var.set(
                    f"WS: {on} | msgs/s {mps} | age {age}s | px {pc}{err}"
                    f"  ||  PRV: {pon} | msgs/s {pps} | age {page}s{perr}"
                )
            except Exception:
                pass
        elif t == "smooth_stop_started":
            # движок принял команду плавного стопа
            self.status_var.set("ПЛАВНЫЙ СТОП — распродажа позиций...")
        elif t == "smooth_stop_done":
            # Движок завершил распродажу и остановился: фиксируем UI так же, как обычный STOP.
            try:
                self._stop(call_engine=False)
            except Exception:
                pass
            self.status_var.set("ОСТАНОВЛЕНО (Плавный стоп)")
        elif t == "top_symbols":
            syms = msg.get("symbols") or []
            if self.auto_top_var.get():
                # Список символов = источник истины
                self.symbols_list.delete(0, "end")
                for s in syms:
                    self.symbols_list.insert("end", s)
                self.status_var.set(f"РАБОТАЕТ — ТОП котировок: {len(syms)}")
            # Синхронизируем таблицу мониторинга под текущий список (чтобы строки не 'замирали')
            self._sync_monitoring_table(syms)


    def _sync_monitoring_table(self, symbols: list[str]):
        """Синхронизирует правую таблицу мониторинга под список символов БЕЗ 'обрушения' UI.

        Раньше таблица полностью очищалась и пересоздавалась — это давало визуальный "провал" (на 0.5–1с)
        раз в ~30–40 секунд, когда движок присылал top_symbols/refresh.
        Теперь: удаляем только ушедшие символы, добавляем новые и аккуратно переупорядочиваем строки.
        """
        try:
            new_syms = []
            for s in (symbols or []):
                ss = str(s or "").strip().upper()
                if ss:
                    new_syms.append(ss)
        except Exception:
            new_syms = list(symbols or [])
        if not new_syms:
            return

        # 1) BTC-USDT первым, 2) ETH-USDT вторым, 3) остальные по убыванию цены.
        try:
            new_syms = self._sort_monitor_symbols(new_syms)
        except Exception:
            pass

        if not hasattr(self, "_active_symbols") or self._active_symbols is None:
            self._active_symbols = []
        if not hasattr(self, "_iid_map") or self._iid_map is None:
            self._iid_map = {}

        old_syms = list(self._active_symbols or [])
        # Ничего не менялось → не трогаем UI
        if old_syms == new_syms:
            return

        # 1) Удаляем символы, которые ушли
        try:
            for sym in list(old_syms):
                if sym not in new_syms:
                    iid = self._iid_map.get(sym)
                    if iid:
                        try:
                            self.tree.delete(iid)
                        except Exception:
                            pass
                    self._iid_map.pop(sym, None)
        except Exception:
            pass

        # 2) Добавляем новые символы
        for sym in new_syms:
            if sym in self._iid_map:
                continue
            try:
                vals = (sym, "—", "—", "—", "ЖДАТЬ", "0.000", "—", "—", "—", "—", "—", "—", "—", "—", "0")
                iid = self.tree.insert("", "end", values=vals)
                self._iid_map[sym] = iid
            except Exception:
                pass

        # 3) Переупорядочиваем строки под новый список (без очистки)
        try:
            for idx, sym in enumerate(new_syms):
                iid = self._iid_map.get(sym)
                if iid:
                    try:
                        self.tree.move(iid, "", idx)
                    except Exception:
                        pass
        except Exception:
            pass

        self._active_symbols = list(new_syms)


    def _sort_monitor_symbols(self, syms: list[str]) -> list[str]:
        """BTC, ETH сверху; остальные — по убыванию цены (last/ask/bid)."""
        s = [str(x).upper().strip() for x in (syms or []) if str(x).strip()]
        # уникальные, сохраняя порядок
        seen = set()
        s2 = []
        for x in s:
            if x not in seen:
                seen.add(x)
                s2.append(x)
        s = s2

        head = []
        for top in ("BTC-USDT", "ETH-USDT"):
            if top in s:
                head.append(top)
                s.remove(top)

        lp = {}
        try:
            lp = (self.engine.shared_state.get('last_prices') or {}) if hasattr(self, 'engine') else {}
            if not isinstance(lp, dict):
                lp = {}
        except Exception:
            lp = {}

        def px(sym: str) -> float:
            try:
                r = lp.get(sym) or {}
                if isinstance(r, dict):
                    return float(r.get('last') or r.get('ask') or r.get('bid') or 0.0)
            except Exception:
                return 0.0
            return 0.0

        rest = sorted(s, key=lambda x: px(x), reverse=True)
        return head + rest


    def _activate_monitoring_symbol(self, sym: str):
        """Добавляет символ в правую таблицу мониторинга на лету (без STOP/START)."""
        sym = str(sym or '').strip().upper()
        if not sym:
            return
        if not hasattr(self, "_active_symbols") or self._active_symbols is None:
            self._active_symbols = []
        if sym not in self._active_symbols:
            self._active_symbols.append(sym)
        if not hasattr(self, "_iid_map") or self._iid_map is None:
            self._iid_map = {}
        if sym in self._iid_map:
            return
        # values must match cols count (14)
        try:
            vals = (sym, "—", "—", "—", "ЖДАТЬ", "0.000", "—", "—", "—", "—", "—", "—", "—", "—", "0")
            iid = self.tree.insert("", "end", values=vals)
            self._iid_map[sym] = iid
        except Exception:
            pass

    def _ingest_alert(self, severity: str, text: str):
        """Добавляет уведомление.

        severity: 'warn' | 'critical'
        warn: жёлтые, живут TTL (10 сек), исчезают если больше не повторяются.
        critical: красные, не исчезают в течение сессии.
        """
        try:
            import time as _t
            now = _t.time()
            s = (text or "").strip()
            if not s:
                return
            bucket = "critical" if str(severity).lower().startswith("crit") else "warn"
            self._alerts.setdefault(bucket, {})
            self._alerts[bucket][s] = now
            # ограничиваем разрастание критичных (иначе UI может забиться мусором)
            if bucket == "critical" and len(self._alerts[bucket]) > int(self._alert_max_critical or 6):
                # удаляем самые старые
                items = sorted(self._alerts[bucket].items(), key=lambda kv: kv[1])
                for k, _ in items[: max(0, len(items) - int(self._alert_max_critical or 6))]:
                    try:
                        del self._alerts[bucket][k]
                    except Exception:
                        pass
            # мгновенно обновляем
            try:
                self._refresh_alerts()
            except Exception:
                pass
        except Exception:
            pass

    def _refresh_alerts(self):
        """Таймер: обновляет нижний статус.

        Требование:
        - WARN: жёлтые, обновляются и пропадают если не актуальны (TTL 10s)
        - CRITICAL: красные, не пропадают
        """
        try:
            import time as _t
            now = _t.time()

            # 1) чистим warn по TTL
            try:
                ttl = float(getattr(self, "_warn_ttl_sec", 10.0) or 10.0)
            except Exception:
                ttl = 10.0
            try:
                w = self._alerts.get("warn", {}) or {}
                for k, ts in list(w.items()):
                    if (now - float(ts or 0.0)) > ttl:
                        try:
                            del w[k]
                        except Exception:
                            pass
                self._alerts["warn"] = w
            except Exception:
                pass

            # 2) выбираем, что показывать: critical > warn
            msg = ""
            color = "#94a3b8"  # нейтральный

            crit = self._alerts.get("critical", {}) or {}
            if crit:
                k, _ts = max(crit.items(), key=lambda kv: kv[1])
                msg = str(k)
                color = "#ef4444"  # красный
            else:
                w = self._alerts.get("warn", {}) or {}
                if w:
                    k, _ts = max(w.items(), key=lambda kv: kv[1])
                    msg = str(k)
                    color = "#facc15"  # жёлтый

            # 3) применяем
            try:
                self.alert_var.set((msg or "")[:220])
                self.alert_label.configure(foreground=color)
            except Exception:
                pass
        finally:
            # планируем следующий тик
            try:
                self.after(1000, self._refresh_alerts)
            except Exception:
                pass

# ---------- Trade panel (таблица сделок) ----------
    def _build_trade_panel(self, parent):
        top = ttk.Frame(parent)
        top.pack(fill="x", pady=(0,6))

        self.trade_panel_status = tk.StringVar(value="")
        ttk.Label(top, textvariable=self.trade_panel_status, style="Sub.TLabel").pack(side="left")
        ttk.Button(top, text="Выгрузить", command=self._export_trade_history).pack(side="right")
        ttk.Button(top, text="Очистить", command=self._clear_trade_history).pack(side="right", padx=(0,6))

        wrap = ttk.Frame(parent)
        wrap.pack(fill="both", expand=True)

        self.trade_canvas = tk.Canvas(wrap, bg=self._tk_bg, highlightthickness=0)
        self.trade_canvas.pack(side="left", fill="both", expand=True)
        vsb = ttk.Scrollbar(wrap, orient="vertical", command=self.trade_canvas.yview)
        vsb.pack(side="right", fill="y")
        self.trade_canvas.configure(yscrollcommand=vsb.set)

        self.trade_inner = tk.Frame(self.trade_canvas, bg=self._tk_bg)
        self.trade_inner_id = self.trade_canvas.create_window((0, 0), window=self.trade_inner, anchor="nw")

        def _on_inner_config(_evt=None):
            self.trade_canvas.configure(scrollregion=self.trade_canvas.bbox("all"))
        self.trade_inner.bind("<Configure>", _on_inner_config)

        def _on_canvas_config(evt):
            try:
                self.trade_canvas.itemconfigure(self.trade_inner_id, width=evt.width)
            except Exception:
                pass
        self.trade_canvas.bind("<Configure>", _on_canvas_config)


        # Ранее Wheel работал только при наведении на canvas/scrollbar, потому что внутри canvas
        # лежат дочерние widgets (labels/buttons), и события уходили не туда.
        # Решение: ставим ОДИН глобальный обработчик bind_all, но прокручиваем только если курсор
        # находится внутри контейнера торгов.
        try:
            self.trade_canvas.configure(yscrollincrement=20)
        except Exception:
            pass
        self._trade_wheel_acc = 0

        def _trade_scroll_units(units: int) -> str | None:
            try:
                if units == 0:
                    return "break"
                # небольшое ускорение: 2 units на один "щелчок" колеса
                self.trade_canvas.yview_scroll(int(units) * 2, "units")
                return "break"
            except Exception:
                return None

        def _trade_on_mousewheel(event):
            # Windows/mac: event.delta (обычно +/-120). На трекпадах может быть меньше.
            try:
                delta = int(getattr(event, "delta", 0) or 0)
            except Exception:
                delta = 0
            if delta == 0:
                return None

            # накапливаем, чтобы трекпады/высокая частота не давали дрожь
            try:
                self._trade_wheel_acc += delta
                acc = int(self._trade_wheel_acc)
            except Exception:
                acc = delta

            step = 0
            if abs(acc) >= 120:
                step = -(acc // 120)
                self._trade_wheel_acc = acc % 120
            else:
                # fallback: хотя бы один шаг
                step = -1 if delta > 0 else 1
                self._trade_wheel_acc = 0

            return _trade_scroll_units(step)

        def _trade_on_button4(_event):
            return _trade_scroll_units(-1)

        def _trade_on_button5(_event):
            return _trade_scroll_units(1)

        def _is_inside_trade_block(w) -> bool:
            try:
                cur = w
                while cur is not None:
                    if cur in (wrap, self.trade_canvas, self.trade_inner):
                        return True
                    cur = getattr(cur, "master", None)
            except Exception:
                pass
            return False

        def _trade_on_mousewheel_global(event):
            try:
                w = self.winfo_containing(int(event.x_root), int(event.y_root))
            except Exception:
                w = getattr(event, "widget", None)
            if not _is_inside_trade_block(w):
                return None
            return _trade_on_mousewheel(event)

        def _trade_on_button4_global(event):
            try:
                w = self.winfo_containing(int(event.x_root), int(event.y_root))
            except Exception:
                w = getattr(event, "widget", None)
            if not _is_inside_trade_block(w):
                return None
            return _trade_on_button4(event)

        def _trade_on_button5_global(event):
            try:
                w = self.winfo_containing(int(event.x_root), int(event.y_root))
            except Exception:
                w = getattr(event, "widget", None)
            if not _is_inside_trade_block(w):
                return None
            return _trade_on_button5(event)

        # bind_all — ОДИН раз; add='+' чтобы не ломать другие binds.
        try:
            self.bind_all("<MouseWheel>", _trade_on_mousewheel_global, add="+")
            self.bind_all("<Button-4>", _trade_on_button4_global, add="+")
            self.bind_all("<Button-5>", _trade_on_button5_global, add="+")
        except Exception:
            pass

        # header
        hdr = tk.Frame(self.trade_inner, bg=self._tk_bg)
        hdr.pack(fill="x", pady=(0,4))

        # Под узкую левую панель: делаем столбцы компактнее, а длинные строки переносим.
        self._trade_cols = [
            ("Котировка", 11),
            ("Владение", 13),
            ("Покупка", 17),
            ("Продажа", 17),
            ("Итоговый PnL", 12),
            ("Мгновенный PnL", 12),
            ("Пик PnL", 12),
            ("Отдали", 10),
            ("t(+) / t(max)", 11),
            ("Действие", 8),
        ]
        for i, (name, w) in enumerate(self._trade_cols):
            lbl = tk.Label(hdr, text=name, bg=self._tk_bg, fg=self._tk_muted, font=("Segoe UI", 9, "bold"), width=w, anchor="w")
            lbl.grid(row=0, column=i, sticky="w", padx=(2,6))
        for i in range(len(self._trade_cols)):
            hdr.grid_columnconfigure(i, weight=1)

        self.trade_rows_widgets = {}  # trade_id -> widget dict
        self.trade_day_widgets = {}   # date_key -> widget frame


    # ---------------- Активы (OKX balances) ----------------
    
    def _build_history_tab(self, parent):
        """UI: История сделок по монете (агрегаты за текущую сессию)."""
        wrap = ttk.Frame(parent)
        wrap.pack(fill="both", expand=True)

        cols = ("symbol","cnt","avg_hold","sum_pnl","reasons","ban")
        self.history_tree = ttk.Treeview(wrap, columns=cols, show="headings", height=18, style="Grid.Treeview")

        headers = {
            "symbol": "Монета",
            "cnt": "Количество сделок",
            "avg_hold": "Среднее время",
            "sum_pnl": "Общий профит",
            "reasons": "Причины",
            "ban": "Блок",
        }
        widths = {"symbol": 120, "cnt": 130, "avg_hold": 170, "sum_pnl": 160, "reasons": 340, "ban": 150}

        for c in cols:
            self.history_tree.heading(c, text=headers.get(c,c))
            self.history_tree.column(c, width=widths.get(c,120), anchor="w", stretch=True)

        vsb = ttk.Scrollbar(wrap, orient="vertical", command=self.history_tree.yview)
        self.history_tree.configure(yscrollcommand=vsb.set)
        self.history_tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        # refresh loop
        self.after(1000, self._refresh_history_panel)

    def _refresh_history_panel(self):
        try:
            if not getattr(self, 'engine', None):
                self.after(2000, self._refresh_history_panel)
                return
            trades = self.engine.portfolio.trade_rows()
            # берём сделки текущей сессии
            run_ts = float(getattr(self, '_run_started_ts', 0.0) or 0.0)
            closed = []
            for tr in trades:
                try:
                    if float(getattr(tr,'sell_ts',0.0) or 0.0) > 0 and (run_ts <= 0 or float(getattr(tr,'sell_ts',0.0)) >= run_ts):
                        closed.append(tr)
                except Exception:
                    continue

            by = {}
            for tr in closed:
                sym = str(getattr(tr,'symbol','') or '')
                if not sym:
                    continue
                rec = by.setdefault(sym, {"cnt":0,"holds":[],"pnls":[],"reasons":[]})
                rec["cnt"] += 1
                try:
                    rec["holds"].append(int(getattr(tr,'holding_sec',0) or 0))
                except Exception:
                    pass
                try:
                    p,_ = tr.realized_pnl()
                    rec["pnls"].append(float(p or 0.0))
                except Exception:
                    rec["pnls"].append(0.0)
                r = str(getattr(tr,'sell_reason','') or '')
                if r:
                    rec["reasons"].append(r)

            # current bans
            banlist = getattr(self.engine, 'banlist', None)

            def mmss(sec:int):
                sec = int(sec or 0)
                m = sec//60
                s = sec%60
                h = m//60
                if h>0:
                    return f"{h:02d}:{(m%60):02d}:{s:02d}"
                return f"{m:02d}:{s:02d}"

            def money(v:float):
                s = f"{v:+.2f}$"
                return s.replace('.', ',')

            # rebuild rows
            for iid in self.history_tree.get_children(''):
                self.history_tree.delete(iid)

            for sym in sorted(by.keys()):
                rec = by[sym]
                holds = rec["holds"] or [0]
                pnls = rec["pnls"] or [0.0]
                avg_hold = int(sum(holds)/max(1,len(holds)))
                hold_txt = f"{mmss(avg_hold)} ({mmss(min(holds))}/{mmss(max(holds))})"
                sum_p = float(sum(pnls))
                pnl_txt = f"{money(sum_p)} ({money(min(pnls))}/{money(max(pnls))})"
                # top reason
                top_reason = ''
                if rec["reasons"]:
                    from collections import Counter
                    top_reason = Counter(rec["reasons"]).most_common(2)
                    top_reason = ', '.join([f"{k}×{v}" for k,v in top_reason])
                else:
                    top_reason = '—'

                ban_txt = 'Нет'
                if banlist is not None:
                    try:
                        ok, until, why = banlist.is_banned(sym)
                        if ok:
                            # until -> MSK
                            import datetime
                            dt = datetime.datetime.fromtimestamp(float(until), tz=MSK_TZ)
                            ban_txt = f"Да до {dt.strftime('%H:%M')}"
                    except Exception:
                        pass

                self.history_tree.insert('', 'end', values=(sym.replace('-',''), rec['cnt'], hold_txt, pnl_txt, top_reason, ban_txt))
        except Exception:
            pass
        self.after(2000, self._refresh_history_panel)

    def _build_assets_tab(self, parent):
        """Таб для отображения текущих активов из OKX.

        Цели:
        1) Видеть реальные доступные количества (avail) из OKX.
        2) Ввести защиту базовых активов (например стартовый 1 BTC в демо).
        3) Уметь продать выделенный актив по доступному объёму, не полагаясь на локальную историю.
        """
        top = ttk.Frame(parent)
        top.pack(fill="x", pady=(0, 8))

        self.assets_status = tk.StringVar(value="Активы: ещё не загружено")
        ttk.Label(top, textvariable=self.assets_status, style="Sub.TLabel").pack(side="left")
        self.assets_hide_dust_btn = ttk.Button(top, text="Скрыть пыль", command=self._toggle_hide_dust_assets)
        self.assets_hide_dust_btn.pack(side="right")
        ttk.Button(top, text="Обновить", command=self._refresh_assets).pack(side="right", padx=(0, 8))

        # Таблица
        # показываем "пыль" (остатки, которые меньше минимального порога и не продаются),
        # чтобы было понятно, почему позиция не продаётся, но при этом BUY не блокируется.
        cols = ("ccy", "total", "avail", "usd", "sellable_usd", "dust", "source", "protected", "baseline", "sellable")
        self.assets_tree = ttk.Treeview(parent, columns=cols, show="headings", height=18, style="Grid.Treeview")
        self.assets_tree.pack(fill="both", expand=True)

        headings = {
            "ccy": "Валюта",
            "total": "Всего",
            "avail": "Доступно",
            "usd": "USD экв.",
            "sellable_usd": "USD к продаже",
            "dust": "Пыль",
            "source": "Источник",
            "protected": "Защита",
            "baseline": "База",
            "sellable": "К продаже",
        }
        widths = {
            "ccy": 80,
            "total": 120,
            "avail": 120,
            "usd": 120,
            "sellable_usd": 130,
            "dust": 70,
            "source": 110,
            "protected": 90,
            "baseline": 120,
            "sellable": 120,
        }
        for c in cols:
            self.assets_tree.heading(c, text=headings.get(c, c))
            self.assets_tree.column(c, width=widths.get(c, 100), anchor="w")

        # Кнопки действий
        actions = ttk.Frame(parent)
        actions.pack(fill="x", pady=(8, 0))

        ttk.Button(actions, text="Переключить защиту (выделенное)", command=self._toggle_protect_selected).pack(side="left", padx=(8, 0))

        ttk.Button(actions, text="Продать выделенное", command=self._sell_selected_asset).pack(side="right")
        ttk.Button(actions, text="Продать всё (кроме защищ.)", command=self._sell_all_assets).pack(side="right", padx=(0, 8))

        hint = ttk.Label(parent, text=(
            "Подсказка: защита BTC по умолчанию сохраняет 1 BTC на балансе. "
            "Если на балансе 1.00015 BTC — к продаже доступно только 0.00015. "
            "Базу можно изменить выше или отключить защиту кнопкой.\n"
            "Пыль: очень малые остатки (USD к продаже < порога). Их обычно нельзя продать на OKX (min size), "
            "но они НЕ блокируют повторную покупку по этому символу."
        ), style="Sub.TLabel")
        hint.pack(anchor="w", pady=(8, 0))

    def _refresh_assets(self):
        """Рендер активов на основе последнего кэша движка."""
        try:
            if self.engine is None:
                self.assets_status.set("Активы: движок не инициализирован")
                return
            snap = self.engine.get_balances_snapshot()
            self._assets_last_snapshot = snap
            self._render_assets_snapshot(snap)
        except Exception as e:
            self.assets_status.set(f"Активы: ошибка — {e}")

    def _assets_auto_refresh_loop(self):
        """Периодическое обновление UI вкладки "Активы".

        Важно: не дергаем OKX каждый тик — используем кэш движка (balances_cache).
        """
        try:
            # обновляем только если вкладка активов вообще существует
            if self.engine is not None:
                # Даже если движок не запущен, кэш может обновляться по нажатию "Обновить".
                # Поэтому просто перерисовываем из кэша.
                self._refresh_assets()
        except Exception:
            pass
        try:
            self.after(int(getattr(self, '_assets_refresh_ms', 700) or 700), self._assets_auto_refresh_loop)
        except Exception:
            pass

    def _toggle_hide_dust_assets(self):
        """UI: скрыть/показать строки "пыль" и всегда скрывать TOTAL."""
        try:
            self._hide_dust_assets = not bool(self._hide_dust_assets)
            if self.assets_hide_dust_btn is not None:
                self.assets_hide_dust_btn.configure(text=("Показать пыль" if self._hide_dust_assets else "Скрыть пыль"))
        except Exception:
            pass
        try:
            self._render_assets_snapshot(self._assets_last_snapshot)
        except Exception:
            pass

    def _render_assets_snapshot(self, snap):
        """UI: отрисовка активов с фильтрами (пыль/итоги)."""
        # сохранить выделение (иначе слетает при обновлении и продать невозможно)
        selected_ccy = ""
        try:
            sel = self.assets_tree.selection() or []
            if sel:
                vals = self.assets_tree.item(sel[0], "values") or []
                if vals:
                    selected_ccy = str(vals[0] or "").upper()
        except Exception:
            selected_ccy = ""

        # очистить
        try:
            for i in self.assets_tree.get_children():
                self.assets_tree.delete(i)
        except Exception:
            pass

        if not snap:
            self.assets_status.set("Активы: нет данных (нет ключей/связи с OKX)")
            return

        # сортировка: USDT наверх, далее по USD экв.
        def key(r):
            c = str(r.get('ccy') or '').upper()
            if c == 'USDT':
                return (-1e18, )
            return (-float(r.get('usd') or 0.0), )

        snap_sorted = sorted(snap, key=key)
        prot_cnt = 0
        shown = 0
        for r in snap_sorted:
            ccy = str(r.get('ccy') or '').upper()
            source = str(r.get('source') or '').upper()

            # Всегда скрываем TOTAL (вся полезная инфа есть в TRADING)
            if ccy == 'TOTAL' or source == 'TOTAL':
                continue

            dust = bool(r.get('dust') or False)
            if self._hide_dust_assets and dust:
                continue

            total = float(r.get('total') or 0.0)
            avail = float(r.get('avail') or 0.0)
            usd = float(r.get('usd') or 0.0)
            sellable_usd = float(r.get('sellable_usd') or 0.0)
            prot = bool(r.get('protected') or False)
            baseline = float(r.get('baseline') or 0.0)
            sellable = float(r.get('sellable') or 0.0)
            if prot:
                prot_cnt += 1
            self.assets_tree.insert("", "end", values=(
                ccy,
                f"{total:.8f}".rstrip('0').rstrip('.'),
                f"{avail:.8f}".rstrip('0').rstrip('.'),
                f"{usd:.2f}",
                f"{sellable_usd:.2f}",
                "ДА" if dust else "нет",
                str(r.get('source') or ''),
                "ДА" if prot else "нет",
                f"{baseline:.8f}".rstrip('0').rstrip('.'),
                f"{sellable:.8f}".rstrip('0').rstrip('.'),
            ))
            shown += 1

        # восстановить выделение
        if selected_ccy:
            try:
                for iid in self.assets_tree.get_children(""):
                    vals = self.assets_tree.item(iid, "values") or []
                    if vals and str(vals[0] or "").upper() == selected_ccy:
                        self.assets_tree.selection_set(iid)
                        self.assets_tree.focus(iid)
                        self.assets_tree.see(iid)
                        break
            except Exception:
                pass

        self.assets_status.set(f"Активы: {shown} валют • защищено: {prot_cnt}" + (" • пыль скрыта" if self._hide_dust_assets else ""))

    def _set_protect_ccy(self, ccy: str, enabled: bool):
        try:
            if self.engine is None:
                return
            self.engine.set_protect_currency(ccy=str(ccy or '').upper(), enabled=bool(enabled))
            self._save_cfg_from_ui()  # чтобы защита сохранилась
            self._refresh_assets()
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Не удалось изменить защиту: {e}")

    def _toggle_protect_selected(self):
        sel = self.assets_tree.selection() or []
        if not sel:
            messagebox.showinfo(APP_NAME, "Выдели валюту в таблице активов.")
            return
        vals = self.assets_tree.item(sel[0], 'values') or []
        if not vals:
            return
        ccy = str(vals[0] or '').upper()
        cur = str(vals[5] or '').strip().lower() == 'да'
        self._set_protect_ccy(ccy, not cur)

    def _sell_selected_asset(self):
        sel = self.assets_tree.selection() or []
        if not sel:
            messagebox.showinfo(APP_NAME, "Выдели валюту в таблице активов.")
            return
        vals = self.assets_tree.item(sel[0], 'values') or []
        if not vals:
            return
        ccy = str(vals[0] or '').upper()
        if ccy in ("USDT", "USD"):
            messagebox.showinfo(APP_NAME, "USDT продавать не нужно.")
            return
        try:
            res = self.engine.sell_currency_from_balance(ccy=ccy)
            if not (res or {}).get('ok'):
                messagebox.showerror(APP_NAME, f"SELL не выполнен: {(res or {}).get('error') or res}")
            else:
                messagebox.showinfo(APP_NAME, f"SELL отправлен: {ccy}")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"SELL не выполнен: {e}")

    def _sell_all_assets(self):
        try:
            res = self.engine.sell_all_assets_from_balance()
            if not (res or {}).get('ok'):
                messagebox.showerror(APP_NAME, f"Продажа не выполнена: {(res or {}).get('error') or res}")
            else:
                messagebox.showinfo(APP_NAME, f"Отправлено SELL ордеров: {int((res or {}).get('sent') or 0)}")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Продажа не выполнена: {e}")


    def _fmt_time_msk_hm(self, ts: float) -> str:
        if not ts:
            return "—"
        try:
            # Москва: UTC+3 (без DST)
            return time.strftime("%H:%M", time.gmtime(float(ts) + 3*3600))
        except Exception:
            return "—"

    def _fmt_qty(self, qty: float, decimals: int = 8) -> str:
        try:
            q = float(qty or 0.0)
        except Exception:
            q = 0.0
        if q == 0.0:
            return "0"
        fmt = f"{{:.{decimals}f}}"
        s = fmt.format(q)
        return s.rstrip('0').rstrip('.')

    def _fmt_time(self, ts: float) -> str:
        if not ts:
            return "—"
        try:
            return time.strftime("%H:%M:%S", time.localtime(ts))
        except Exception:
            return "—"

    def _fmt_hold(self, sec: int) -> str:
        sec = int(sec or 0)
        h = sec // 3600
        m = (sec % 3600) // 60
        s = sec % 60
        if h > 0:
            return f"{h:02d}:{m:02d}:{s:02d}"
        return f"{m:02d}:{s:02d}"

    def _upsert_trade_row(self, row_idx: int, tr, fee_rate: float):
        sym = tr.symbol
        tid = getattr(tr, 'trade_id', '') or f"{sym}-{int(getattr(tr,'buy_ts',0) or 0)}"
        tid_short = str(tid)[:6] if tid else ''

        # last price for pnl now
        last_px = 0.0
        try:
            # Для мгновенного PnL используем bid (реалистичная цена продажи).
            lp = (getattr(self.engine, 'shared_state', {}) or {}).get('last_prices') or {}
            lpo = lp.get(sym) or {}
            last_px = float(lpo.get('bid') or lpo.get('last') or 0.0)
            if last_px <= 0:
                p = self.engine.portfolio.positions.get(sym)
                if p:
                    last_px = float(getattr(p, 'last_price', 0.0) or 0.0)
        except Exception:
            last_px = 0.0

        # buy card
        base_ccy = (str(sym).split('-', 1)[0].upper() if '-' in str(sym) else str(sym).upper())
        if tr.buy_ts > 0:
            buy_fee_mode = str(getattr(tr, 'buy_fee_mode', '') or '').lower()
            fee_amt = float(getattr(tr, 'buy_fee_amt', 0.0) or 0.0)
            fee_quote = fee_amt if buy_fee_mode == 'quote' else 0.0
            spent_total = float(getattr(tr, 'buy_usd', 0.0) or 0.0) + float(fee_quote or 0.0)

            qty_net = float(getattr(tr, 'buy_qty', 0.0) or 0.0)
            px_exec = float(getattr(tr, 'buy_px', 0.0) or 0.0)

            buy_line1 = f"${spent_total:.2f} (Списано)"
            buy_line2 = f"{self._fmt_qty(qty_net)} {base_ccy} (Получено)"
            buy_line3 = f"{px_exec:.6f} (Цена)" if px_exec > 0 else ""
            buy_txt = "\n".join([x for x in [buy_line1, buy_line2, buy_line3] if x]).strip()
        else:
            buy_txt = "—"

# sell card
        if tr.sell_ts > 0:
            sell_fee_mode = str(getattr(tr, 'sell_fee_mode', '') or '').lower()
            fee_amt = float(getattr(tr, 'sell_fee_amt', 0.0) or 0.0)
            fee_quote = fee_amt if sell_fee_mode == 'quote' else 0.0
            got_total = float(getattr(tr, 'sell_usd', 0.0) or 0.0) - float(fee_quote or 0.0)
            qty_out = float(getattr(tr, 'sell_qty', 0.0) or 0.0)
            px_exec = float(getattr(tr, 'sell_px', 0.0) or 0.0)

            sell_line1 = f"{self._fmt_qty(qty_out)} {base_ccy} (Списано)"
            sell_line2 = f"${got_total:.2f} (Получено)"
            sell_line3 = f"{px_exec:.6f} (Цена)" if px_exec > 0 else ""
            sell_txt = "\n".join([x for x in [sell_line1, sell_line2, sell_line3] if x]).strip()
            prof, prof_pct = tr.realized_pnl()
            delta_txt = f"{prof:+.2f} ({prof_pct*100:+.2f}%)"
            pnl_now_txt = "—"
        else:
            sell_txt = "—"
            delta_txt = "—"
            pnl_now, pnl_now_pct = tr.est_pnl_now(last_px=last_px or tr.buy_px, fee_rate=fee_rate)
            pnl_now_txt = f"{pnl_now:+.2f} ({pnl_now_pct*100:+.2f}%)"

        try:
            peak_usd = float(getattr(tr, 'max_net_pnl_usd', 0.0) or 0.0)
            peak_pp = float(getattr(tr, 'max_net_pnl_pct', 0.0) or 0.0)
            peak_txt = f"{peak_usd:+.2f} ({peak_pp:+.2f}%)" if float(getattr(tr,'max_net_pnl_ts',0.0) or 0.0) > 0.0 else "—"
        except Exception:
            peak_usd, peak_pp, peak_txt = 0.0, 0.0, "—"

        # drop from peak
        drop_txt = "—"
        try:
            if tr.sell_ts > 0:
                du, dpp = tr.analytics_drop_from_peak_realized()
                drop_txt = f"{du:+.2f} ({dpp:+.2f}%)" if (abs(du) > 1e-9 or abs(dpp) > 1e-9) else "0.00 (0.00%)"
            else:
                # open: compare peak vs current pnl
                cur_usd = float(pnl_now or 0.0)
                cur_pp = float(pnl_now_pct or 0.0) * 100.0
                du = float(peak_usd - cur_usd)
                dpp = float(peak_pp - cur_pp)
                drop_txt = f"{du:+.2f} ({dpp:+.2f}%)" if (abs(du) > 1e-9 or abs(dpp) > 1e-9) else "0.00 (0.00%)"
        except Exception:
            drop_txt = "—"

        # timings
        def _mmss(sec: float) -> str:
            try:
                s = int(max(0, float(sec or 0.0)))
            except Exception:
                s = 0
            m = s // 60
            ss = s % 60
            if m >= 100:
                return f"{m}m"
            return f"{m:02d}:{ss:02d}"

        t_first = "—"
        t_peak = "—"
        try:
            bts = float(getattr(tr, 'buy_ts', 0.0) or 0.0)
            fa = float(getattr(tr, 'first_accept_ts', 0.0) or 0.0)
            mp = float(getattr(tr, 'max_net_pnl_ts', 0.0) or 0.0)
            if bts > 0 and fa > 0:
                t_first = _mmss(fa - bts)
            if bts > 0 and mp > 0:
                t_peak = _mmss(mp - bts)
        except Exception:
            pass
        time_txt = f"{t_first} / {t_peak}"

        buy_tm = self._fmt_time_msk_hm(getattr(tr, 'buy_ts', 0.0) or 0.0)
        sell_tm = self._fmt_time_msk_hm(getattr(tr, 'sell_ts', 0.0) or 0.0) if float(getattr(tr, 'sell_ts', 0.0) or 0.0) > 0 else ""
        hold_txt = f"{self._fmt_hold(tr.holding_sec)}\nПокупка - {buy_tm}\nПродажа - {sell_tm}"

        # create widgets once
        w = self.trade_rows_widgets.get(tid)
        if not w:
            row = tk.Frame(self.trade_inner, bg=self._tk_card)
            row.pack(fill="x", pady=2)

            lbl_sym = tk.Label(row, text=(sym.replace('-', '') + (f"\n#{tid_short}" if tid_short else '')), bg=self._tk_card, fg=self._tk_fg, font=("Segoe UI", 10, "bold"), width=self._trade_cols[0][1], anchor="w")
            lbl_hold = tk.Label(row, text=hold_txt, bg=self._tk_card, fg=self._tk_fg, font=("Segoe UI", 9), width=self._trade_cols[1][1], anchor="w", justify="left")

            # wraplength даёт автоматический перенос длинных строк и увеличивает высоту карточки позиции
            lbl_buy = tk.Label(row, text=buy_txt, bg=self._tk_card, fg=self._tk_fg, justify="left",
                               font=("Segoe UI", 9), width=self._trade_cols[2][1], anchor="nw", wraplength=160)
            lbl_sell = tk.Label(row, text=sell_txt, bg=self._tk_card, fg=self._tk_fg, justify="left",
                                font=("Segoe UI", 9), width=self._trade_cols[3][1], anchor="nw", wraplength=160)

            lbl_delta = tk.Label(row, text=delta_txt, bg=self._tk_card, fg=self._tk_fg, font=("Segoe UI", 10, "bold"), width=self._trade_cols[4][1], anchor="w")
            lbl_pnl = tk.Label(row, text=pnl_now_txt, bg=self._tk_card, fg=self._tk_fg, font=("Segoe UI", 10, "bold"), width=self._trade_cols[5][1], anchor="w")
            lbl_peak = tk.Label(row, text=peak_txt, bg=self._tk_card, fg=self._tk_muted, font=("Segoe UI", 9), width=self._trade_cols[6][1], anchor="w")
            lbl_drop = tk.Label(row, text=drop_txt, bg=self._tk_card, fg=self._tk_muted, font=("Segoe UI", 9), width=self._trade_cols[7][1], anchor="w")
            lbl_time = tk.Label(row, text=time_txt, bg=self._tk_card, fg=self._tk_muted, font=("Segoe UI", 9), width=self._trade_cols[8][1], anchor="w")
            btn_sell = ttk.Button(row, text="Продать", style="MiniDanger.TButton", command=lambda s=sym: self._quick_sell(s))

            lbl_sym.grid(row=0, column=0, sticky="w", padx=(6,6), pady=6)
            lbl_hold.grid(row=0, column=1, sticky="w", padx=(2,6), pady=6)
            lbl_buy.grid(row=0, column=2, sticky="w", padx=(2,6), pady=6)
            lbl_sell.grid(row=0, column=3, sticky="w", padx=(2,6), pady=6)
            lbl_delta.grid(row=0, column=4, sticky="w", padx=(2,6), pady=6)
            lbl_pnl.grid(row=0, column=5, sticky="w", padx=(2,6), pady=6)
            lbl_peak.grid(row=0, column=6, sticky="w", padx=(2,6), pady=6)
            lbl_drop.grid(row=0, column=7, sticky="w", padx=(2,6), pady=6)
            lbl_time.grid(row=0, column=8, sticky="w", padx=(2,6), pady=6)
            btn_sell.grid(row=0, column=9, sticky="w", padx=(2,6), pady=6)

            for i in range(len(self._trade_cols)):
                row.grid_columnconfigure(i, weight=1)

            w = {
                "frame": row,
                "sym": lbl_sym,
                "hold": lbl_hold,
                "buy": lbl_buy,
                "sell": lbl_sell,
                "delta": lbl_delta,
                "pnl": lbl_pnl,
                "peak": lbl_peak,
                "drop": lbl_drop,
                "time": lbl_time,
                "btn_sell": btn_sell,
            }
            self.trade_rows_widgets[tid] = w

        # update text
        w["hold"].configure(text=hold_txt)
        w["buy"].configure(text=buy_txt)
        w["sell"].configure(text=sell_txt)
        w["delta"].configure(text=delta_txt)
        w["pnl"].configure(text=pnl_now_txt)
        try:
            w["peak"].configure(text=peak_txt)
            w["drop"].configure(text=drop_txt)
            w["time"].configure(text=time_txt)
        except Exception:
            pass

        # кнопка быстрого SELL в строке позиции
        try:
            pos = self.engine.portfolio.position(sym)
            is_open = (tr.buy_ts > 0 and tr.sell_ts <= 0 and pos.qty > 0)
            w["btn_sell"].configure(state=("normal" if is_open else "disabled"))
            # Требование: после закрытия кнопка должна ПРОПАДАТЬ, а не быть disabled
            try:
                if is_open:
                    w["btn_sell"].grid()
                else:
                    w["btn_sell"].grid_remove()
            except Exception:
                pass
        except Exception:
            pass

        # colors for profit/pnl
        def _color(val: float) -> str:
            if val > 0:
                return "#21c55d"  # green
            if val < 0:
                return "#ef4444"  # red
            return self._tk_fg

        if tr.sell_ts > 0:
            prof, _ = tr.realized_pnl()
            w["delta"].configure(fg=_color(prof))
            w["pnl"].configure(fg=self._tk_muted)
        else:
            pnl_now, _ = tr.est_pnl_now(last_px=last_px or tr.buy_px, fee_rate=fee_rate)
            w["pnl"].configure(fg=_color(pnl_now))
            w["delta"].configure(fg=self._tk_muted)

        # peak/drop coloring: пик зелёный если >0, отдали красным если отдали много
        try:
            w["peak"].configure(fg=_color(float(getattr(tr, 'max_net_pnl_usd', 0.0) or 0.0)))
        except Exception:
            pass
        try:
            # если "отдали" >0 — значит прибыль уменьшилась → красным
            du = 0.0
            if tr.sell_ts > 0:
                du, _ = tr.analytics_drop_from_peak_realized()
            else:
                du = float(peak_usd - float(pnl_now or 0.0))
            if du > 0.0:
                w["drop"].configure(fg="#ef4444")
            else:
                w["drop"].configure(fg=self._tk_muted)
        except Exception:
            pass


    def _refresh_trade_panel(self):
        try:
            fee_rate = 0.001
            try:
                fee_rate = float((self.cfg.get("trading", {}) or {}).get("fee_rate", 0.001))
            except Exception:
                fee_rate = 0.001

            trades = self.engine.portfolio.trade_rows()

            # Статистика сделок для шапки вкладки "Торговля"
            total_cnt = len(trades)
            closed = []
            opened = []
            for tr in trades:
                try:
                    if float(getattr(tr, 'sell_ts', 0) or 0) > 0:
                        closed.append(tr)
                    else:
                        opened.append(tr)
                except Exception:
                    opened.append(tr)

            # сортировка: открытые по buy_ts desc, закрытые по sell_ts desc
            opened.sort(key=lambda t: float(getattr(t,'buy_ts',0.0) or 0.0), reverse=True)
            closed.sort(key=lambda t: float(getattr(t,'sell_ts',0.0) or 0.0), reverse=True)

            pos_cnt = 0
            neg_cnt = 0
            pos_sum = 0.0
            neg_sum = 0.0
            for tr in closed:
                try:
                    p, _ = tr.realized_pnl()
                    p = float(p or 0.0)
                except Exception:
                    p = 0.0
                if p >= 0:
                    pos_cnt += 1
                    pos_sum += p
                else:
                    neg_cnt += 1
                    neg_sum += p

            def _fmt_money(v: float) -> str:
                s = f"{v:+.2f}$"
                return s.replace('.', ',')

            head_lines = [
                f"Всего сделок: {total_cnt}",
                f"Закрытых: {len(closed)}",
                f"Открытых: {len(opened)}",
                f"Закрыто в плюс:  {pos_cnt} / {_fmt_money(pos_sum)}",
                f"Закрыто в минус: {neg_cnt} / {_fmt_money(neg_sum)}",
            ]
            self.trade_panel_status.set("\n".join(head_lines))
            try:
                if float(self._run_started_ts or 0.0) > 0:
                    prof = float(self.engine.shared_state.get('session_realized_pnl', 0.0) or 0.0)
                    self.profit_var.set(f"Прибыль: ${prof:.2f}")
            except Exception:
                pass

            # удаляем строки, которых больше нет
            active = {getattr(t, "trade_id", "") or "" for t in trades}
            for tid in list(self.trade_rows_widgets.keys()):
                if tid not in active:
                    try:
                        self.trade_rows_widgets[tid]["frame"].destroy()
                    except Exception:
                        pass
                    del self.trade_rows_widgets[tid]

            # purge day separators
            # days are keyed as YYYY-MM-DD
            closed_days = set()
            for tr in closed:
                try:
                    import datetime
                    dt = datetime.datetime.fromtimestamp(float(getattr(tr,'sell_ts',0.0) or 0.0), tz=MSK_TZ)
                    closed_days.add(dt.date().isoformat())
                except Exception:
                    pass
            for dk in list(self.trade_day_widgets.keys()):
                if dk not in closed_days:
                    try:
                        self.trade_day_widgets[dk].destroy()
                    except Exception:
                        pass
                    del self.trade_day_widgets[dk]

            # map fee rates
            fee_map = {}
            try:
                fee_map = self.engine.shared_state.get("fee_rate_by_symbol", {}) or {}
            except Exception:
                fee_map = {}

            # Build ordered list: opened first, then closed grouped by day with separators
            ordered_items = []  # widgets frames in display order

            # open trades
            for tr in opened:
                try:
                    fr = float(fee_map.get(getattr(tr, "symbol", ""), fee_rate) or fee_rate)
                except Exception:
                    fr = fee_rate
                self._upsert_trade_row(0, tr, fr)
                tid = getattr(tr, "trade_id", "") or f"{tr.symbol}-{int(getattr(tr,'buy_ts',0) or 0)}"
                ordered_items.append(self.trade_rows_widgets[tid]["frame"])

            # closed trades grouped by day (sell_ts MSK)
            import datetime
            by_day = {}
            for tr in closed:
                try:
                    dt = datetime.datetime.fromtimestamp(float(getattr(tr,'sell_ts',0.0) or 0.0), tz=MSK_TZ)
                    dk = dt.date().isoformat()
                except Exception:
                    dk = 'unknown'
                by_day.setdefault(dk, []).append(tr)

            day_keys = sorted([k for k in by_day.keys() if k!='unknown'], reverse=True)
            if 'unknown' in by_day:
                day_keys.append('unknown')

            def day_header(dk:str):
                if dk=='unknown':
                    return "------------ неизвестно -------------"
                d = datetime.date.fromisoformat(dk)
                # stats per day
                trs = by_day.get(dk, [])
                cnt = len(trs)
                pc=nc=0
                ps=ns=0.0
                for tr in trs:
                    try:
                        p,_ = tr.realized_pnl(); p=float(p or 0.0)
                    except Exception:
                        p=0.0
                    if p>=0:
                        pc+=1; ps+=p
                    else:
                        nc+=1; ns+=p
                total = ps+ns
                ds = d.strftime('%d.%m.%Y')
                return f"-------------------- {ds} | {cnt} сделок ({pc}(+) / {nc}(-)) | {_fmt_money(ps)} / {_fmt_money(ns)} | Общий: {_fmt_money(total)} --------------------"

            for dk in day_keys:
                if dk not in self.trade_day_widgets:
                    fr = tk.Frame(self.trade_inner, bg=self._tk_bg)
                    lbl = tk.Label(fr, text=day_header(dk), bg=self._tk_bg, fg=self._tk_muted, font=("Segoe UI", 9))
                    lbl.pack(side='left', padx=6, pady=2)
                    self.trade_day_widgets[dk] = fr
                else:
                    # update label text
                    try:
                        lbl = self.trade_day_widgets[dk].winfo_children()[0]
                        lbl.configure(text=day_header(dk))
                    except Exception:
                        pass
                ordered_items.append(self.trade_day_widgets[dk])

                for tr in by_day.get(dk, []):
                    try:
                        frate = float(fee_map.get(getattr(tr, "symbol", ""), fee_rate) or fee_rate)
                    except Exception:
                        frate = fee_rate
                    self._upsert_trade_row(0, tr, frate)
                    tid = getattr(tr, "trade_id", "") or f"{tr.symbol}-{int(getattr(tr,'buy_ts',0) or 0)}"
                    ordered_items.append(self.trade_rows_widgets[tid]["frame"])

            # reorder pack
            # first forget all widgets we manage
            for w in list(self.trade_rows_widgets.values()):
                try:
                    w['frame'].pack_forget()
                except Exception:
                    pass
            for fr in list(self.trade_day_widgets.values()):
                try:
                    fr.pack_forget()
                except Exception:
                    pass

            for it in ordered_items:
                if isinstance(it, tk.Frame):
                    # separator or row
                    if it in self.trade_day_widgets.values():
                        it.pack(fill='x', pady=(6,2))
                    else:
                        it.pack(fill='x', pady=2)
        except Exception:
            pass
        self.after(self._trade_panel_refresh_ms, self._refresh_trade_panel)

    def _export_trade_history(self):
        """Выгрузка таблицы сделок в TXT (отдельно от логов)."""
        try:
            trades = self.engine.portfolio.trade_rows()
            ts = time.strftime("%Y-%m-%d_%H-%M-%S")
            out = os.path.join(self.data_path, f"trade_export_{ts}.txt")
            fee_rate = 0.001
            try:
                fee_rate = float((self.cfg.get("trading", {}) or {}).get("fee_rate", 0.001))
            except Exception:
                fee_rate = 0.001

            lines = []
            for tr in trades:
                lines.append(f"=== {tr.symbol} ===")
                lines.append(f"Покупка: {tr.buy_usd:.2f} USD | qty {tr.buy_qty:.8f} | px {tr.buy_px:.8f} | fee {tr.buy_fee_usd:.6f} | {self._fmt_time(tr.buy_ts)}")
                if tr.sell_ts > 0:
                    pnl, pct = tr.realized_pnl()
                    lines.append(f"Продажа: {tr.sell_usd:.2f} USD | qty {tr.sell_qty:.8f} | px {tr.sell_px:.8f} | fee {tr.sell_fee_usd:.6f} | {self._fmt_time(tr.sell_ts)}")
                    lines.append(f"Итог: {pnl:+.4f} USD ({pct*100:+.2f}%)")
                else:
                    last_px = 0.0
                    try:
                        p = self.engine.portfolio.positions.get(tr.symbol)
                        if p:
                            last_px = float(p.last_price or 0.0)
                    except Exception:
                        last_px = 0.0
                    pnl_now, pct_now = tr.est_pnl_now(last_px=last_px or tr.buy_px, fee_rate=fee_rate)
                    lines.append(f"PnL сейчас (оценка): {pnl_now:+.4f} USD ({pct_now*100:+.2f}%)")
                lines.append("")

            with open(out, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
            messagebox.showinfo(APP_NAME, f"Выгружено: {out}")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Не удалось выгрузить: {e}")

    def _clear_trade_history(self):
        if not messagebox.askyesno(APP_NAME, "Очистить историю сделок в интерфейсе? (логи на диске останутся)"):
            return
        try:
            self.engine.portfolio.clear_trade_ledger()
            # UI очистим сразу
            for tid in list(self.trade_rows_widgets.keys()):
                try:
                    self.trade_rows_widgets[tid]["frame"].destroy()
                except Exception:
                    pass
                del self.trade_rows_widgets[tid]
        except Exception:
            pass

def main():
    # Важно для EXE: в onefile PyInstaller __file__ указывает на временную папку.
    # А пользователю нужно, чтобы data/config/logs жили рядом с EXE.
    if getattr(sys, "frozen", False):
        base_path = os.path.abspath(os.path.dirname(sys.executable))
    else:
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    os.makedirs(os.path.join(base_path, "data"), exist_ok=True)
    # Если запускаем EXE ...
    cfg_path = os.path.join(base_path, "data", "config.json")
    if not os.path.exists(cfg_path):
        try:
            with open(cfg_path, "w", encoding="utf-8") as f:
                json.dump({
                    "version": APP_TITLE,
                    "okx": {"api_key": "", "api_secret": "", "passphrase": "", "save_keys": False, "simulated_trading": False},
                    "symbols": {"list": [], "auto_top": True, "auto_top_count": 30, "auto_top_refresh_min": 60, "symbol_blacklist": ["USDC-USDT", "USDT-USDC"]},
                    "logging": {"decision_log_enabled": True, "decision_log_mode": "signals+ticks", "decision_log_tick_sec": 3.0},
                    "trading": {
                        "dry_run": False,
                        "auto_trade": True,
                        "snapshots_enabled": True,
                        "check_old_orders": False,
                        "check_old_orders_hours": 5,
                        "quote_ccy": "USDT",
                        "order_type": "market",
                        "order_size_mode": "fixed",
                        "default_order_usd": 500.0,
                        "order_size_pct": 5.0,
                        "min_cash_reserve_pct": 10.0,
                        "max_positions": 8,
                        "min_order_usd": 10.0,
                        "paper_equity_usd": 1000.0,
                        "warmup_sec": 60,
    "v3_buy_score_min": 0.75,
    "v3_sell_score_min": 0.70,
        "min_exit_hold_sec": 30.0,
        "hard_stop_loss_pct": 1.20,
        "max_daily_loss_usdt": 30.0,
        "prv_watchdog_stale_sec": 60.0,
        "prv_restart_if_off_sec": 120.0,
                        "cooldown_sec": 10.0
                    }
                }, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
    app = App(base_path)
    app.mainloop()

if __name__ == "__main__":
    main()
