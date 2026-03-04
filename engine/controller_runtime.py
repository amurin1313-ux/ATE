from __future__ import annotations

import os
import time
from typing import Any, Dict

from engine.auto_trader import AutoTrader


def apply_runtime_config_impl(self: Any) -> Dict[str, Any]:
    """Применить актуальные настройки из self.config в runtime без STOP/START.
    
    Важно:
    - self.config — это общий dict (UI меняет его и сохраняет на диск).
    - часть параметров (пороги/бан-политика/auto_trade) живёт в shared_state и
      должна обновляться, чтобы изменения вступали в силу сразу.
    """
    tcfg = (self.config.get('trading', {}) or {})
    scfg = (self.config.get('symbols', {}) or {})

    # Единый источник правды конфигурации в рантайме.
    # Многие узлы (SymbolChannel/MarketRegime/UI) читают self.shared_state['cfg'].
    # Без этого часть параметров может "не доходить" до стратегии.
    try:
        self.shared_state['cfg'] = self.config
    except Exception:
        pass
    
    # dust threshold: и в shared_state, и в env (используется в некоторых местах)
    try:
        dust_usd = float(tcfg.get('dust_usd_threshold', 1.0) or 1.0)
    except Exception:
        dust_usd = 1.0
    os.environ['ATE_DUST_USD'] = str(dust_usd)
    self.shared_state['dust_usd_threshold'] = float(dust_usd)
    # v3: пороги rule_score (Score), НЕ вероятность
    try:
        self.shared_state['v3_buy_score_min'] = float(tcfg.get('v3_buy_score_min', 1.0) or 1.0)
    except Exception:
        self.shared_state['v3_buy_score_min'] = 1.0
    try:
        self.shared_state['v3_sell_score_min'] = float(tcfg.get('v3_sell_score_min', 1.0) or 1.0)
    except Exception:
        self.shared_state['v3_sell_score_min'] = 1.0

    try:
        self.shared_state['min_exit_hold_sec'] = float(tcfg.get('min_exit_hold_sec', 30.0) or 30.0)
    except Exception:
        self.shared_state['min_exit_hold_sec'] = 30.0
    try:
        self.shared_state['hard_stop_loss_pct'] = float(tcfg.get('hard_stop_loss_pct', 1.20) or 1.20)
    except Exception:
        self.shared_state['hard_stop_loss_pct'] = 1.20

    try:
        self.shared_state['micro_profit_enabled'] = bool(tcfg.get('micro_profit_enabled', False))
    except Exception:
        self.shared_state['micro_profit_enabled'] = False

    try:
        self.shared_state['max_daily_loss_usdt'] = float(tcfg.get('max_daily_loss_usdt', 30.0) or 30.0)
    except Exception:
        self.shared_state['max_daily_loss_usdt'] = 30.0

    try:
        self.shared_state['prv_watchdog_stale_sec'] = float(tcfg.get('prv_watchdog_stale_sec', 60.0) or 60.0)
    except Exception:
        self.shared_state['prv_watchdog_stale_sec'] = 60.0
    try:
        self.shared_state['prv_restart_if_off_sec'] = float(tcfg.get('prv_restart_if_off_sec', 120.0) or 120.0)
    except Exception:
        self.shared_state['prv_restart_if_off_sec'] = 120.0

    try:
        self.shared_state['buy_cooldown_sec'] = float(tcfg.get('buy_cooldown_sec', 90.0) or 90.0)
    except Exception:
        self.shared_state['buy_cooldown_sec'] = 90.0
    try:
        self.shared_state['sell_cooldown_sec'] = float(tcfg.get('sell_cooldown_sec', 10.0) or 10.0)
    except Exception:
        self.shared_state['sell_cooldown_sec'] = 10.0
    # legacy
    try:
        self.shared_state['cooldown_sec'] = float(tcfg.get('cooldown_sec', 10.0) or 10.0)
    except Exception:
        self.shared_state['cooldown_sec'] = 10.0

    try:
        self.shared_state['signal_ttl_sec'] = float(tcfg.get('signal_ttl_sec', 3.0) or 3.0)
    except Exception:
        self.shared_state['signal_ttl_sec'] = 3.0
    try:
        self.shared_state['global_buy_throttle_sec'] = float(tcfg.get('global_buy_throttle_sec', 4.0) or 4.0)
    except Exception:
        self.shared_state['global_buy_throttle_sec'] = 4.0

    try:
        self.shared_state['max_spread_buy_pct'] = float(tcfg.get('max_spread_buy_pct', 0.25) or 0.25)
    except Exception:
        self.shared_state['max_spread_buy_pct'] = 0.25
    try:
        self.shared_state['max_lag_buy_sec'] = float(tcfg.get('max_lag_buy_sec', 10.0) or 10.0)
    except Exception:
        self.shared_state['max_lag_buy_sec'] = 10.0

    # confirm ticks (anti-noise)
    try:
        self.shared_state['buy_confirm_ticks'] = int(tcfg.get('buy_confirm_ticks', 4) or 4)
    except Exception:
        self.shared_state['buy_confirm_ticks'] = 4
    try:
        self.shared_state['sell_confirm_ticks'] = int(tcfg.get('sell_confirm_ticks', 3) or 3)
    except Exception:
        self.shared_state['sell_confirm_ticks'] = 3

    # loop period for metrics update
    try:
        self.shared_state['metrics_loop_sec'] = float(tcfg.get('metrics_loop_sec', 0.25) or 0.25)
    except Exception:
        self.shared_state['metrics_loop_sec'] = 0.25

    # compact prep-buy diagnostics
    try:
        self.shared_state['prep_log_every_sec'] = float(tcfg.get('prep_log_every_sec', 5.0) or 5.0)
    except Exception:
        self.shared_state['prep_log_every_sec'] = 5.0
    try:
        self.shared_state['prep_log_rects_min'] = int(tcfg.get('prep_log_rects_min', 3) or 3)
    except Exception:
        self.shared_state['prep_log_rects_min'] = 3
    try:
        warmup_sec = int(tcfg.get('warmup_sec', 12) or 12)
    except Exception:
        warmup_sec = 12
    # warmup_until влияет на новые BUY сразу
    self.shared_state['warmup_until'] = time.time() + max(0, warmup_sec)
    # бан-политика
    try:
        self.shared_state['ban_after_failures'] = int(tcfg.get('ban_after_failures', 3) or 3)
        self.shared_state['ban_ttl_min'] = int(tcfg.get('ban_ttl_min', 60) or 60)
        self.shared_state['winrate_min'] = float(tcfg.get('winrate_min', 0.40) or 0.40)
        self.shared_state['winrate_min_trades'] = int(tcfg.get('winrate_min_trades', 10) or 10)
        self.shared_state['winrate_ban_hours'] = int(tcfg.get('winrate_ban_hours', 24) or 24)
    except Exception:
        pass
    
    # blacklist
    try:
        bl = scfg.get('symbol_blacklist', []) or []
        self.shared_state['symbol_blacklist'] = [str(x).strip().upper() for x in bl if str(x).strip()]
    except Exception:
        self.shared_state['symbol_blacklist'] = []
    
    # thresholds per symbol
    try:
        per_thr = tcfg.get('per_symbol_thresholds', {}) or {}
        self.shared_state['per_symbol_thresholds'] = per_thr if isinstance(per_thr, dict) else {}
    except Exception:
        self.shared_state['per_symbol_thresholds'] = {}
    
    # auto_trade включение/выключение без перезапуска
    try:
        dry_run = bool(tcfg.get('dry_run', False))
        paper_trade = bool(tcfg.get('paper_trade', False))
        desired = bool(tcfg.get('auto_trade', False)) and (not dry_run or paper_trade)
    except Exception:
        desired = False
    
    self.shared_state['auto_trade'] = bool(desired)

    # Если чекбокс выключен — не пишем decision-файлы. Если включен — пишем (если не запрещено в logging).
    try:
        dbg_enabled = bool(tcfg.get('snapshots_enabled', False))
        lcfg = (self.config.get('logging', {}) or {})
        enabled_final = bool(lcfg.get('decision_log_enabled', True)) and dbg_enabled
        if getattr(self, 'decision_logger', None) is not None:
            self.decision_logger.enabled = bool(enabled_final)
            if self.decision_logger.enabled:
                try:
                    os.makedirs(getattr(self.decision_logger, '_dir', os.path.join(self.data_dir, 'decision_logs')), exist_ok=True)
                except Exception:
                    pass
    except Exception:
        pass
    
    # если включили во время работы — поднимаем AutoTrader
    try:
        if desired:
            if self.auto_trader is None or (hasattr(self.auto_trader, 'is_alive') and (not self.auto_trader.is_alive())):
                self.auto_trader = AutoTrader(data_dir=self.data_dir, controller=self, signal_q=self.signal_queue, shared_state=self.shared_state)
                self.auto_trader.start()
        else:
            if self.auto_trader is not None:
                try:
                    self.auto_trader.stop()
                except Exception:
                    pass
                try:
                    if hasattr(self.auto_trader, 'join'):
                        self.auto_trader.join(timeout=1.0)
                except Exception:
                    pass
                self.auto_trader = None
    except Exception:
        pass
    
    return {"ok": True, "auto_trade": bool(desired)}
    
