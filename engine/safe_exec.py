from __future__ import annotations

import time
import traceback
from typing import Any, Callable, Optional

from engine.logging_utils import log_event


def _get_debounce_dict(controller: Any) -> dict:
    d = getattr(controller, '_warn_debounce', None)
    if not isinstance(d, dict):
        d = {}
        setattr(controller, '_warn_debounce', d)
    return d


def ui_warn_once(controller: Any, key: str, msg: str, ttl_sec: float = 10.0, extra: Optional[dict] = None) -> None:
    try:
        d = _get_debounce_dict(controller)
        now = time.time()
        last = float(d.get(key) or 0.0)
        if now - last < ttl_sec:
            return
        d[key] = now
        if getattr(controller, 'ui_queue', None) is not None:
            try:
                controller.ui_queue.put({'type': 'warn', 'warn': msg, 'extra': extra or {}})
            except Exception:
                pass
    except Exception:
        # Never crash on notification
        pass


def safe_call(
    controller: Any,
    fn: Callable[[], Any],
    *,
    default: Any = None,
    log_msg: str = 'exception',
    ui_msg: Optional[str] = None,
    debounce_key: Optional[str] = None,
    debounce_ttl_sec: float = 10.0,
    extra: Optional[dict] = None,
    level: str = 'ERROR',
) -> Any:
    """Call fn(); on exception logs and optionally sends a UI warning (debounced)."""
    try:
        return fn()
    except Exception as e:
        try:
            tb = traceback.format_exc(limit=12)
        except Exception:
            tb = ''
        try:
            log_event(getattr(controller, 'data_dir', ''), {
                'level': level,
                'msg': log_msg,
                'extra': {**(extra or {}), 'error': str(e), 'trace': tb}
            })
        except Exception:
            pass
        if ui_msg:
            key = debounce_key or f'{log_msg}:{type(e).__name__}'
            ui_warn_once(controller, key, ui_msg, ttl_sec=debounce_ttl_sec, extra=extra)
        return default
