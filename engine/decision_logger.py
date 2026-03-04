"""Decision Log (JSONL) для последующего replay/моделирования.

Цель:
- зафиксировать "что видел движок" и "какое решение принял".
- дать возможность воспроизводимо сравнивать версии стратегии и движка.

Требование пользователя: никаких системных/скрытых папок.
Поэтому пишем строго внутрь папки программы: ./data/decision_logs/

Формат: JSONL (одна запись = один JSON на строку).
"""

from __future__ import annotations

import json
import os
import threading
import time
import datetime
import gzip
import shutil
from typing import Any, Dict, Optional

from engine.version import APP_TITLE, APP_VERSION
from engine.logging_utils import MSK_TZ


def _msk_date() -> str:
    # Единый стандарт времени по всей системе: Москва (MSK)
    return datetime.datetime.now(MSK_TZ).date().isoformat()


class DecisionLogger:
    def __init__(
        self,
        *,
        data_dir: str,
        enabled: bool = True,
        mode: str = "signals",
        tick_every_sec: float = 15.0,
        max_bytes_per_file: int = 50 * 1024 * 1024,
        max_keep_days: int = 10,
        max_total_mb: int = 1500,
    ):
        self.data_dir = data_dir
        self.enabled = bool(enabled)
        self.mode = str(mode or "signals").strip()
        self.tick_every_sec = float(tick_every_sec or 15.0)
        self.max_bytes_per_file = int(max_bytes_per_file)
        self.max_keep_days = int(max_keep_days or 10)
        self.max_total_mb = int(max_total_mb or 1500)

        self._lock = threading.Lock()
        self._last_tick_ts_by_symbol: Dict[str, float] = {}
        # периодически делаем fsync, чтобы не оставлять "нулевых хвостов".
        self._last_fsync_ts: float = 0.0
        self._fsync_every_sec: float = 2.0

        # Важно: если логирование выключено (например, чекбокс "Снапшоты BUY/SELL (отладка)" OFF),
        # мы не должны создавать/писать decision-файлы вообще.
        self._dir = os.path.join(self.data_dir, "decision_logs")
        if self.enabled:
            os.makedirs(self._dir, exist_ok=True)

        # лёгкая уборка при старте, чтобы папка decision_logs не разрасталась неделями
        try:
            self._cleanup_old_files()
        except Exception:
            pass

    def _cleanup_old_files(self) -> None:
        """Чистка старых .gz/.jsonl решений.

        Требование: всё хранится внутри папки программы, но при 5–6 днях непрерывной работы
        файлы могут вырасти очень сильно. Поэтому держим:
        - max_keep_days дней;
        - и дополнительно ограничение общего объёма.
        """
        if not self.enabled:
            return
        try:
            root = self._dir
            if not os.path.isdir(root):
                return
            files = []
            for fn in os.listdir(root):
                if not (fn.endswith('.jsonl') or fn.endswith('.gz')):
                    continue
                p = os.path.join(root, fn)
                try:
                    st = os.stat(p)
                    files.append((p, st.st_mtime, st.st_size))
                except Exception:
                    continue
            if not files:
                return

            now = time.time()
            keep_days = max(1, int(self.max_keep_days or 10))
            cutoff = now - keep_days * 86400

            # 1) удалить по давности
            for p, mt, sz in list(files):
                try:
                    if mt < cutoff:
                        os.remove(p)
                except Exception:
                    pass

            # 2) ограничить общий размер
            files2 = []
            for fn in os.listdir(root):
                if not (fn.endswith('.jsonl') or fn.endswith('.gz')):
                    continue
                p = os.path.join(root, fn)
                try:
                    st = os.stat(p)
                    files2.append((p, st.st_mtime, st.st_size))
                except Exception:
                    continue
            if not files2:
                return
            total = sum(int(x[2] or 0) for x in files2)
            max_total = int(self.max_total_mb or 1500) * 1024 * 1024
            if max_total <= 0:
                return
            if total <= max_total:
                return
            # сортируем по старости и удаляем пока не влезем
            files2.sort(key=lambda x: x[1])
            for p, mt, sz in files2:
                if total <= max_total:
                    break
                try:
                    os.remove(p)
                    total -= int(sz or 0)
                except Exception:
                    pass
        except Exception:
            return

    def _path_for_today(self) -> str:
        return os.path.join(self._dir, f"decisions_{_msk_date()}.jsonl")

    def finalize_session(self, *, reason: str = "stop") -> None:
        """Финализирует текущий файл решений и упаковывает его в .gz.

        Требование пользователя: если был STOP или плавный STOP, последний JSONL
        тоже должен быть упакован (в обычной работе он мог не дойти до ротации по размеру).
        """
        if not self.enabled:
            return
        try:
            path = self._path_for_today()
            if not os.path.exists(path):
                return
            if os.path.getsize(path) <= 0:
                return
            ts = datetime.datetime.now(MSK_TZ).strftime("%H%M%S")
            # переименуем текущий файл, чтобы новый запуск в тот же день начал с чистого файла
            rotated = path.replace(".jsonl", f"_{reason}_{ts}.jsonl")
            try:
                os.replace(path, rotated)
            except Exception:
                return

            gz_path = rotated + ".gz"
            try:
                with open(rotated, "rb") as f_in:
                    with gzip.open(gz_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out, length=1024 * 1024)
                try:
                    os.remove(rotated)
                except Exception:
                    pass
            except Exception:
                # если упаковка не удалась — вернём файл обратно, чтобы не потерять данные
                try:
                    if not os.path.exists(path) and os.path.exists(rotated):
                        os.replace(rotated, path)
                except Exception:
                    pass
            # уборка после финализации
            try:
                self._cleanup_old_files()
            except Exception:
                pass
        except Exception:
            return

    def _rotate_if_needed(self, path: str) -> None:
        try:
            if not os.path.exists(path):
                return
            if os.path.getsize(path) < self.max_bytes_per_file:
                return
            # Ротация: decisions_YYYY-MM-DD.jsonl -> decisions_YYYY-MM-DD_HHMMSS.jsonl
            ts = datetime.datetime.now(MSK_TZ).strftime("%H%M%S")
            rotated = path.replace(".jsonl", f"_{ts}.jsonl")
            try:
                os.replace(path, rotated)
            except Exception:
                return

            # compress rotated file in background (do not block trading)
            def _compress(src_path: str) -> None:
                try:
                    gz_path = src_path + ".gz"
                    with open(src_path, "rb") as f_in:
                        with gzip.open(gz_path, "wb") as f_out:
                            shutil.copyfileobj(f_in, f_out, length=1024 * 1024)
                    try:
                        os.remove(src_path)
                    except Exception:
                        pass
                except Exception:
                    # ignore
                    return

            try:
                import threading
                import shutil
                threading.Thread(target=_compress, args=(rotated,), daemon=True).start()
            except Exception:
                pass
            # уборка после ротации (не блокирует торговлю)
            try:
                self._cleanup_old_files()
            except Exception:
                pass
        except Exception:
            pass

    def _write_line(self, payload: Dict[str, Any]) -> None:
        if not self.enabled:
            return
        path = self._path_for_today()
        with self._lock:
            self._rotate_if_needed(path)
            try:
                # line-buffered + flush; fsync раз в несколько секунд
                with open(path, "a", encoding="utf-8", buffering=1) as f:
                    f.write(json.dumps(payload, ensure_ascii=False) + "\n")
                    try:
                        f.flush()
                    except Exception:
                        pass
                    try:
                        now = time.time()
                        if (now - float(self._last_fsync_ts or 0.0)) >= float(self._fsync_every_sec or 2.0):
                            os.fsync(f.fileno())
                            self._last_fsync_ts = now
                    except Exception:
                        pass
            except Exception:
                # Никогда не ломаем торговлю из-за логов.
                return

    def _should_log_tick(self, symbol: str, now_ts: float) -> bool:
        last = float(self._last_tick_ts_by_symbol.get(symbol, 0.0) or 0.0)
        if (now_ts - last) >= self.tick_every_sec:
            self._last_tick_ts_by_symbol[symbol] = now_ts
            return True
        return False

    def log_tick(
        self,
        *,
        symbol: str,
        features: Dict[str, Any],
        position: Dict[str, Any],
        portfolio_state: Dict[str, Any],
        decision_raw: Dict[str, Any],
        decision_ui: Dict[str, Any],
        thresholds: Dict[str, Any],
        metrics: Optional[Dict[str, Any]] = None,
        is_signal: bool = False,
    ) -> None:
        """Логирует тик/сигнал.

        - is_signal=True: запись всегда (если включён режим signals)
        - иначе: по режиму ticks и с троттлингом
        """

        if not self.enabled:
            return

        mode = self.mode
        now_ts = time.time()

        if is_signal:
            if "signals" not in mode:
                return
        else:
            if "ticks" not in mode:
                return
            # троттлинг тиков по символу
            if not self._should_log_tick(symbol, now_ts):
                return

        payload: Dict[str, Any] = {
            "type": "signal" if is_signal else "tick",
            "ts": now_ts,
            "app": APP_TITLE,
            "build": APP_TITLE,
            "app_version": str(APP_VERSION),
            "symbol": symbol,
            "features": features,
            "position": position,
            "portfolio": portfolio_state,
            "decision": {
                "raw": decision_raw,
                "ui": decision_ui,
            },
            "thresholds": thresholds,
        }
        if metrics is not None:
            payload["metrics"] = metrics

        self._write_line(payload)

    def log_event(self, name: str, extra: Optional[Dict[str, Any]] = None) -> None:
        if not self.enabled:
            return
        payload = {
            "type": "event",
            "ts": time.time(),
            "app": APP_TITLE,
            "build": APP_TITLE,
            "app_version": str(APP_VERSION),
            "name": str(name),
            "extra": extra or {},
        }
        self._write_line(payload)
