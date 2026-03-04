import os, json, time, threading
from datetime import datetime, timezone, timedelta

_log_lock = threading.Lock()

MSK_TZ = timezone(timedelta(hours=3))


def now_iso():
    # MSK timestamp for easier human analysis
    return datetime.now(MSK_TZ).isoformat(timespec="milliseconds")

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def append_line(path: str, line: str) -> None:
    ensure_dir(os.path.dirname(path))
    with _log_lock:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

def log_event(data_dir: str, event: dict) -> None:
    ensure_dir(os.path.join(data_dir, "logs"))
    path = os.path.join(data_dir, "logs", f"app_{datetime.now(MSK_TZ).date().isoformat()}.log")
    line = f"[{now_iso()}] {event.get('level','INFO')} {event.get('msg','')}"
    if event.get("extra") is not None:
        try:
            line += " " + json.dumps(event["extra"], ensure_ascii=False)
        except Exception:
            line += " " + str(event["extra"])
    append_line(path, line)

def log_trade_event(data_dir: str, event: dict) -> None:
    ensure_dir(os.path.join(data_dir, "trade_history"))
    path = os.path.join(data_dir, "trade_history", f"trade_events_{datetime.now(MSK_TZ).date().isoformat()}.jsonl")
    event = dict(event)
    event.setdefault("ts", now_iso())
    append_line(path, json.dumps(event, ensure_ascii=False))
