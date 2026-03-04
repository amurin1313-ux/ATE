import os, json, base64
from dataclasses import dataclass
from typing import Any, Dict

def _xor(data: bytes, key: bytes) -> bytes:
    if not key:
        return data
    return bytes([b ^ key[i % len(key)] for i, b in enumerate(data)])

def obfuscate(s: str, salt: str) -> str:
    raw = s.encode("utf-8")
    key = (salt or "ATE").encode("utf-8")
    x = _xor(raw, key)
    return base64.urlsafe_b64encode(x).decode("ascii")

def deobfuscate(s: str, salt: str) -> str:
    if not s:
        return ""
    key = (salt or "ATE").encode("utf-8")
    try:
        raw = base64.urlsafe_b64decode(s.encode("ascii"))
        x = _xor(raw, key)
        return x.decode("utf-8", errors="replace")
    except Exception:
        return ""

class ConfigManager:
    def __init__(self, path: str):
        self.path = path
        self.data: Dict[str, Any] = {}

    def load(self) -> Dict[str, Any]:
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                self.data = json.load(f)
        else:
            self.data = {}

        # Дополнительный стабильный слой конфигурации:
        # per_symbol_thresholds.json хранится рядом с config.json и переживает пересборку EXE.
        try:
            thr_path = os.path.join(os.path.dirname(self.path), "per_symbol_thresholds.json")
            if os.path.exists(thr_path):
                with open(thr_path, "r", encoding="utf-8") as f:
                    per = json.load(f)
                self.data.setdefault("trading", {})
                self.data["trading"]["per_symbol_thresholds"] = per
        except Exception:
            pass

        return self.data

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
