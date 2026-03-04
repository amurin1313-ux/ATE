from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any
from strategies.strategy_v3 import StrategyV3

class StrategyRegistry:
    def __init__(self):
        # v3: новый мозг (rule-based). Старые стратегии намеренно не регистрируем,
        # чтобы не было "артефактов" и случайного отката на прошлую логику.
        self.available = {
            "StrategyV3": StrategyV3,
        }

    def create(self, name: str):
        cls = self.available.get(name)
        if cls is None:
            raise ValueError(f"Unknown strategy: {name}")
        return cls()

class SymbolStrategyInstance:
    def __init__(self, strategy_obj, strategy_params: Dict[str, Any] | None = None):
        self.strategy = strategy_obj
        self.params = strategy_params or {}
        # StrategyV3 хранит внутреннее состояние по символу; отдельный инстанс = изолированный канал.

    def decide(self, features: Dict[str, Any], position: Dict[str, Any], portfolio_state: Dict[str, Any], cfg: Dict[str, Any] | None = None, **kwargs) -> Dict[str, Any]:
        # StrategyV3 принимает (features, position, portfolio_state)
        # В проекте используется только StrategyV3. Старые стратегии (6PRO и т.п.) удалены.
        try:
            params = cfg if isinstance(cfg, dict) else self.params
            return self.strategy.decide(features=features, position=position, portfolio_state=portfolio_state, cfg=params)
        except TypeError:
            return self.strategy.decide(features=features, position=position, portfolio_state=portfolio_state)
