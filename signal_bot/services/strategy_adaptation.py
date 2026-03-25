"""
Strategy Adaptation Module — tracking only, no penalties.

All strategies always run at ×1.0 confidence.
Threshold (55) is the only filter — no multipliers, no status penalties.

Winrate data is still tracked in DB for informational purposes.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

ALL_STRATEGIES = [
    "ema_bounce",
    "level_breakout",
    "level_bounce",
]


def get_confidence_multiplier(strategy_name: str) -> float:
    """Always returns 1.0 — no adaptation penalties applied."""
    return 1.0


def is_strategy_enabled(strategy_name: str) -> bool:
    """All strategies are always enabled."""
    return True


async def update_strategy_statuses() -> None:
    """No-op — adaptation multipliers removed."""
    pass


async def initialize() -> None:
    """No-op — no statuses to initialize."""
    logger.info("strategy_adaptation: all strategies active at ×1.0 — no multipliers")
