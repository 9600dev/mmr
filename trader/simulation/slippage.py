from abc import ABC, abstractmethod
from trader.objects import Action

import numpy as np
import pandas as pd


class SlippageModel(ABC):
    @abstractmethod
    def calculate(self, price: float, quantity: float, action: Action, bar: pd.Series) -> float:
        """Return the fill price after slippage."""
        ...


class ZeroSlippage(SlippageModel):
    def calculate(self, price: float, quantity: float, action: Action, bar: pd.Series) -> float:
        return price


class FixedBPS(SlippageModel):
    def __init__(self, bps: float = 1.0):
        self.bps = bps

    def calculate(self, price: float, quantity: float, action: Action, bar: pd.Series) -> float:
        mult = 1 + (self.bps / 10_000)
        return price * mult if action == Action.BUY else price / mult


class SquareRootImpact(SlippageModel):
    """Almgren-Chriss square-root market impact model.

    slippage = k * sigma * sqrt(Q / ADV)

    k: impact coefficient (default 0.1)
    sigma: estimated from bar's high-low range
    ADV: bar volume as proxy for average daily volume
    """

    def __init__(self, k: float = 0.1):
        self.k = k

    def calculate(self, price: float, quantity: float, action: Action, bar: pd.Series) -> float:
        volume = bar.get('volume', 0)
        high = bar.get('high', price)
        low = bar.get('low', price)
        if volume <= 0 or high == low:
            return price
        sigma = (high - low) / price
        impact = self.k * sigma * np.sqrt(quantity / volume)
        return price * (1 + impact) if action == Action.BUY else price * (1 - impact)


class VolatilitySlippage(SlippageModel):
    """Slippage proportional to bar volatility (high-low range).

    slippage = k * (high - low) / price
    """

    def __init__(self, k: float = 0.5):
        self.k = k

    def calculate(self, price: float, quantity: float, action: Action, bar: pd.Series) -> float:
        high = bar.get('high', price)
        low = bar.get('low', price)
        if high == low:
            return price
        vol_fraction = self.k * (high - low) / price
        return price * (1 + vol_fraction) if action == Action.BUY else price * (1 - vol_fraction)


SLIPPAGE_MODELS = {
    'zero': ZeroSlippage,
    'fixed': FixedBPS,
    'sqrt': SquareRootImpact,
    'volatility': VolatilitySlippage,
}


def get_slippage_model(name: str, **kwargs) -> SlippageModel:
    cls = SLIPPAGE_MODELS.get(name)
    if cls is None:
        raise ValueError(f'Unknown slippage model: {name}. Choose from: {list(SLIPPAGE_MODELS.keys())}')
    return cls(**kwargs)
