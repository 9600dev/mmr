"""Ensemble Strategy: weighted voting across RSI, MACD, and Bollinger Bands.

Combines three independent signal sources with weighted voting:
  - RSI (14-period): oversold/overbought
  - MACD (12/26/9): momentum crossover
  - Bollinger Bands (20, 2): mean reversion

Only triggers a trade when 2+ signals agree (majority vote).
Signal probability is the weighted average of agreeing signals.
"""

from trader.trading.strategy import Signal, Strategy
from trader.objects import Action
from typing import Optional

import numpy as np
import pandas as pd


def _compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


class Ensemble(Strategy):
    def __init__(self):
        super().__init__()

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        if len(prices) < 30:
            return None

        close = prices['close']

        # --- RSI Signal ---
        # Use zone-based RSI: below 40 = bullish zone, above 60 = bearish zone
        # (not just crossover, which is too rare on daily bars)
        rsi = _compute_rsi(close)
        rsi_signal = 0  # -1=sell, 0=neutral, 1=buy
        rsi_weight = 0.3
        if len(rsi.dropna()) >= 2:
            curr_rsi = rsi.iloc[-1]
            if not np.isnan(curr_rsi):
                if curr_rsi < 40:
                    rsi_signal = 1
                elif curr_rsi > 60:
                    rsi_signal = -1

        # --- MACD Signal ---
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        hist = macd_line - signal_line

        macd_signal = 0
        macd_weight = 0.4
        if len(hist.dropna()) >= 2:
            curr_hist = hist.iloc[-1]
            prev_hist = hist.iloc[-2]
            if not np.isnan(curr_hist) and not np.isnan(prev_hist):
                if curr_hist > 0 and prev_hist <= 0:
                    macd_signal = 1
                elif curr_hist < 0 and prev_hist >= 0:
                    macd_signal = -1

        # --- Bollinger Bands Signal ---
        sma20 = close.rolling(20).mean()
        std20 = close.rolling(20).std()
        upper = sma20 + 2 * std20
        lower = sma20 - 2 * std20

        bb_signal = 0
        bb_weight = 0.3
        last_close = close.iloc[-1]
        if not np.isnan(lower.iloc[-1]) and not np.isnan(upper.iloc[-1]):
            # Zone-based: below lower band = bullish, above upper = bearish
            if last_close < lower.iloc[-1]:
                bb_signal = 1
            elif last_close > upper.iloc[-1]:
                bb_signal = -1

        # --- Voting ---
        buy_votes = sum(1 for s in [rsi_signal, macd_signal, bb_signal] if s == 1)
        sell_votes = sum(1 for s in [rsi_signal, macd_signal, bb_signal] if s == -1)

        # Need 2+ signals to agree (majority)
        if buy_votes >= 2:
            # Weighted probability from agreeing signals
            weights = []
            if rsi_signal == 1:
                weights.append(rsi_weight)
            if macd_signal == 1:
                weights.append(macd_weight)
            if bb_signal == 1:
                weights.append(bb_weight)
            prob = min(0.85, 0.5 + sum(weights) * 0.3)
            return Signal(
                source_name=self.name,
                action=Action.BUY,
                probability=prob,
                risk=1.0 - prob,
            )

        if sell_votes >= 2:
            weights = []
            if rsi_signal == -1:
                weights.append(rsi_weight)
            if macd_signal == -1:
                weights.append(macd_weight)
            if bb_signal == -1:
                weights.append(bb_weight)
            prob = min(0.85, 0.5 + sum(weights) * 0.3)
            return Signal(
                source_name=self.name,
                action=Action.SELL,
                probability=prob,
                risk=1.0 - prob,
            )

        return None
