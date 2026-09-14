"""Walk-forward linear (ridge) regression forecaster for daily bars."""

from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

from trader.objects import Action
from trader.trading.strategy import Signal, Strategy


class LinearModelDaily(Strategy):
    """Ridge regression on lagged-return features, refit walk-forward.

    At every refit point the model trains ONLY on (features, next-bar
    return) pairs whose target was already observable at that bar, then
    forecasts the next bar's return until the following refit. Long-only:
    a forecast above BUY_THRESHOLD_BPS opens, below EXIT_THRESHOLD_BPS
    closes.
    """

    TRAIN_WINDOW = 252         # trailing bars in the fitting window
    REFIT_EVERY = 21           # refit cadence, in bars
    MIN_TRAIN = 120            # bars required before the first forecast
    RIDGE_LAMBDA = 10.0        # L2 penalty on standardized features
    BUY_THRESHOLD_BPS = 10.0   # forecast must exceed this to go long
    EXIT_THRESHOLD_BPS = 0.0   # forecast below this closes the position

    @staticmethod
    def _features(prices: pd.DataFrame) -> pd.DataFrame:
        """Backward-looking feature matrix; row i uses only bars [0..i]."""
        close = prices["close"].astype(float)
        high = prices["high"].astype(float)
        low = prices["low"].astype(float)
        volume = prices["volume"].astype(float)
        ret = close.pct_change()
        span = high.rolling(5).max() - low.rolling(5).min()
        return pd.DataFrame(
            {
                "ret1": ret,
                "ret5": ret.rolling(5).mean(),
                "ret10": ret.rolling(10).mean(),
                "dist_sma20": close / close.rolling(20).mean() - 1.0,
                "vol10": ret.rolling(10).std(),
                "vol_ratio": volume / volume.rolling(20).mean() - 1.0,
                "stoch5": (close - low.rolling(5).min()) / span.replace(0.0, np.nan) - 0.5,
            },
            index=prices.index,
        )

    def _fit(self, x: np.ndarray, y: np.ndarray, i: int) -> Optional[Tuple[np.ndarray, np.ndarray, np.ndarray]]:
        """Fit ridge on rows [i-TRAIN_WINDOW, i-1] — targets observable by bar i."""
        lo = max(0, i - int(self.TRAIN_WINDOW))
        xs, ys = x[lo:i], y[lo:i]
        mask = np.isfinite(ys) & np.all(np.isfinite(xs), axis=1)
        if int(mask.sum()) < max(30, int(self.MIN_TRAIN) // 2):
            return None
        xs, ys = xs[mask], ys[mask]
        mu = xs.mean(axis=0)
        sigma = xs.std(axis=0)
        sigma = np.where(sigma < 1e-12, 1.0, sigma)
        z = (xs - mu) / sigma
        k = z.shape[1]
        try:
            coef = np.linalg.solve(z.T @ z + float(self.RIDGE_LAMBDA) * np.eye(k), z.T @ (ys - ys.mean()))
        except np.linalg.LinAlgError:
            return None
        return np.concatenate(([ys.mean()], coef)), mu, sigma

    def precompute(self, prices: pd.DataFrame) -> Dict[str, Any]:
        n = len(prices)
        close = prices["close"].astype(float).to_numpy()
        x = self._features(prices).to_numpy(dtype=float)
        y = np.full(n, np.nan)
        y[:-1] = close[1:] / close[:-1] - 1.0  # target only ever read for past rows

        pred = np.full(n, np.nan)
        beta = mu = sigma = None
        for i in range(int(self.MIN_TRAIN), n):
            if beta is None or (i - int(self.MIN_TRAIN)) % int(self.REFIT_EVERY) == 0:
                fit = self._fit(x, y, i)
                if fit is not None:
                    beta, mu, sigma = fit
            if beta is None or not np.all(np.isfinite(x[i])):
                continue
            pred[i] = float(beta[0] + ((x[i] - mu) / sigma) @ beta[1:])
        return {"pred": pred}

    def on_bar(self, prices: pd.DataFrame, state: Dict[str, Any], index: int) -> Optional[Signal]:
        if not state:
            return None
        pred = state["pred"]
        if index >= len(pred) or not np.isfinite(pred[index]):
            return None
        bps = float(pred[index]) * 10_000.0
        if bps > float(self.BUY_THRESHOLD_BPS):
            return Signal(
                source_name=self.name,
                action=Action.BUY,
                probability=float(min(0.9, 0.5 + abs(bps) / 200.0)),
                risk=0.4,
            )
        if bps < float(self.EXIT_THRESHOLD_BPS):
            return Signal(source_name=self.name, action=Action.SELL, probability=0.6, risk=0.4)
        return None

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        """Live path — recompute over the accumulated window (daily bars, cheap)."""
        if len(prices) < int(self.MIN_TRAIN) + 2:
            return None
        return self.on_bar(prices, self.precompute(prices), len(prices) - 1)
