"""PyramidProbe — a deterministic surface-test strategy for the pyramiding add path.

NOT AN EDGE. This exists to exercise, against the live (paper) broker, the one
auto-executor path that has never fired in production: a **bounded pyramiding
add** — the second BUY while holding, which must cancel the protective stop and
re-place it covering the whole stack, top out at ``pyramid_max_adds`` adds, and
close the ENTIRE stack on SELL.

Behaviour, counted in dispatched bars (1-min): emit BUY with a fixed 5-share
quantity on every one of the first ``BUY_BARS`` bars — the executor takes the
first as the entry and (cooldown permitting, 300s between executions) later
ones as adds until the cap — then emit exactly one SELL, which must close the
full stack. Everything after that is silence.

Deploy paper-only, run once, undeploy. If you find this armed on a real
account, disarm it: it buys on a timer, not a thesis.
"""

from trader.objects import Action
from trader.trading.strategy import Signal, Strategy


class PyramidProbe(Strategy):
    BUY_BARS = 14      # BUY on dispatched bars 1..N (cooldown thins these to ~3 executions)
    SELL_BAR = 16      # then one SELL — must close the whole stack
    LOT = 5.0          # fixed shares per signal; explicit so sizing/FX stay out of the test

    def __init__(self):
        super().__init__()
        self._bars_seen = 0
        self._sold = False

    def on_prices(self, prices):
        self._bars_seen += 1
        if self._bars_seen <= self.BUY_BARS:
            return Signal(source_name=self.name, action=Action.BUY,
                          probability=0.6, risk=0.4, quantity=self.LOT)
        if not self._sold and self._bars_seen >= self.SELL_BAR:
            self._sold = True
            return Signal(source_name=self.name, action=Action.SELL,
                          probability=0.6, risk=0.4)
        return None
