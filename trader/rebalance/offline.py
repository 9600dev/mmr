"""Offline dry run: an EMPTY sleeve priced from stored daily closes.

The integration seam for "targets file -> plan" with no trader_service, no
proposal store and no IB (``python -m trader.rebalance plan --offline``): the
sleeve holds nothing, has nothing in flight and no journal rows, and every
ticker is priced at its stored ``pit_daily_bars`` close on the file's
``prices_as_of`` session - the closes the strategy sized its weights from
(D08 ``prices_as_of``). A name with no usable close is unpriceable, so the plan
refuses (D15), exactly as it would live.

Every write the ports could express is refused here, so ``--offline`` can
never create a proposal or a journal row (D15): the CLI refuses ``propose
--offline`` before this object exists, and this object refuses again if asked.
"""
from __future__ import annotations

import datetime as dt
import math
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from trader.rebalance.plan import Refusal
from trader.rebalance.ports import ExecutionEvidence, Fill, ProposalRecord

# The equity loader's price-sanity rule (trader.lowfreq_live.ledger.DuckDBBars): closes at or below
# one cent or at/above $100k are exchange test prints, not prices, and are blanked.
_MIN_CLOSE = 0.01
_MAX_CLOSE = 100_000.0


def default_bars_db() -> str:
    """``history_duckdb_path`` of the research config (D03: the DB the targets were computed from)."""
    from trader.research.lowfreq.config import research_db_paths
    return str(research_db_paths()['history'])


class OfflineEmptySleeve:
    """The ``Gateway`` ports for a dry run: empty sleeve, stored closes, no writes."""

    def __init__(self, bars_db: str, prices_as_of: dt.date):
        self.bars_db = str(Path(bars_db).expanduser())
        if not Path(self.bars_db).is_file():
            # loud, up front: the pricing loop reports a per-ticker failure as UNPRICEABLE, which would
            # make a missing database read like 69 unpriceable names
            raise FileNotFoundError(f'bars database not found: {self.bars_db}')
        self.prices_as_of = prices_as_of
        self._closes: Optional[dict[str, float]] = None
        self.closed = False

    def close(self) -> None:
        self.closed = True

    # -- prices: the stored closes of the decision session --------------------------------------
    def _load_closes(self) -> dict[str, float]:
        if self._closes is not None:
            return self._closes
        import duckdb
        con = duckdb.connect(self.bars_db, read_only=True)
        try:
            rows = con.execute('SELECT ticker, close FROM pit_daily_bars WHERE day = ?',
                               [self.prices_as_of]).fetchall()
        finally:
            con.close()
        closes: dict[str, float] = {}
        for ticker, close in rows:
            if close is None:
                continue
            value = float(close)
            if math.isfinite(value) and _MIN_CLOSE < value < _MAX_CLOSE:
                closes[str(ticker).upper()] = value
        self._closes = closes
        return closes

    def price(self, ticker: str) -> Optional[float]:
        return self._load_closes().get(str(ticker).upper())

    def n_priced(self) -> int:
        return len(self._load_closes())

    # -- the empty sleeve ------------------------------------------------------------------------
    def proposals_in_group(self, group: str) -> Sequence[ProposalRecord]:
        return ()

    def stock_positions(self) -> Mapping[str, float]:
        return {}

    def account_net_liquidation(self) -> Optional[float]:
        return None     # unreadable offline: reported as unevaluable, never guessed

    def sleeve_holdings(self, sleeve: str) -> Mapping[str, float]:
        return {}

    # -- nothing can be read from the broker or written anywhere ---------------------------------
    def execution_evidence(self, order_ids: Sequence[int]) -> ExecutionEvidence:
        raise Refusal('OFFLINE', 'broker executions cannot be read offline; run reconcile with trader_service')

    def record_fills(self, fills: Sequence[Fill]) -> int:
        raise Refusal('OFFLINE', 'the offline dry run never writes the sleeve_fills journal')

    def ensure_group(self, group: str, description: str) -> None:
        raise Refusal('OFFLINE', 'the offline dry run creates nothing; run propose with trader_service')

    def propose(self, *, symbol: str, action: str, quantity: float, execution: Any, reasoning: str,
                source: str, group: str, metadata: Mapping[str, Any]) -> int:
        raise Refusal('OFFLINE', 'the offline dry run creates nothing; run propose with trader_service')
