from ib_async import TradeLogEntry
from ib_async.contract import Contract
from ib_async.objects import PnLSingle, PortfolioItem, Position
from ib_async.order import Order, Trade
from ib_async.ticker import Ticker
from reactivex.abc import DisposableBase
from reactivex.disposable import Disposable
from reactivex.observer import Observer
from trader.common.logging_helper import setup_logging
from trader.common.reactivex import SuccessFail, SuccessFailEnum
from trader.data.data_access import PortfolioSummary, SecurityDefinition
from trader.data.universe import Universe
from trader.messaging.clientserver import RPCHandler, rpcmethod
from trader.objects import TickList, TradeLogSimple
from trader.trading.strategy import StrategyConfig, StrategyState
from typing import List, Optional, Tuple, Union

import asyncio
import time
import trader.trading.trading_runtime as runtime


logging = setup_logging(module_name='trader_service_api')


class TraderServiceApi(RPCHandler):
    def __init__(self, trader):
        self.trader: runtime.Trader = trader

    # json can't encode Dict's with keys that aren't primitive and hashable
    # so we often have to convert to weird containers like List[Tuple]
    @rpcmethod
    def get_positions(self) -> list[Position]:
        return self.trader.get_positions()

    @rpcmethod
    def get_portfolio(self) -> list[PortfolioItem]:
        return self.trader.client.ib.portfolio()

    @rpcmethod
    async def get_portfolio_summary(self) -> list[PortfolioSummary]:
        return await self.trader.get_portfolio_summary()

    @rpcmethod
    def get_universes(self) -> dict[str, int]:
        return self.trader.universe_accessor.list_universes_count()

    @rpcmethod
    def get_trades(self) -> dict[int, list[Trade]]:
        return self.trader.book.get_trades()

    @rpcmethod
    def get_trade_log(self) -> list[TradeLogSimple]:
        return self.trader.book.get_trade_log()

    @rpcmethod
    def get_orders(self) -> dict[int, list[Order]]:
        return self.trader.book.get_orders()

    @rpcmethod
    def get_pnl(self) -> list[PnLSingle]:
        return self.trader.get_pnl()

    @rpcmethod
    async def place_order_simple(
        self, contract: Contract,
        action: str,
        equity_amount: Optional[float],
        quantity: Optional[float],
        limit_price: Optional[float],
        market_order: bool = False,
        stop_loss_percentage: float = 0.0,
        algo_name: str = 'global',
        debug: bool = False,
        skip_risk_gate: bool = False,
    ) -> SuccessFail[Trade]:
        # Proposal-approval gate: when require_proposal_approval is on in
        # trader.yaml, reject direct buy/sell unless the caller explicitly
        # flagged this as a liquidation (skip_risk_gate=True). All actionable
        # *new* trades have to come in via place_expressive_order, which the
        # approve() path uses after a proposal has been reviewed. Defensive
        # against LLM loops / scripts firing direct orders that weren't in
        # a reviewed plan.
        if getattr(self.trader, 'require_proposal_approval', False) and not skip_risk_gate:
            return SuccessFail.fail(
                error=(
                    'Direct order rejected: require_proposal_approval is true. '
                    'New trades must go through propose → approve (see `mmr propose` '
                    'and `mmr approve`). Set skip_risk_gate=True only for '
                    'liquidation paths (close-all, resize-positions).'
                ),
            )
        # todo: we'll have to make the cli async so we can subscribe to the trade
        # changes as orders get hit etc
        logging.warn('place_order_simple() is not complete, your mileage may vary')
        from trader.trading.trading_runtime import Action
        # Strict: any string not literally BUY/SELL must be refused, not silently
        # coerced. The old `'BUY' in action else SELL` turned a typo like 'BYU'
        # (or lowercase 'buy') into a live SELL.
        _a = str(action).strip().upper()
        if _a == 'BUY':
            act = Action.BUY
        elif _a == 'SELL':
            act = Action.SELL
        else:
            return SuccessFail.fail(
                error=f'invalid action {action!r}: expected "BUY" or "SELL"')

        task = asyncio.Event()
        disposable: DisposableBase = Disposable()
        result: Optional[SuccessFail] = None

        def on_next(trade: Trade):
            nonlocal result
            result = SuccessFail.success(obj=trade)
            task.set()

        def on_error(ex):
            nonlocal result
            result = SuccessFail.fail(exception=ex)
            task.set()

        def on_completed():
            task.set()

        observer = Observer(on_next=on_next, on_completed=on_completed, on_error=on_error)

        observable = await self.trader.place_order_simple(
            contract=contract,
            action=act,
            equity_amount=equity_amount,
            quantity=quantity,
            limit_price=limit_price,
            market_order=market_order,
            stop_loss_percentage=stop_loss_percentage,
            algo_name=algo_name,
            debug=debug,
            skip_risk_gate=skip_risk_gate,
        )
        observable.subscribe(observer)

        try:
            await asyncio.wait_for(task.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            if result is None:
                result = SuccessFail.fail(error='order placement timed out waiting for confirmation')
        disposable.dispose()
        return result if result else SuccessFail.fail()

    @rpcmethod
    def cancel_order(self, order_id: int) -> SuccessFail[Trade]:
        order = self.trader.cancel_order(order_id)
        if order:
            return SuccessFail.success(order)
        else:
            return SuccessFail.fail()

    @rpcmethod
    def cancel_all(self) -> SuccessFail[list[int]]:
        return self.trader.cancel_all()

    @rpcmethod
    async def get_snapshot(self, contract: Contract, delayed: bool) -> Ticker:
        return await self.trader.client.get_snapshot(contract=contract, delayed=delayed)

    @rpcmethod
    async def get_market_depth(self, contract: Contract, num_rows: int = 5, is_smart_depth: bool = False) -> dict:
        return await self.trader.get_market_depth(contract, num_rows, is_smart_depth)

    @rpcmethod
    def publish_contract(self, contract: Contract, delayed: bool) -> bool:
        self.trader.publish_contract(contract, delayed)
        return True

    @rpcmethod
    def get_published_contracts(self) -> list[int]:
        return list(self.trader.zmq_pubsub_contracts.keys())

    @rpcmethod
    def get_unique_client_id(self) -> int:
        return self.trader.get_unique_client_id()

    @rpcmethod
    async def resolve_contract(self, contract: Contract) -> list[SecurityDefinition]:
        return await self.trader.resolve_contract(contract)

    @rpcmethod
    async def resolve_symbol(
        self,
        symbol: Union[str, int],
        exchange: str = '',
        universe: str = '',
        sec_type: str = ''
    ) -> list[SecurityDefinition]:
        return await self.trader.resolve_symbol(symbol, exchange, universe, sec_type)

    @rpcmethod
    async def resolve_universe(
        self,
        symbol: Union[str, int],
        exchange: str,
        universe: str,
        sec_type: str,
    ) -> list[tuple[str, SecurityDefinition]]:
        return await self.trader.resolve_universe(symbol, exchange, universe, sec_type)

    @rpcmethod
    def release_client_id(self, client_id: int) -> bool:
        self.trader.release_client_id(client_id)
        return True

    @rpcmethod
    async def get_shortable_shares(self, contract: Contract) -> float:
        return await self.trader.get_shortable_shares(contract)

    @rpcmethod
    async def get_strategies(self) -> SuccessFail[list[StrategyConfig]]:
        return await self.trader.get_strategies()

    @rpcmethod
    async def enable_strategy(self, name: str) -> SuccessFail[StrategyState]:
        return await self.trader.enable_strategy(name)

    @rpcmethod
    async def disable_strategy(self, name: str) -> SuccessFail[StrategyState]:
        return await self.trader.disable_strategy(name)

    @rpcmethod
    async def reload_strategies(self) -> SuccessFail[list[StrategyConfig]]:
        return await self.trader.reload_strategies()

    @rpcmethod
    def get_status(self) -> dict:
        return self.trader.status()

    @rpcmethod
    async def scanner_data(
        self,
        scan_code: str = 'TOP_PERC_GAIN',
        instrument: str = 'STK',
        location_code: str = 'STK.US.MAJOR',
        num_rows: int = 20,
        above_price: float = 0.0,
        above_volume: int = 0,
        market_cap_above: float = 0.0,
    ) -> list[dict]:
        return await self.trader.scanner_data(
            scan_code=scan_code,
            instrument=instrument,
            location_code=location_code,
            num_rows=num_rows,
            above_price=above_price,
            above_volume=above_volume,
            market_cap_above=market_cap_above,
        )

    @rpcmethod
    def diagnose_portfolio_feed(self) -> dict:
        """Dump raw IB portfolio/positions per managed account. Used to
        diagnose "status shows margin used but positions=0"."""
        return self.trader.diagnose_portfolio_feed()

    @rpcmethod
    async def scanner_locations(self) -> list[dict]:
        """Diagnostic: list every location code this account is
        authorised to scan. If your target (e.g. ``STK.AU.ASX``) isn't
        in the result, the string is wrong or the subscription is
        missing — error 162 would follow any scan against it."""
        return await self.trader.scanner_locations()

    @rpcmethod
    async def get_snapshots_batch(self, contracts: list, delayed: bool = False) -> list[dict]:
        return await self.trader.get_snapshots_batch(contracts, delayed)

    @rpcmethod
    async def get_history_bars(self, contract: Contract, duration: str = '60 D', bar_size: str = '1 day') -> list[dict]:
        return await self.trader.get_history_bars(contract, duration, bar_size)

    @rpcmethod
    async def get_fundamental_data(self, contract: Contract, report_type: str = 'ReportSnapshot') -> str:
        return await self.trader.get_fundamental_data(contract, report_type)

    @rpcmethod
    async def get_news_headlines(self, conId: int, provider_codes: str = '',
                                  total_results: int = 5) -> list[dict]:
        return await self.trader.get_news_headlines(conId, provider_codes, total_results)

    @rpcmethod
    async def place_expressive_order(
        self,
        contract: Contract,
        action: str,
        quantity: float,
        execution_spec: dict,
        algo_name: str = 'proposal',
    ) -> SuccessFail[list[Trade]]:
        """Place an order with full execution specification (brackets, trailing stops, etc.)."""
        result = await self.trader.place_expressive_order(
            contract=contract,
            action=action,
            quantity=quantity,
            execution_spec=execution_spec,
            algo_name=algo_name,
        )
        return result

    @rpcmethod
    async def place_standalone_order(
        self,
        contract: Contract,
        action: str,
        quantity: float,
        order_type: str,
        aux_price: float = 0,
        limit_price: float = 0,
        trailing_percent: float = 0,
        tif: str = 'GTC',
        outside_rth: bool = True,
    ) -> SuccessFail[Trade]:
        """Place a standalone protective order (stop, trailing stop, limit)."""
        return await self.trader.place_standalone_order(
            contract=contract,
            action=action,
            quantity=quantity,
            order_type=order_type,
            aux_price=aux_price,
            limit_price=limit_price,
            trailing_percent=trailing_percent,
            tif=tif,
            outside_rth=outside_rth,
        )

    @rpcmethod
    async def check_order_margin(
        self,
        contract: Contract,
        order: Order,
    ) -> dict:
        """Simulate order to get margin impact without placing it."""
        return await self.trader.check_order_margin(contract, order)

    @rpcmethod
    def get_risk_limits(self) -> dict:
        """Return current risk gate limits."""
        from dataclasses import asdict
        return asdict(self.trader.risk_gate.limits)

    @rpcmethod
    def set_risk_limits(self, **kwargs) -> dict:
        """Update risk gate limits. Only provided fields are changed."""
        from dataclasses import asdict
        limits = self.trader.risk_gate.limits
        for key, value in kwargs.items():
            if hasattr(limits, key):
                setattr(limits, key, type(getattr(limits, key))(value))
        return asdict(limits)

    @rpcmethod
    async def get_ib_account(self) -> str:
        return self.trader.ib_account

    @rpcmethod
    def debug_raw_account_values(self) -> list:
        """Every AccountValue row the service currently holds (all tags, all
        currencies, all accounts). Diagnostic — used to see whether IB is
        streaming per-currency cash into this connection."""
        return [
            {'account': v.account, 'tag': v.tag, 'currency': v.currency, 'value': v.value, 'modelCode': getattr(v, 'modelCode', '')}
            for v in self.trader.client.ib.accountValues()
        ]

    @rpcmethod
    def get_account_values(self) -> dict:
        """Return key account values (cash, net liquidation, buying power, etc.).

        Scoped to the configured ``ib_account``. When the login manages
        multiple accounts (e.g. an advisor/master + client sub-accounts),
        ``ib.accountValues()`` returns rows for *all* of them; without an
        account filter the per-tag dict would be clobbered last-wins and
        could surface a different account's balances (the master's
        aggregate, say) under our account's label. Pin to ``ib_account``
        so the numbers always match the account we actually trade.
        """
        vals = self.trader.client.ib.accountValues()
        keys = {
            'TotalCashValue', 'NetLiquidation', 'AvailableFunds',
            'BuyingPower', 'GrossPositionValue', 'MaintMarginReq',
            'InitMarginReq', 'ExcessLiquidity', 'Cushion',
            'FullInitMarginReq', 'FullMaintMarginReq',
            'DayTradesRemaining',
        }
        active_account = self.trader.ib_account
        if not active_account:
            managed = self.trader.client.ib.managedAccounts() or []
            active_account = managed[0] if managed else None
        result = {}
        for v in vals:
            if v.tag not in keys or v.currency == 'BASE':
                continue
            # Only accept rows for the configured account. If we couldn't
            # determine one (no ib_account, no managed accounts), fall back
            # to unfiltered behaviour rather than returning nothing.
            if active_account and v.account and v.account != active_account:
                continue
            result[v.tag] = {'value': v.value, 'currency': v.currency}
        return result

    @rpcmethod
    def get_account_cash_by_currency(self) -> dict:
        """Per-currency cash ledger for the configured account.

        trader_service already holds the ``reqAccountUpdates`` subscription,
        so ``accountValues()`` carries the per-currency ``CashBalance`` and
        ``ExchangeRate`` rows. A second ad-hoc IB client can't obtain these
        — ``reqAccountUpdates`` is single-subscriber per account and the
        service owns it — which is why this read lives on the service.

        Scoped to ``ib_account`` exactly like ``get_account_values`` so a
        multi-account (advisor/master) login never leaks another account's
        cash. Returns::

            {
              'account': 'U26774889',
              'base_currency': 'CAD',
              'currencies': {
                'AUD': {'cash': 5000.0, 'exchange_rate': 0.9, 'base_value': 4500.0},
                'CAD': {'cash': 5000.0, 'exchange_rate': 1.0, 'base_value': 5000.0},
                'USD': {'cash': 5000.0, 'exchange_rate': 1.36, 'base_value': 6800.0},
              },
              'total_base_value': 16300.0,
            }

        ``exchange_rate`` / ``base_value`` are ``None`` when IB didn't supply
        a rate for that currency.
        """
        active_account = self.trader.ib_account
        if not active_account:
            managed = self.trader.client.ib.managedAccounts() or []
            active_account = managed[0] if managed else None

        def _for_account(v) -> bool:
            return not (active_account and v.account and v.account != active_account)

        # Per-currency cash arrives via reqAccountUpdates as ``$LEDGER-*``
        # rows (ib_async's rendering of IB's ``$LEDGER:ALL`` summary): tag
        # ``$LEDGER-CashBalance`` / ``$LEDGER-ExchangeRate`` per currency,
        # plus a consolidated ``BASE`` row we skip. Some non-ledger IB
        # account types instead expose plain ``CashBalance`` / ``ExchangeRate``
        # tags, so we accept both — the ledger form takes precedence.
        def _num(x):
            try:
                return float(x)
            except (TypeError, ValueError):
                return None

        ledger_cash: dict = {}
        plain_cash: dict = {}
        ledger_fx: dict = {}
        plain_fx: dict = {}
        base_currency = None
        for v in self.trader.client.ib.accountValues():
            if not _for_account(v):
                continue
            if v.tag == 'NetLiquidation' and v.currency and v.currency != 'BASE':
                # The account's own NetLiquidation row is denominated in the
                # account base currency — use it to label the base column.
                base_currency = v.currency
                continue
            cur = v.currency
            if not cur or cur == 'BASE':
                continue
            n = _num(v.value)
            if n is None:
                continue
            if v.tag == '$LEDGER-CashBalance':
                ledger_cash[cur] = n
            elif v.tag == 'CashBalance':
                plain_cash[cur] = n
            elif v.tag == '$LEDGER-ExchangeRate':
                ledger_fx[cur] = n
            elif v.tag == 'ExchangeRate':
                plain_fx[cur] = n

        # Ledger form wins where present; plain tags fill any gaps.
        cash = {**plain_cash, **ledger_cash}
        fx = {**plain_fx, **ledger_fx}
        # Base currency's rate is 1.0 by definition — backfill if IB omitted it.
        if base_currency and base_currency not in fx:
            fx[base_currency] = 1.0

        currencies: dict = {}
        total_base = 0.0
        have_total = False
        for cur in sorted(cash):
            amt = cash[cur]
            rate = fx.get(cur)
            base_value = (amt * rate) if rate is not None else None
            if base_value is not None:
                total_base += base_value
                have_total = True
            currencies[cur] = {
                'cash': amt,
                'exchange_rate': rate,
                'base_value': base_value,
            }

        # Fallback: an account holding only its base currency (or one where
        # IB reports just the consolidated TotalCashValue and no per-currency
        # CashBalance rows) would otherwise come back with no currencies at
        # all — misleading when there's clearly cash. Surface the base-
        # currency TotalCashValue so the view always reflects real cash.
        consolidated = False
        if not currencies:
            for v in self.trader.client.ib.accountValues():
                if not _for_account(v):
                    continue
                if v.tag == 'TotalCashValue' and v.currency and v.currency != 'BASE':
                    try:
                        amt = float(v.value)
                    except (TypeError, ValueError):
                        continue
                    currencies[v.currency] = {
                        'cash': amt,
                        'exchange_rate': 1.0 if v.currency == base_currency else fx.get(v.currency),
                        'base_value': amt if v.currency == base_currency else None,
                    }
                    total_base = amt
                    have_total = True
                    consolidated = True
                    break

        return {
            'account': active_account,
            'base_currency': base_currency,
            'currencies': currencies,
            'total_base_value': total_base if have_total else None,
            'consolidated': consolidated,
        }
