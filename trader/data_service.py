"""DataService: concurrent historical data downloader.

Can run in two modes:
1. Direct (from CLI): instantiate and call pull_massive()/pull_ib()
2. Persistent: start_server() runs a ZMQ RPC server on port 42003
"""

from trader.common.helpers import dateify, pdt
from trader.common.logging_helper import prune_old_logs, setup_logging
from trader.container import Container, default_config_path
from trader.data.data_access import SecurityDefinition, TickData, TickStorage
from trader.data.store import DateRange
from trader.data.universe import Universe, UniverseAccessor
from trader.listeners.ib_history_worker import IBHistoryWorker
from trader.listeners.massive_history import MassiveHistoryWorker
from trader.listeners.twelvedata_history import TwelveDataHistoryWorker
from trader.messaging.clientserver import RPCServer
from trader.messaging.data_service_api import DataServiceApi
from trader.objects import BarSize, WhatToShow
from typing import List, Optional

import argparse
import asyncio
import datetime as dt
import exchange_calendars
import signal


logging = setup_logging(module_name='data_service')


def _try_get_exchange_calendar(security: SecurityDefinition):
    try:
        return exchange_calendars.get_calendar(security.primaryExchange)
    except exchange_calendars.exchange_calendar.errors.InvalidCalendarName:
        try:
            return exchange_calendars.get_calendar(security.exchange)
        except Exception:
            return None


def _task_label(security: SecurityDefinition, date_range: DateRange) -> dict:
    def _iso(value):
        return value.isoformat() if hasattr(value, 'isoformat') else str(value)
    return {'symbol': security.symbol, 'conId': security.conId,
            'start': _iso(date_range.start), 'end': _iso(date_range.end)}


class DataService:
    def __init__(
        self,
        massive_api_key: str = '',
        twelvedata_api_key: str = '',
        ib_server_address: str = '127.0.0.1',
        ib_server_port: int = 7497,
        duckdb_path: str = '',
        history_duckdb_path: str = '',
        universe_library: str = 'Universes',
        zmq_data_rpc_server_address: str = 'tcp://127.0.0.1',
        zmq_data_rpc_server_port: int = 42003,
        **kwargs,
    ):
        self.massive_api_key = massive_api_key
        self.twelvedata_api_key = twelvedata_api_key
        self.ib_server_address = ib_server_address
        self.ib_server_port = ib_server_port
        self.duckdb_path = duckdb_path
        self.history_duckdb_path = history_duckdb_path or duckdb_path
        self.universe_library = universe_library
        self.zmq_data_rpc_server_address = zmq_data_rpc_server_address
        self.zmq_data_rpc_server_port = zmq_data_rpc_server_port

        self._running_count = 0
        self._completed_count = 0
        self._failed_count = 0

    def _resolve_symbols(
        self,
        symbols: Optional[list[str]],
        universe: Optional[str],
    ) -> List[SecurityDefinition]:
        accessor = UniverseAccessor(self.duckdb_path, self.universe_library)

        if symbols and universe:
            u = accessor.get(universe)
            defs = []
            for sym in symbols:
                sec_def = u.find_symbol(sym)
                if sec_def:
                    defs.append(sec_def)
                else:
                    logging.warning('symbol {} not found in universe {}'.format(sym, universe))
            return defs
        elif universe:
            u = accessor.get(universe)
            return u.security_definitions
        elif symbols:
            # Try to find symbols across all universes
            defs = []
            for sym in symbols:
                results = accessor.resolve_symbol(sym)
                if results:
                    defs.append(results[0])
                else:
                    logging.warning('symbol {} not found in any universe'.format(sym))
            return defs
        else:
            return []

    async def _download_massive_one(
        self,
        sem: asyncio.Semaphore,
        security: SecurityDefinition,
        date_range: DateRange,
        bar_size: BarSize,
        tick_data: TickData,
    ) -> dict:
        async with sem:
            self._running_count += 1
            try:
                logging.info('downloading massive {} from {} to {}'.format(
                    security.symbol, pdt(date_range.start), pdt(date_range.end)
                ))

                worker = MassiveHistoryWorker(self.massive_api_key)
                df = await asyncio.to_thread(
                    worker.get_history,
                    ticker=security.symbol,
                    bar_size=bar_size,
                    start_date=dateify(date_range.start, timezone=security.timeZoneId, make_sod=True),
                    end_date=dateify(date_range.end, timezone=security.timeZoneId, make_eod=True),
                    timezone=security.timeZoneId if security.timeZoneId else 'US/Eastern',
                )

                written = tick_data.write(security, df) if len(df) > 0 else 0
                logging.debug('wrote {} of {} rows for {}'.format(written, len(df), security.symbol))

                self._completed_count += 1
                return {**_task_label(security, date_range), 'fetched': len(df),
                        'rows': written, 'ok': True}
            except Exception as ex:
                self._failed_count += 1
                logging.error('massive download failed for {}: {}'.format(security.symbol, ex))
                return {**_task_label(security, date_range), 'error': str(ex), 'ok': False}
            finally:
                self._running_count -= 1

    async def _download_twelvedata_one(
        self,
        sem: asyncio.Semaphore,
        security: SecurityDefinition,
        date_range: DateRange,
        bar_size: BarSize,
        tick_data: TickData,
    ) -> dict:
        async with sem:
            self._running_count += 1
            try:
                logging.info('downloading twelvedata {} from {} to {}'.format(
                    security.symbol, pdt(date_range.start), pdt(date_range.end)
                ))

                worker = TwelveDataHistoryWorker(self.twelvedata_api_key)
                df = await asyncio.to_thread(
                    worker.get_history,
                    ticker=security.symbol,
                    bar_size=bar_size,
                    start_date=dateify(date_range.start, timezone=security.timeZoneId, make_sod=True),
                    end_date=dateify(date_range.end, timezone=security.timeZoneId, make_eod=True),
                    timezone=security.timeZoneId if security.timeZoneId else 'US/Eastern',
                )

                written = tick_data.write(security, df) if len(df) > 0 else 0
                logging.debug('wrote {} of {} rows for {}'.format(written, len(df), security.symbol))

                self._completed_count += 1
                return {**_task_label(security, date_range), 'fetched': len(df),
                        'rows': written, 'ok': True}
            except Exception as ex:
                self._failed_count += 1
                logging.error('twelvedata download failed for {}: {}'.format(security.symbol, ex))
                return {**_task_label(security, date_range), 'error': str(ex), 'ok': False}
            finally:
                self._running_count -= 1

    async def _download_ib_one(
        self,
        sem: asyncio.Semaphore,
        security: SecurityDefinition,
        date_range: DateRange,
        bar_size: BarSize,
        tick_data: TickData,
        ib_client_id: int,
    ) -> dict:
        async with sem:
            self._running_count += 1
            try:
                logging.info('downloading ib {} from {} to {}'.format(
                    security.symbol, pdt(date_range.start), pdt(date_range.end)
                ))

                def _ib_work():
                    # Connect AND fetch on the SAME event loop. The previous
                    # code sync-connected via `with IBHistoryWorker(...)`
                    # (__enter__ → sync connect, binding the IB socket to
                    # ib_async's util loop) and then drove the request on a
                    # separate asyncio.run() loop — so every request timed out
                    # and the empty result was backfilled with NaN bars.
                    worker = IBHistoryWorker(
                        self.ib_server_address,
                        self.ib_server_port,
                        ib_client_id,
                    )

                    async def _run():
                        try:
                            await worker.connect_async()
                            return await worker.get_contract_history(
                                security=Universe.to_contract(security),
                                what_to_show=WhatToShow.TRADES,
                                start_date=dateify(date_range.start, timezone=security.timeZoneId, make_sod=True),
                                end_date=dateify(date_range.end, timezone=security.timeZoneId, make_eod=True),
                                bar_size=bar_size,
                                filter_between_dates=True,
                            )
                        finally:
                            worker.shutdown()

                    return asyncio.run(_run())

                df = await asyncio.to_thread(_ib_work)

                written = tick_data.write(security, df) if len(df) > 0 else 0
                logging.debug('wrote {} of {} rows for {}'.format(written, len(df), security.symbol))

                self._completed_count += 1
                return {**_task_label(security, date_range), 'fetched': len(df),
                        'rows': written, 'ok': True}
            except Exception as ex:
                self._failed_count += 1
                logging.error('ib download failed for {}: {}'.format(security.symbol, ex))
                return {**_task_label(security, date_range), 'error': str(ex), 'ok': False}
            finally:
                self._running_count -= 1

    def _plan_downloads(
        self,
        securities: List[SecurityDefinition],
        tick_data: TickData,
        prev_days: int,
        now: Optional[dt.datetime] = None,
        force: bool = False,
    ) -> List[tuple]:
        """(security, DateRange) for every missing range in each security's window.

        One task per CONTIGUOUS missing range, so one symbol can legitimately
        enqueue several: `history ib --symbol AAPL` reporting "2 enqueued" is
        two disjoint gaps in AAPL's stored window, not a duplicate job.

        The window is [today - prev_days - 1, yesterday] dated in the
        SECURITY's timezone. It used to be computed from the HOST's naive
        local clock and then converted, so on a host west of the venue (a
        Pacific host and a US listing) "yesterday 23:59:59" local landed on
        TODAY in New York, missing() asked for today's session, and the
        still-forming daily bar was downloaded and stored as complete.

        ``force`` re-downloads the WHOLE window even where bars exist (the
        store replaces matching keys), the only way to repair a bad stored bar
        — e.g. a partial session written before the unclosed-session guard.
        """
        current = now or dt.datetime.now(dt.timezone.utc)
        if current.tzinfo is None:
            raise ValueError('now must be timezone-aware')
        plan: List[tuple] = []
        for security in securities:
            tz = security.timeZoneId or 'US/Eastern'
            tz_start = dateify(current - dt.timedelta(days=prev_days + 1), timezone=tz, make_sod=True)
            tz_end = dateify(current - dt.timedelta(days=1), timezone=tz, make_eod=True)

            exchange_calendar = _try_get_exchange_calendar(security)
            if force:
                plan.append((security, DateRange(start=tz_start, end=tz_end)))
                continue
            try:
                if exchange_calendar:
                    date_ranges = tick_data.missing(
                        security,
                        exchange_calendar,
                        date_range=DateRange(start=tz_start, end=tz_end),
                    )
                else:
                    date_ranges = [DateRange(start=tz_start, end=tz_end)]
            except Exception as ex:
                logging.warning('missing() failed for {}: {}, downloading full range'.format(
                    security.symbol, ex))
                date_ranges = [DateRange(start=tz_start, end=tz_end)]
            plan.extend((security, dr) for dr in date_ranges)
        return plan

    @staticmethod
    def _summarize(securities: List[SecurityDefinition], results: list) -> dict:
        errors = []
        completed = 0
        failed = 0
        tasks = []
        for r in results:
            if isinstance(r, BaseException):
                failed += 1
                errors.append(str(r))
                tasks.append({'ok': False, 'error': str(r)})
                continue
            tasks.append(r)
            if isinstance(r, dict) and r.get('ok'):
                completed += 1
            else:
                failed += 1
                if isinstance(r, dict):
                    errors.append(r.get('error', 'unknown error'))
        return {'enqueued': len(results), 'completed': completed, 'failed': failed,
                'errors': errors, 'symbols': [s.symbol for s in securities], 'tasks': tasks}

    async def _pull(self, source: str, symbols, universe, bar_size: str, prev_days: int,
                    max_concurrent: int, make_task, force: bool = False) -> dict:
        securities = self._resolve_symbols(symbols, universe)
        if not securities:
            return {'enqueued': 0, 'completed': 0, 'failed': 0,
                    'errors': ['no securities resolved'], 'symbols': [], 'tasks': []}

        bs = BarSize.parse_str(bar_size)
        tick_data = TickStorage(self.history_duckdb_path).get_tickdata(bar_size=bs)
        plan = self._plan_downloads(securities, tick_data, prev_days, force=force)
        if not plan:
            return self._summarize(securities, [])

        sem = asyncio.Semaphore(max_concurrent)
        tasks = [make_task(i, sem, security, dr, bs, tick_data)
                 for i, (security, dr) in enumerate(plan)]
        logging.info('enqueued {} {} download tasks'.format(len(tasks), source))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return self._summarize(securities, list(results))

    async def pull_massive(
        self,
        symbols: Optional[list[str]] = None,
        universe: Optional[str] = None,
        bar_size: str = '1 day',
        prev_days: int = 30,
        max_concurrent: int = 5,
        force: bool = False,
    ) -> dict:
        """Find missing date ranges, download from Massive concurrently, write to DuckDB.

        Returns {'enqueued', 'completed', 'failed', 'errors', 'symbols', 'tasks'}
        where each task names its symbol and date range.
        """
        if not self.massive_api_key:
            return {'enqueued': 0, 'completed': 0, 'failed': 0,
                    'errors': ['massive_api_key not configured'], 'symbols': [], 'tasks': []}
        return await self._pull(
            'massive', symbols, universe, bar_size, prev_days, max_concurrent,
            force=force, make_task=
            lambda i, sem, sec, dr, bs, td: self._download_massive_one(sem, sec, dr, bs, td))

    async def pull_twelvedata(
        self,
        symbols: Optional[list[str]] = None,
        universe: Optional[str] = None,
        bar_size: str = '1 day',
        prev_days: int = 30,
        max_concurrent: int = 5,
        force: bool = False,
    ) -> dict:
        """Find missing date ranges, download from TwelveData concurrently, write to DuckDB.

        Returns {'enqueued', 'completed', 'failed', 'errors', 'symbols', 'tasks'}.
        """
        if not self.twelvedata_api_key:
            return {'enqueued': 0, 'completed': 0, 'failed': 0,
                    'errors': ['twelvedata_api_key not configured'], 'symbols': [], 'tasks': []}
        return await self._pull(
            'twelvedata', symbols, universe, bar_size, prev_days, max_concurrent,
            force=force, make_task=
            lambda i, sem, sec, dr, bs, td: self._download_twelvedata_one(sem, sec, dr, bs, td))

    async def pull_ib(
        self,
        symbols: Optional[list[str]] = None,
        universe: Optional[str] = None,
        bar_size: str = '1 min',
        prev_days: int = 5,
        ib_client_id: int = 10,
        max_concurrent: int = 1,
        force: bool = False,
    ) -> dict:
        """Find missing date ranges, download from IB concurrently, write to DuckDB.

        Returns {'enqueued', 'completed', 'failed', 'errors', 'symbols', 'tasks'}.
        Each IB task gets its own client id (ib_client_id + task index).
        """
        return await self._pull(
            'ib', symbols, universe, bar_size, prev_days, max_concurrent,
            force=force, make_task=
            lambda i, sem, sec, dr, bs, td: self._download_ib_one(
                sem, sec, dr, bs, td, ib_client_id + i))

    async def status(self) -> dict:
        """Return current state: running jobs, completed count, etc."""
        return {
            'running': self._running_count,
            'completed': self._completed_count,
            'failed': self._failed_count,
        }

    def start_server(self):
        """Persistent mode: start ZMQ RPC server + run event loop."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        api = DataServiceApi(self)
        rpc_server = RPCServer(
            instance=api,
            zmq_rpc_server_address=self.zmq_data_rpc_server_address,
            zmq_rpc_server_port=self.zmq_data_rpc_server_port,
        )

        is_stopping = False

        async def run():
            await rpc_server.serve()
            logging.info('data_service RPC server listening on {}:{}'.format(
                self.zmq_data_rpc_server_address, self.zmq_data_rpc_server_port
            ))
            # Keep running until cancelled
            try:
                while True:
                    await asyncio.sleep(3600)
            except asyncio.CancelledError:
                pass

        async def graceful_shutdown():
            nonlocal is_stopping
            if is_stopping:
                return
            is_stopping = True
            logging.info('shutting down data_service...')
            rpc_server.close()
            pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
            if pending:
                await asyncio.wait(pending, timeout=5)
                still_pending = [t for t in pending if not t.done()]
                for t in still_pending:
                    t.cancel()
                if still_pending:
                    await asyncio.wait(still_pending, timeout=2)
            loop.stop()

        def handle_sigint():
            asyncio.ensure_future(graceful_shutdown())

        try:
            loop.add_signal_handler(signal.SIGINT, handle_sigint)
            loop.add_signal_handler(signal.SIGTERM, handle_sigint)
            loop.run_until_complete(run())
        except KeyboardInterrupt:
            pass


def main():
    parser = argparse.ArgumentParser(prog='data-service', description='MMR Data Service')
    parser.add_argument('--config', default='', help='trader.yaml config file location')
    args = parser.parse_args()

    # Session log retention (MMR_LOG_RETENTION_DAYS, default 14; 0 = off).
    prune_old_logs(logger=logging)

    config_file = args.config or default_config_path()
    container = Container.create(config_file)
    service = container.resolve(DataService)

    logging.info('starting data_service in persistent RPC server mode')
    service.start_server()


if __name__ == '__main__':
    main()
