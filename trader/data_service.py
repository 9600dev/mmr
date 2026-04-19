"""DataService: concurrent historical data downloader.

Can run in two modes:
1. Direct (from CLI): instantiate and call pull_massive()/pull_ib()
2. Persistent: start_server() runs a ZMQ RPC server on port 42003
"""

from trader.common.helpers import dateify, pdt, timezoneify
from trader.common.logging_helper import setup_logging
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

                if len(df) > 0:
                    tick_data.write(security, df)
                    logging.debug('wrote {} rows for {}'.format(len(df), security.symbol))

                self._completed_count += 1
                return {'symbol': security.symbol, 'rows': len(df), 'ok': True}
            except Exception as ex:
                self._failed_count += 1
                logging.error('massive download failed for {}: {}'.format(security.symbol, ex))
                return {'symbol': security.symbol, 'error': str(ex), 'ok': False}
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

                if len(df) > 0:
                    tick_data.write(security, df)
                    logging.debug('wrote {} rows for {}'.format(len(df), security.symbol))

                self._completed_count += 1
                return {'symbol': security.symbol, 'rows': len(df), 'ok': True}
            except Exception as ex:
                self._failed_count += 1
                logging.error('twelvedata download failed for {}: {}'.format(security.symbol, ex))
                return {'symbol': security.symbol, 'error': str(ex), 'ok': False}
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
                    with IBHistoryWorker(
                        self.ib_server_address,
                        self.ib_server_port,
                        ib_client_id,
                    ) as worker:
                        return asyncio.run(worker.get_contract_history(
                            security=Universe.to_contract(security),
                            what_to_show=WhatToShow.TRADES,
                            start_date=dateify(date_range.start, timezone=security.timeZoneId, make_sod=True),
                            end_date=dateify(date_range.end, timezone=security.timeZoneId, make_eod=True),
                            bar_size=bar_size,
                            filter_between_dates=True,
                        ))

                df = await asyncio.to_thread(_ib_work)

                if len(df) > 0:
                    tick_data.write(security, df)
                    logging.debug('wrote {} rows for {}'.format(len(df), security.symbol))

                self._completed_count += 1
                return {'symbol': security.symbol, 'rows': len(df), 'ok': True}
            except Exception as ex:
                self._failed_count += 1
                logging.error('ib download failed for {}: {}'.format(security.symbol, ex))
                return {'symbol': security.symbol, 'error': str(ex), 'ok': False}
            finally:
                self._running_count -= 1

    async def pull_massive(
        self,
        symbols: Optional[list[str]] = None,
        universe: Optional[str] = None,
        bar_size: str = '1 day',
        prev_days: int = 30,
        max_concurrent: int = 5,
    ) -> dict:
        """Find missing date ranges, download from Massive concurrently, write to DuckDB.

        Returns {'enqueued': N, 'completed': N, 'failed': N, 'errors': [...]}
        """
        if not self.massive_api_key:
            return {'enqueued': 0, 'completed': 0, 'failed': 0,
                    'errors': ['massive_api_key not configured']}

        securities = self._resolve_symbols(symbols, universe)
        if not securities:
            return {'enqueued': 0, 'completed': 0, 'failed': 0,
                    'errors': ['no securities resolved']}

        bs = BarSize.parse_str(bar_size)
        tick_data = TickStorage(self.history_duckdb_path).get_tickdata(bar_size=bs)

        start_date = dateify(dt.datetime.now() - dt.timedelta(days=prev_days + 1), make_sod=True)
        end_date = dateify(dt.datetime.now() - dt.timedelta(days=1), make_eod=True)

        sem = asyncio.Semaphore(max_concurrent)
        tasks = []

        for security in securities:
            tz_start = timezoneify(start_date, timezone=security.timeZoneId)
            tz_end = timezoneify(end_date, timezone=security.timeZoneId)

            exchange_calendar = _try_get_exchange_calendar(security)

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

            for dr in date_ranges:
                tasks.append(self._download_massive_one(sem, security, dr, bs, tick_data))

        enqueued = len(tasks)
        if enqueued == 0:
            return {'enqueued': 0, 'completed': 0, 'failed': 0, 'errors': []}

        logging.info('enqueued {} massive download tasks'.format(enqueued))
        results = await asyncio.gather(*tasks, return_exceptions=True)

        errors = []
        completed = 0
        failed = 0
        for r in results:
            if isinstance(r, Exception):
                failed += 1
                errors.append(str(r))
            elif isinstance(r, dict) and r.get('ok'):
                completed += 1
            else:
                failed += 1
                if isinstance(r, dict):
                    errors.append(r.get('error', 'unknown error'))

        return {'enqueued': enqueued, 'completed': completed, 'failed': failed, 'errors': errors}

    async def pull_twelvedata(
        self,
        symbols: Optional[list[str]] = None,
        universe: Optional[str] = None,
        bar_size: str = '1 day',
        prev_days: int = 30,
        max_concurrent: int = 5,
    ) -> dict:
        """Find missing date ranges, download from TwelveData concurrently, write to DuckDB.

        Returns {'enqueued': N, 'completed': N, 'failed': N, 'errors': [...]}
        """
        if not self.twelvedata_api_key:
            return {'enqueued': 0, 'completed': 0, 'failed': 0,
                    'errors': ['twelvedata_api_key not configured']}

        securities = self._resolve_symbols(symbols, universe)
        if not securities:
            return {'enqueued': 0, 'completed': 0, 'failed': 0,
                    'errors': ['no securities resolved']}

        bs = BarSize.parse_str(bar_size)
        tick_data = TickStorage(self.history_duckdb_path).get_tickdata(bar_size=bs)

        start_date = dateify(dt.datetime.now() - dt.timedelta(days=prev_days + 1), make_sod=True)
        end_date = dateify(dt.datetime.now() - dt.timedelta(days=1), make_eod=True)

        sem = asyncio.Semaphore(max_concurrent)
        tasks = []

        for security in securities:
            tz_start = timezoneify(start_date, timezone=security.timeZoneId)
            tz_end = timezoneify(end_date, timezone=security.timeZoneId)

            exchange_calendar = _try_get_exchange_calendar(security)

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

            for dr in date_ranges:
                tasks.append(self._download_twelvedata_one(sem, security, dr, bs, tick_data))

        enqueued = len(tasks)
        if enqueued == 0:
            return {'enqueued': 0, 'completed': 0, 'failed': 0, 'errors': []}

        logging.info('enqueued {} twelvedata download tasks'.format(enqueued))
        results = await asyncio.gather(*tasks, return_exceptions=True)

        errors = []
        completed = 0
        failed = 0
        for r in results:
            if isinstance(r, Exception):
                failed += 1
                errors.append(str(r))
            elif isinstance(r, dict) and r.get('ok'):
                completed += 1
            else:
                failed += 1
                if isinstance(r, dict):
                    errors.append(r.get('error', 'unknown error'))

        return {'enqueued': enqueued, 'completed': completed, 'failed': failed, 'errors': errors}

    async def pull_ib(
        self,
        symbols: Optional[list[str]] = None,
        universe: Optional[str] = None,
        bar_size: str = '1 min',
        prev_days: int = 5,
        ib_client_id: int = 10,
        max_concurrent: int = 1,
    ) -> dict:
        """Find missing date ranges, download from IB concurrently, write to DuckDB.

        Returns {'enqueued': N, 'completed': N, 'failed': N, 'errors': [...]}
        """
        securities = self._resolve_symbols(symbols, universe)
        if not securities:
            return {'enqueued': 0, 'completed': 0, 'failed': 0,
                    'errors': ['no securities resolved']}

        bs = BarSize.parse_str(bar_size)
        tick_data = TickStorage(self.history_duckdb_path).get_tickdata(bar_size=bs)

        start_date = dateify(dt.datetime.now() - dt.timedelta(days=prev_days + 1), make_sod=True)
        end_date = dateify(dt.datetime.now() - dt.timedelta(days=1), make_eod=True)

        sem = asyncio.Semaphore(max_concurrent)
        tasks = []
        task_index = 0

        for security in securities:
            tz_start = timezoneify(start_date, timezone=security.timeZoneId)
            tz_end = timezoneify(end_date, timezone=security.timeZoneId)

            exchange_calendar = _try_get_exchange_calendar(security)

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

            for dr in date_ranges:
                # Each IB task needs a unique client_id
                client_id = ib_client_id + task_index
                tasks.append(self._download_ib_one(sem, security, dr, bs, tick_data, client_id))
                task_index += 1

        enqueued = len(tasks)
        if enqueued == 0:
            return {'enqueued': 0, 'completed': 0, 'failed': 0, 'errors': []}

        logging.info('enqueued {} ib download tasks'.format(enqueued))
        results = await asyncio.gather(*tasks, return_exceptions=True)

        errors = []
        completed = 0
        failed = 0
        for r in results:
            if isinstance(r, Exception):
                failed += 1
                errors.append(str(r))
            elif isinstance(r, dict) and r.get('ok'):
                completed += 1
            else:
                failed += 1
                if isinstance(r, dict):
                    errors.append(r.get('error', 'unknown error'))

        return {'enqueued': enqueued, 'completed': completed, 'failed': failed, 'errors': errors}

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

    config_file = args.config or default_config_path()
    container = Container.create(config_file)
    service = container.resolve(DataService)

    logging.info('starting data_service in persistent RPC server mode')
    service.start_server()


if __name__ == '__main__':
    main()
