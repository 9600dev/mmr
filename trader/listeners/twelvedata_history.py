from trader.common.logging_helper import setup_logging
from trader.objects import BarSize, WhatToShow
from twelvedata import TDClient

import datetime as dt
import pandas as pd
import pytz
import time


logging = setup_logging(module_name='twelvedata_history')


# TwelveData caps a single time_series response at 5000 bars. Requesting a
# wider window silently truncates to the most recent 5000 bars, so we chunk
# the date range. Values below are calendar days per chunk, chosen to stay
# safely under the 5000-bar cap assuming a liquid US equity session.
_CHUNK_DAYS_FOR_BAR_SIZE = {
    BarSize.Mins1: 7,      # ~2700 bars / 7d
    BarSize.Mins5: 35,     # ~2700 bars / 35d
    BarSize.Mins15: 100,   # ~2600 bars / 100d
    BarSize.Mins30: 200,   # ~2600 bars / 200d
    BarSize.Hours1: 400,   # ~2800 bars / 400d
    BarSize.Hours2: 400,
    BarSize.Hours4: 400,
    # Daily/weekly/monthly: no need to chunk — 5000 daily bars is ~20y.
    BarSize.Days1: None,
    BarSize.Weeks1: None,
    BarSize.Months1: None,
}

# Small per-chunk pacing so paginated downloads don't burst past pro-plan
# rate limits (typical pro plans allow ~55-610 req/min; 10 req/s is a safe
# middle ground).
_CHUNK_PACING_SECS = 0.1


def _is_benign_no_data(ex: Exception) -> bool:
    """Classify a time_series exception as a known "chunk has no bars" signal
    vs. a real failure. TwelveData surfaces empty spans as
    "No data is available on the specified dates" — those are expected for
    holiday-only weeks or very recent intraday edge cases, not a plan-limit
    or outage."""
    msg = str(ex).lower()
    return 'no data is available' in msg or 'not found in our database' in msg


class TwelveDataHistoryWorker:
    def __init__(self, twelvedata_api_key: str):
        if not twelvedata_api_key:
            raise ValueError('twelvedata_api_key is required')
        self.client = TDClient(apikey=twelvedata_api_key)

    def get_history(
        self,
        ticker: str,
        bar_size: BarSize,
        start_date: dt.datetime,
        end_date: dt.datetime,
        timezone: str = 'US/Eastern',
    ) -> pd.DataFrame:
        interval = BarSize.to_twelvedata_interval(bar_size)
        chunk_days = _CHUNK_DAYS_FOR_BAR_SIZE.get(bar_size)

        if chunk_days is None:
            # Daily or coarser — single call is always enough.
            return self._fetch_one(ticker, bar_size, interval, start_date, end_date, timezone)

        logging.info('get_history {} {} {} {} to {} (chunked @ {}d)'.format(
            ticker, bar_size, interval,
            start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'),
            chunk_days,
        ))

        frames = []
        chunk_start = start_date
        chunk_idx = 0
        skipped = 0
        while chunk_start < end_date:
            chunk_end = min(chunk_start + dt.timedelta(days=chunk_days), end_date)
            try:
                df_chunk = self._fetch_one(
                    ticker, bar_size, interval, chunk_start, chunk_end, timezone,
                    log_prefix='  chunk {}: '.format(chunk_idx),
                )
                if df_chunk is not None and not df_chunk.empty:
                    frames.append(df_chunk)
            except Exception as ex:
                # A single bad chunk (weekend-only span, TwelveData "no data"
                # for a holiday week, transient 429) must NOT abort the whole
                # symbol — log and keep going. The caller sees partial
                # coverage, not empty output.
                skipped += 1
                if _is_benign_no_data(ex):
                    logging.warning('  chunk {}: no data ({} {} to {}): {}'.format(
                        chunk_idx, ticker,
                        chunk_start.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d'),
                        ex,
                    ))
                else:
                    logging.error('  chunk {}: {} {} to {} failed: {}'.format(
                        chunk_idx, ticker,
                        chunk_start.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d'),
                        ex,
                    ))
            chunk_start = chunk_end
            chunk_idx += 1
            if chunk_start < end_date:
                time.sleep(_CHUNK_PACING_SECS)

        if not frames:
            logging.info('no data returned for {} over full window ({} chunks, {} skipped)'.format(
                ticker, chunk_idx, skipped))
            return pd.DataFrame()

        df = pd.concat(frames)
        df = df[~df.index.duplicated(keep='first')]
        df.sort_index(ascending=True, inplace=True)
        logging.info('get_history returned {} total rows for {} ({} chunks, {} skipped)'.format(
            len(df), ticker, chunk_idx, skipped
        ))
        return df

    def _fetch_one(
        self,
        ticker: str,
        bar_size: BarSize,
        interval: str,
        start_date: dt.datetime,
        end_date: dt.datetime,
        timezone: str,
        log_prefix: str = '',
    ) -> pd.DataFrame:
        if bar_size in (BarSize.Days1, BarSize.Weeks1, BarSize.Months1):
            from_ = start_date.strftime('%Y-%m-%d')
            to = end_date.strftime('%Y-%m-%d')
        else:
            from_ = start_date.strftime('%Y-%m-%d %H:%M:%S')
            to = end_date.strftime('%Y-%m-%d %H:%M:%S')

        logging.info('{}fetch {} {} {} to {}'.format(log_prefix, ticker, interval, from_, to))

        ts = self.client.time_series(
            symbol=ticker,
            interval=interval,
            start_date=from_,
            end_date=to,
            outputsize=5000,
            timezone=timezone,
        )

        try:
            df = ts.as_pandas()
        except Exception as ex:
            logging.error('twelvedata time_series failed for {} [{} to {}]: {}'.format(
                ticker, from_, to, ex))
            raise

        if df is None or len(df) == 0:
            logging.info('{}no data returned for {}'.format(log_prefix, ticker))
            return pd.DataFrame()

        df = df.copy()
        df.index.name = 'date'

        idx = pd.to_datetime(df.index)
        if idx.tz is None:
            idx = idx.tz_localize(pytz.timezone(timezone))
        else:
            idx = idx.tz_convert(pytz.timezone(timezone))
        df.index = idx

        for col in ('open', 'high', 'low', 'close', 'volume'):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df['average'] = float('nan')
        df['bar_count'] = 0
        df['bar_size'] = str(bar_size)
        df['what_to_show'] = int(WhatToShow.TRADES)

        keep = ['open', 'high', 'low', 'close', 'volume', 'average', 'bar_count', 'bar_size', 'what_to_show']
        df = df[[c for c in keep if c in df.columns]]

        df.sort_index(ascending=True, inplace=True)
        logging.info('{}returned {} rows for {}'.format(log_prefix, len(df), ticker))
        return df
