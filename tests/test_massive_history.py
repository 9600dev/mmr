import datetime as dt
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from trader.objects import BarSize


# ---------------------------------------------------------------------------
# TestBarSizeMapping
# ---------------------------------------------------------------------------

class TestBarSizeMapping:
    def test_all_bar_sizes_have_mapping(self):
        for bar_size in BarSize:
            mult, timespan = BarSize.to_massive_timespan(bar_size)
            assert isinstance(mult, int)
            assert mult > 0
            assert timespan in ('second', 'minute', 'hour', 'day', 'week', 'month')

    def test_specific_mappings(self):
        assert BarSize.to_massive_timespan(BarSize.Mins1) == (1, 'minute')
        assert BarSize.to_massive_timespan(BarSize.Days1) == (1, 'day')
        assert BarSize.to_massive_timespan(BarSize.Hours1) == (1, 'hour')
        assert BarSize.to_massive_timespan(BarSize.Secs5) == (5, 'second')
        assert BarSize.to_massive_timespan(BarSize.Weeks1) == (1, 'week')
        assert BarSize.to_massive_timespan(BarSize.Months1) == (1, 'month')
        assert BarSize.to_massive_timespan(BarSize.Mins30) == (30, 'minute')
        assert BarSize.to_massive_timespan(BarSize.Hours4) == (4, 'hour')


# ---------------------------------------------------------------------------
# TestMassiveHistoryWorker
# ---------------------------------------------------------------------------

class TestMassiveHistoryWorker:
    def _make_agg(self, ts_ms, o=100.0, h=101.0, l=99.0, c=100.5, v=1000.0, vw=100.3, n=50):
        agg = MagicMock()
        agg.timestamp = ts_ms
        agg.open = o
        agg.high = h
        agg.low = l
        agg.close = c
        agg.volume = v
        agg.vwap = vw
        agg.transactions = n
        return agg

    @patch('trader.listeners.massive_history.RESTClient')
    def test_get_history_returns_dataframe(self, mock_rest_cls):
        from trader.listeners.massive_history import MassiveHistoryWorker

        # Two aggs 1 minute apart
        base_ts = int(dt.datetime(2024, 6, 1, 14, 30, tzinfo=dt.timezone.utc).timestamp() * 1000)
        agg1 = self._make_agg(base_ts)
        agg2 = self._make_agg(base_ts + 60_000, o=100.5, h=101.5, l=99.5, c=101.0, v=2000.0)

        mock_client = MagicMock()
        mock_client.list_aggs.return_value = [agg1, agg2]
        mock_rest_cls.return_value = mock_client

        worker = MassiveHistoryWorker(massive_api_key='test_key')
        df = worker.get_history(
            ticker='AAPL',
            bar_size=BarSize.Mins1,
            start_date=dt.datetime(2024, 6, 1),
            end_date=dt.datetime(2024, 6, 1),
            timezone='US/Eastern',
        )

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert 'open' in df.columns
        assert 'high' in df.columns
        assert 'low' in df.columns
        assert 'close' in df.columns
        assert 'volume' in df.columns
        assert 'average' in df.columns
        assert 'bar_count' in df.columns
        assert 'bar_size' in df.columns
        assert 'what_to_show' in df.columns
        assert df.index.name == 'date'

        # Verify data
        assert df['open'].iloc[0] == 100.0
        assert df['close'].iloc[1] == 101.0
        assert df['average'].iloc[0] == 100.3

    @patch('trader.listeners.massive_history.RESTClient')
    def test_get_history_empty_result(self, mock_rest_cls):
        from trader.listeners.massive_history import MassiveHistoryWorker

        mock_client = MagicMock()
        mock_client.list_aggs.return_value = []
        mock_rest_cls.return_value = mock_client

        worker = MassiveHistoryWorker(massive_api_key='test_key')
        df = worker.get_history(
            ticker='AAPL',
            bar_size=BarSize.Days1,
            start_date=dt.datetime(2024, 6, 1),
            end_date=dt.datetime(2024, 6, 1),
        )

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    @patch('trader.listeners.massive_history.RESTClient')
    def test_get_history_skips_null_timestamps(self, mock_rest_cls):
        from trader.listeners.massive_history import MassiveHistoryWorker

        base_ts = int(dt.datetime(2024, 6, 1, 14, 30, tzinfo=dt.timezone.utc).timestamp() * 1000)
        agg_good = self._make_agg(base_ts)
        agg_bad = self._make_agg(None)

        mock_client = MagicMock()
        mock_client.list_aggs.return_value = [agg_good, agg_bad]
        mock_rest_cls.return_value = mock_client

        worker = MassiveHistoryWorker(massive_api_key='test_key')
        df = worker.get_history(
            ticker='AAPL',
            bar_size=BarSize.Mins1,
            start_date=dt.datetime(2024, 6, 1),
            end_date=dt.datetime(2024, 6, 1),
        )

        assert len(df) == 1


# ---------------------------------------------------------------------------
# TestDataServiceInit
# ---------------------------------------------------------------------------

class TestDataServiceInit:
    def test_data_service_init(self, tmp_duckdb_path):
        from trader.data_service import DataService

        service = DataService(
            massive_api_key='test_key',
            duckdb_path=tmp_duckdb_path,
        )
        assert service.massive_api_key == 'test_key'
        assert service.duckdb_path == tmp_duckdb_path
