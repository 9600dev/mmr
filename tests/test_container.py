import os
import pytest
from trader.container import Container


class _SimpleService:
    """A dummy service class whose __init__ params match config keys."""
    def __init__(self, duckdb_path: str = '', ib_server_address: str = ''):
        self.duckdb_path = duckdb_path
        self.ib_server_address = ib_server_address



class TestContainer:
    def test_create_loads_config(self, test_config_file):
        container = Container.create(test_config_file)
        assert container.config_file == test_config_file
        assert isinstance(container.configuration, dict)
        Container._instance = None  # cleanup class-level state

    def test_instance_returns_same_object(self, test_config_file):
        c1 = Container.create(test_config_file)
        c2 = Container.instance()
        assert c1 is c2
        Container._instance = None

    def test_resolve_with_config_values(self, test_config_file):
        container = Container.create(test_config_file)
        svc = container.resolve(_SimpleService)
        assert svc.ib_server_address == '127.0.0.1'
        assert 'duckdb' in svc.duckdb_path or svc.duckdb_path != ''
        Container._instance = None

    def test_resolve_with_extra_args(self, test_config_file):
        container = Container.create(test_config_file)
        svc = container.resolve(_SimpleService, duckdb_path='/override/path.duckdb')
        assert svc.duckdb_path == '/override/path.duckdb'
        Container._instance = None

    def test_resolve_cache_returns_same_instance(self, test_config_file):
        container = Container.create(test_config_file)
        s1 = container.resolve_cache(_SimpleService)
        s2 = container.resolve_cache(_SimpleService)
        assert s1 is s2
        Container._instance = None

