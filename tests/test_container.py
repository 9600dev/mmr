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


class _RequiresMissing:
    def __init__(self, nonexistent_required: str, duckdb_path: str = ''):
        self.nonexistent_required = nonexistent_required
        self.duckdb_path = duckdb_path


class _NeedsTypedPort:
    def __init__(self, custom_env_port: int = 0):
        self.custom_env_port = custom_env_port


class TestContainerHardening:
    def test_missing_required_param_raises_explicit(self, test_config_file):
        """Constructor with a required param that isn't in config/env/extra_args
        should raise ContainerResolutionError naming the param — not a cryptic
        TypeError."""
        from trader.container import ContainerResolutionError

        container = Container.create(test_config_file)
        try:
            with pytest.raises(ContainerResolutionError) as exc:
                container.resolve(_RequiresMissing)
            assert 'nonexistent_required' in str(exc.value)
        finally:
            Container._instance = None

    def test_env_var_coerced_to_int(self, test_config_file, monkeypatch):
        """An int-annotated param with an env var string value gets coerced."""
        monkeypatch.setenv('CUSTOM_ENV_PORT', '9000')
        container = Container.create(test_config_file)
        try:
            svc = container.resolve(_NeedsTypedPort)
            assert svc.custom_env_port == 9000
            assert isinstance(svc.custom_env_port, int)
        finally:
            Container._instance = None

    def test_env_var_malformed_int_raises_clear(self, test_config_file, monkeypatch):
        from trader.container import ContainerResolutionError

        monkeypatch.setenv('CUSTOM_ENV_PORT', 'not-a-number')
        container = Container.create(test_config_file)
        try:
            with pytest.raises(ContainerResolutionError) as exc:
                container.resolve(_NeedsTypedPort)
            assert 'CUSTOM_ENV_PORT' in str(exc.value)
            assert 'not-a-number' in str(exc.value)
        finally:
            Container._instance = None

    def test_var_args_and_kwargs_are_not_required(self, test_config_file):
        """Classes with ``*args``/``**kwargs`` must resolve without caller
        having to supply them — this is what DataService hit in prod."""
        class _WithKwargs:
            def __init__(self, duckdb_path: str = '', **kwargs):
                self.duckdb_path = duckdb_path
                self.kwargs = kwargs

        class _WithArgs:
            def __init__(self, duckdb_path: str = '', *args):
                self.duckdb_path = duckdb_path
                self.args = args

        container = Container.create(test_config_file)
        try:
            # Both should resolve cleanly — no ContainerResolutionError.
            kw_svc = container.resolve(_WithKwargs)
            assert kw_svc.kwargs == {}
            a_svc = container.resolve(_WithArgs)
            assert a_svc.args == ()
        finally:
            Container._instance = None

    def test_yaml_safe_load_rejects_python_object_tag(self, tmp_path):
        """Container must not allow YAML Python-object tags (arbitrary code
        execution vector)."""
        import yaml
        malicious = tmp_path / 'evil.yaml'
        malicious.write_text(
            'duckdb_path: !!python/object/apply:os.system ["echo pwn"]\n'
        )
        with pytest.raises(yaml.constructor.ConstructorError):
            Container(str(malicious))
        Container._instance = None

