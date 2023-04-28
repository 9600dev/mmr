from prompt_toolkit.history import FileHistory

import click
import click_repl
import cloup
import configobj
import functools
import os
import yaml


__all__ = ('configobj_provider', 'configuration_option')

class configobj_provider:
    """
    A parser for configobj configuration files

    Parameters
    ----------
    unrepr : bool
        Controls whether the file should be parsed using configobj's unrepr
        mode. Defaults to `True`.
    section : str
        If this is set to something other than the default of `None`, the
        provider will look for a corresponding section inside the
        configuration file and return only the values from that section.
    """

    def __init__(self, unrepr=True, section=None):
        self.unrepr = unrepr
        self.section = section

    def __call__(self, file_path, cmd_name):
        """
        Parse and return the configuration parameters.

        Parameters
        ----------
        file_path : str
            The path to the configuration file
        cmd_name : str
            The name of the click command

        Returns
        -------
        dict
            A dictionary containing the configuration parameters.
        """
        config = configobj.ConfigObj(file_path, unrepr=self.unrepr)
        if self.section:
            config = config[self.section] if self.section in config else {}
        return config


def configuration_callback(cmd_name, option_name, config_file_name,
                           saved_callback, provider, implicit, ctx,
                           param, value):
    """
    Callback for reading the config file.

    Also takes care of calling user specified custom callback afterwards.

    cmd_name : str
        The command name.
        This is used to determine the configuration directory.
    option_name : str
        The name of the option. This is used for error messages.
    config_file_name : str
        The name of the configuration file.
    saved_callback: callable
        User-specified callback to be called later.
    provider : callable
        A callable that parses the configuration file and returns a dictionary
        of the configuration parameters. Will be called as
        `provider(file_path, cmd_name)`. Default: `configobj_provider()`
    implicit : bool
        Whether a implicit value should be applied if no configuration option
        value was provided.
        Default: `False`
    ctx : object
        Click context.
    """
    ctx.default_map = ctx.default_map or {}
    cmd_name = cmd_name or ctx.info_name

    if implicit:
        default_value = os.path.join(
            click.get_app_dir(cmd_name), config_file_name)
        param.default = default_value
        value = value or default_value

    if value:
        try:
            config = provider(value, cmd_name)
            ctx.default_map.update(config)
        except Exception as e:
            pass

    return saved_callback(ctx, param, value) if saved_callback else value


def default_config(*param_decls, **attrs):
    """
    Adds configuration file support to a click application.

    This will create an option of type `click.File` expecting the path to a
    configuration file. When specified, it overwrites the default values for
    all other click arguments or options with the corresponding value from the
    configuration file.

    The default name of the option is `--config`.

    By default, the configuration will be read from a configuration directory
    as determined by `click.get_app_dir`.

    This decorator accepts the same arguments as `click.option` and
    `click.Path`. In addition, the following keyword arguments are available:

    cmd_name : str
        The command name. This is used to determine the configuration
        directory. Default: `ctx.info_name`
    config_file_name : str
        The name of the configuration file. Default: `'config'``
    implicit: bool
        If 'True' then implicitly create a value for the configuration option
        using the above parameters. If a configuration file exists in this
        path it will be applied even if no configuration option was suppplied
        as a CLI argument or environment variable.
        If 'False` only apply a configuration file that has been explicitely
        specified.
        Default: `False`
    provider : callable
        A callable that parses the configuration file and returns a dictionary
        of the configuration parameters. Will be called as
        `provider(file_path, cmd_name)`. Default: `configobj_provider()`
        """
    param_decls = param_decls or ('--config', )
    option_name = param_decls[0]

    def decorator(f):

        attrs.setdefault('is_eager', True)
        attrs.setdefault('help', 'Read configuration from FILE.')
        attrs.setdefault('expose_value', False)
        implicit = attrs.pop('implicit', True)
        cmd_name = attrs.pop('cmd_name', None)
        config_file_name = attrs.pop('config_file_name', 'config')
        provider = default_config_provider  # attrs.pop('provider', configobj_provider())
        path_default_params = {
            'exists': False,
            'file_okay': True,
            'dir_okay': False,
            'writable': False,
            'readable': True,
            'resolve_path': False
        }
        path_params = {
            k: attrs.pop(k, v)
            for k, v in path_default_params.items()
        }
        attrs['type'] = attrs.get('type', click.Path(**path_params))
        saved_callback = attrs.pop('callback', None)
        partial_callback = functools.partial(
            configuration_callback, cmd_name, option_name,
            config_file_name, saved_callback, provider, implicit)
        attrs['callback'] = partial_callback
        return click.option(*param_decls, **attrs)(f)

    return decorator


def default_config_provider(file_path, cmd_name):
    config_file = file_path

    if os.getenv('TRADER_CONFIG'):
        config_file = str(os.getenv('TRADER_CONFIG'))  # type: ignore

    if not os.path.exists(config_file) and os.path.exists('configs/trader.yaml'):
        config_file = 'configs/trader.yaml'

    if os.path.exists(config_file):  # type: ignore
        conf_file = open(config_file, 'r')
        return yaml.load(conf_file, Loader=yaml.FullLoader)


def common_options():
    def inner_func(function):
        function = click.option(
            '--zmq_pubsub_server_address',
            help='zero mq publish/subscribe ticker update server address, eg: 127.0.0.1',
            hidden=True,
        )(function)
        function = click.option(
            '--zmq_pubsub_server_port',
            help='zero mq publish/subscribe ticker update server port, eg: 42002',
            hidden=True,
        )(function)
        function = click.option(
            '--zmq_rpc_server_address',
            help='zero mq rpc server address, eg: 127.0.0.1',
            hidden=True,
        )(function)
        function = click.option(
            '--zmq_rpc_server_port',
            help='zero mq rpc server port, eg: 42001',
            hidden=True,
        )(function)
        function = click.option(
            '--arctic_universe_library',
            help='arctic library that describes securities universes, eg: Universes',
            hidden=True,
        )(function)
        function = click.option(
            '--arctic_server_address',
            help='arctic server address, eg: 127.0.0.1',
            hidden=True,
        )(function)
        function = click.option(
            '--redis_server_port',
            help='redis server port, eg: 6379',
            hidden=True,
        )(function)
        function = click.option(
            '--redis_server_address',
            help='redis server address, eg: 127.0.0.1',
            hidden=True,
        )(function)
        function = click.option(
            '--ib_server_port',
            default=7496,
            help='tws trader API address, eg: 7496',
            hidden=True,
        )(function)
        function = click.option(
            '--ib_server_address',
            required=True,
            help='tws trader instance address, eg: 127.0.0.1',
            hidden=True,
        )(function)
        return function
    return inner_func


REPL_CONTEXT_SETTINGS = cloup.Context.settings(
    # parameters of Command:
    align_option_groups=False,
    align_sections=True,
    show_constraints=True,
    # parameters of HelpFormatter:
    formatter_settings=cloup.HelpFormatter.settings(
        max_width=120,
        col1_max_width=25,
        col2_min_width=30,
        indent_increment=3,
        col_spacing=3,
        theme=cloup.HelpTheme.light(),
    )
)


@cloup.group(
    invoke_without_command=True,
    context_settings=REPL_CONTEXT_SETTINGS,
)
@cloup.pass_context
def cli(ctx):
    pass


@cloup.group(
    invoke_without_command=False,
    context_settings=REPL_CONTEXT_SETTINGS,
)
@cloup.pass_context
def cli_norepl(ctx):
    pass
