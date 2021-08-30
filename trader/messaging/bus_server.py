import os
import sys
import lightbus
import asyncio
import signal
import nest_asyncio
from lightbus import BusPath
from lightbus.commands import utilities as command_utilities
from lightbus.commands import lightbus_entry_point, run_command_from_args, parse_args, load_config
from lightbus.creation import ThreadLocalClientProxy
from lightbus.utilities.features import Feature, ALL_FEATURES


async def _handle(args, config):
    bus: BusPath
    bus_module, bus = command_utilities.import_bus(args)

    if isinstance(bus.client, ThreadLocalClientProxy):
        bus.client.disable_proxy()

    bus.client.set_features(ALL_FEATURES)  # type: ignore
    restart_signals = (signal.SIGINT, signal.SIGTERM)

    # Handle incoming signals
    async def signal_handler():
        # Stop handling signals now. If we receive the signal again
        # let the process quit naturally
        for signal_ in restart_signals:
            asyncio.get_event_loop().remove_signal_handler(signal_)

        bus.client.request_shutdown()

    for signal_ in restart_signals:
        asyncio.get_event_loop().add_signal_handler(
            signal_, lambda: asyncio.ensure_future(signal_handler())
        )

    await bus.client.start_worker()


def start_lightbus(lightbus_config_file: str, bus_py_location: str, loop):
    # there's a lot that relies on this arg command line format,
    # so we'll keep it for now, but will need to clean it up later
    args = ['bus_server.py', 'run', '--config', lightbus_config_file, '--log-level', 'debug']
    os.environ['LIGHTBUS_MODULE'] = bus_py_location

    old_argv = sys.argv
    sys.argv = args
    parsed_args = parse_args()
    sys.argv = old_argv
    config = None

    if hasattr(parsed_args, "config_file"):
        config = load_config(parsed_args)

    loop.run_until_complete(_handle(parsed_args, config))
