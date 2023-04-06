from trader.common.logging_helper import get_callstack, log_method, setup_logging
from typing import cast, List, Optional, TYPE_CHECKING


if TYPE_CHECKING:
    from trader.trading.trading_runtime import Trader

import datetime as dt


logging = setup_logging(module_name='trading_runtime')


def trader_exception(trader: 'Trader', exception_type: type, message: str, inner: Optional[Exception] = None) -> Exception:
    # todo use reflection here to automatically populate trader runtime vars that we care about
    # given a particular exception type
    data = trader.data if hasattr(trader, 'data') else None
    client = trader.client.is_connected() if hasattr(trader, 'client') else False
    last_connect_time = trader.last_connect_time if hasattr(trader, 'last_connect_time') else dt.datetime.min

    exception = exception_type(
        data is not None,
        client,
        trader.startup_time,
        last_connect_time,
        message,
        inner,
        get_callstack(10)
    )
    logging.exception(exception)
    return cast(Exception, exception)


class TraderException(Exception):
    def __init__(
        self,
        message: str,
        arctic_connected: bool,
        ib_connected: bool,
        startup_time: dt.datetime,
        last_connect_time: dt.datetime,
        inner: Optional[Exception] = None,
        call_stack: Optional[List[str]] = None,
    ):
        super().__init__(message, arctic_connected, ib_connected, startup_time, last_connect_time, inner, call_stack)
        self.message = message
        self.arctic_connected = arctic_connected
        self.ib_connected = ib_connected
        self.startup_time = startup_time
        self.last_connect_time = last_connect_time
        self.inner = inner
        self.call_stack = call_stack

    def __str__(self):
        builder = '{}\nstartup_time: {}\nlast_connect_time: {}\narctic_connected: {}\nib_connected: {}\n'.format(
            self.message, self.startup_time, self.last_connect_time, self.ib_connected, self.arctic_connected
        )
        if self.call_stack:
            builder += 'call_stack:\n'
            for line in self.call_stack:
                builder += '    {}\n'.format(line)
        if self.inner:
            builder += 'inner_exception:\n'
            builder += '    {}: {}'.format(str(type(self.inner)), str(self.inner))
        return builder

class TraderConnectionException(TraderException):
    def __init__(
        self,
        message: str,
        arctic_connected: bool,
        ib_connected: bool,
        startup_time: dt.datetime,
        last_connect_time: dt.datetime,
        inner: Optional[Exception] = None,
        call_stack: Optional[List[str]] = None,
    ):
        super().__init__(
            message,
            arctic_connected,
            ib_connected,
            startup_time,
            last_connect_time,
            inner,
            call_stack
        )
