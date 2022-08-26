from typing import List, Optional

import datetime as dt


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
