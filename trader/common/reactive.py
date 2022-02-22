from abc import abstractmethod
import aioreactive as rx
import datetime as dt
import asyncio
import pandas as pd
import contextlib
from asyncio import iscoroutinefunction
from aioreactive.types import AsyncObserver, AsyncObservable
from aioreactive.subject import AsyncMultiSubject
from typing import TypeVar, Optional, Callable, Awaitable, Tuple, Generic, Dict, cast, List, Union
from functools import wraps
from eventkit import Event, event

from expression.system.disposable import AsyncDisposable


# With aioreactive you subscribe observers to observables

TSource = TypeVar('TSource')
TResult = TypeVar('TResult')
TKey = TypeVar('TKey')
Any = TypeVar('Any')

async def anoop(value: Optional[Any] = None):
    pass

# With aioreactive you subscribe observers to observables
class AsyncCachedObserver(AsyncObserver[TSource]):
    def __init__(self,
                 asend: Callable[[TSource], Awaitable[None]] = anoop,
                 athrow: Callable[[Exception], Awaitable[None]] = anoop,
                 aclose: Callable[[], Awaitable[None]] = anoop,
                 capture_asend_exception: bool = False):
        super().__init__()
        assert iscoroutinefunction(asend)
        self._asend = asend

        assert iscoroutinefunction(athrow)
        self._athrow = athrow

        assert iscoroutinefunction(aclose)
        self._aclose = aclose
        self._value: Optional[TSource] = None
        self._dt: Optional[dt.datetime] = None
        self._capture_ex = capture_asend_exception
        self._task: asyncio.Event = asyncio.Event()

    async def asend(self, value: TSource) -> None:
        self._value = value
        self._dt = dt.datetime.now()
        if self._capture_ex:
            try:
                self._task.set()
                await self._asend(value)
            except Exception as ex:
                self._task.clear()
                await self._athrow(ex)
        else:
            self._task.set()
            await self._asend(value)

    async def athrow(self, error: Exception) -> None:
        self._task.clear()
        await self._athrow(error)

    async def aclose(self) -> None:
        self._task.clear()
        await self._aclose()

    def value(self) -> Optional[TSource]:
        return self._value

    async def wait_value(self, wait_timeout: Optional[float] = None) -> TSource:
        async def event_wait(evt, timeout):
            # suppress TimeoutError because we'll return False in case of timeout
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(evt.wait(), timeout)
            return evt.is_set()

        if self._value:
            return self._value
        else:
            # await self._task.wait()
            if wait_timeout:
                await event_wait(self._task, wait_timeout)
            else:
                await self._task.wait()
            self._task.clear()
            return cast(TSource, self._value)

    def dt(self) -> Optional[dt.datetime]:
        return self._dt


class AsyncCachedObservable(AsyncObservable[TSource]):
    @abstractmethod
    async def subscribe_async(self, observer: AsyncObserver[TSource]) -> AsyncDisposable:
        raise NotImplementedError

    @abstractmethod
    def value(self) -> Optional[TSource]:
        raise NotImplementedError

    @abstractmethod
    def value_dt(self) -> Optional[Tuple[TSource, dt.datetime]]:
        raise NotImplementedError


class AsyncCachedSubject(AsyncMultiSubject[TSource], AsyncCachedObservable[TSource]):
    def __init__(self):
        super().__init__()
        self._value: Optional[TSource] = None
        self._datetime: Optional[dt.datetime] = None
        self._task: asyncio.Event = asyncio.Event()

    async def asend(self, value: TSource) -> None:
        self.check_disposed()

        if self._is_stopped:
            return

        self._task.set()
        self._value = value
        self.datetime = dt.datetime.now()

        for obv in list(self._observers):
            await obv.asend(self._value)

    async def subscribe_async(self, observer: AsyncObserver[TSource]) -> AsyncDisposable:
        self.check_disposed()

        self._observers.append(observer)

        async def dispose() -> None:
            if observer in self._observers:
                self._observers.remove(observer)

        result = AsyncDisposable.create(dispose)

        # send the last cached result
        if self._value:
            await observer.asend(self._value)
        return result

    def value(self) -> Optional[TSource]:
        return self._value

    async def wait_value(self) -> TSource:
        if self._value:
            return self._value
        else:
            await self._task.wait()
            return cast(TSource, self.value)

    def value_dt(self) -> Optional[Tuple[TSource, dt.datetime]]:
        if self._value and self._datetime:
            return (self._value, self._datetime)
        else:
            return None


class AsyncCachedPandasSubject(AsyncCachedSubject[pd.DataFrame]):
    def __init__(self):
        super().__init__()

    async def asend(self, value: pd.DataFrame) -> None:
        self.check_disposed()

        if self._is_stopped:
            return

        self._task.set()
        # if self._value is not None:
        #     self._value = self._value.append(value)
        # else:
        self._value = value
        self.datetime = dt.datetime.now()

        for obv in list(self._observers):
            await obv.asend(self._value)


class AsyncEventSubject(AsyncCachedSubject[TSource]):
    def __init__(self, eventkit_event: Optional[Union[Event, List[Event]]] = None):
        super().__init__()
        self.eventkit_event: List[Event] = []
        if eventkit_event and type(eventkit_event) is list:
            self.eventkit_event = eventkit_event
            for e in eventkit_event:
                e += self.on_eventkit_update
        elif eventkit_event and type(eventkit_event) is Event:
            self.eventkit_event += [eventkit_event]
            e = cast(Event, eventkit_event)
            e += self.on_eventkit_update

    async def subscribe_to_eventkit_event(self, eventkit: Union[List[Event], Event]) -> None:
        if type(eventkit) is Event:
            self.eventkit_event += [eventkit]
            eventkit = cast(Event, eventkit)
            eventkit += self.on_eventkit_update
        elif type(eventkit) is list:
            for e in eventkit:
                e += self.on_eventkit_update

    async def call_event_subscriber(self, awaitable_event_subscriber: Awaitable[TSource]) -> None:
        result = await awaitable_event_subscriber
        # todo this doesn't feel right. I want isinstance(result, TSource) but that doesn't work
        if result:
            await self.asend(result)

    def call_event_subscriber_sync(self, callable_lambda: Callable):
        result = callable_lambda()
        if result:
            print('call_event_subscriber_sync')
            test = asyncio.run(self.asend(result))
            print('done')

    async def call_cancel_subscription(self, awaitable_canceller: Awaitable):
        await awaitable_canceller
        await self.aclose()

    def call_cancel_subscription_sync(self, callable_lambda: Callable):
        callable_lambda()
        asyncio.run(self.aclose())

    async def on_eventkit_update(self, e: TSource, *args):
        await self.asend(e)


def awaitify(sync_func):
    @wraps(sync_func)
    async def async_func(*args, **kwargs):
        return sync_func(*args, **kwargs)
    return async_func


# class SubscribeEventHelper(Generic[TKey, TValue]):
#     def __init__(self, source: AsyncEventSubject[TValue]):
#         self.cache: Dict[TKey, rx.AsyncObservable[TValue]] = {}
#         self.source: AsyncEventSubject[TValue] = source
#         self.subject = AsyncCachedSubject[TValue]()

#     async def subscribe(self, key: TKey,
#                         source: AsyncEventSubject[TValue],
#                         filter_function: Callable[[AsyncObservable[TValue]], AsyncObservable[TValue]]
#                         ) -> rx.AsyncObservable[TValue]:

#         if key in self.cache:
#             return self.cache[key]
#         else:
#             # call, then attach a filter to the result
#             await source.subscribe_async(self.subject)

#             xs = pipe(
#                 self.subject,
#                 filter_function
#             )
#             self.cache[key] = xs
#             return self.cache[key]

#     async def on_event_update(self, event_update: TValue):
#         await self.subject.asend(event_update)

