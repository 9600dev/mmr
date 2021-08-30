import aioreactive as rx
import datetime as dt
import asyncio
from asyncio import iscoroutinefunction
from aioreactive import AsyncObserver
from aioreactive.subject import AsyncMultiSubject
from typing import TypeVar, Optional, Callable, Awaitable, Tuple, Generic, Dict
from functools import wraps
from eventkit import Event

from expression.system.disposable import AsyncDisposable


TSource = TypeVar('TSource')
TResult = TypeVar('TResult')
TKey = TypeVar('TKey')
Any = TypeVar('Any')

async def anoop(value: Optional[Any] = None):
    pass

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
        self.capture_ex = capture_asend_exception

    async def asend(self, value: TSource) -> None:
        self._value = value
        self._dt = dt.datetime.now()
        if self.capture_ex:
            try:
                await self._asend(value)
            except Exception as ex:
                await self._athrow(ex)
        else:
            await self._asend(value)

    async def athrow(self, error: Exception) -> None:
        await self._athrow(error)

    async def aclose(self) -> None:
        await self._aclose()

    def value(self) -> Optional[TSource]:
        return self._value

    def dt(self) -> Optional[dt.datetime]:
        return self._dt


class AsyncCachedSubject(AsyncMultiSubject[TSource]):
    def __init__(self):
        super().__init__()
        self.value: Optional[TSource] = None
        self.datetime: Optional[dt.datetime] = None

    async def asend(self, value: TSource) -> None:
        self.check_disposed()

        if self._is_stopped:
            return

        self.value = value
        self.datetime = dt.datetime.now()

        for obv in list(self._observers):
            await obv.asend(value)

    async def subscribe_async(self, observer: AsyncObserver[TSource]) -> AsyncDisposable:
        self.check_disposed()

        self._observers.append(observer)

        async def dispose() -> None:
            if observer in self._observers:
                self._observers.remove(observer)

        result = AsyncDisposable.create(dispose)

        # send the last cached result
        if self.value:
            await observer.asend(self.value)
        return result

    def get_value(self) -> Optional[TSource]:
        return self.value

    def get_value_dt(self) -> Optional[Tuple[TSource, dt.datetime]]:
        if self.value and self.datetime:
            return (self.value, self.datetime)
        else:
            return None


class AsyncEventSubject(AsyncCachedSubject[TSource]):
    def __init__(self, eventkit_event: Event):
        super().__init__()
        self.eventkit_event = eventkit_event
        eventkit_event += self.on_eventkit_update

    async def call_event_subscriber(self, awaitable_event_subscriber: Awaitable[TSource]):
        result = await awaitable_event_subscriber
        # todo this doesn't feel right. I want isinstance(result, TSource) but that doesn't work
        if result:
            await self.asend(result)

    def call_event_subscriber_sync(self, callable_lambda: Callable):
        result = callable_lambda()
        if result:
            test = asyncio.run(self.asend(result))

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

