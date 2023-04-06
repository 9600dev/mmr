from enum import Enum
from eventkit import Event
from functools import wraps
from reactivex.disposable import Disposable
from reactivex.observable import Observable
from reactivex.observer import Observer
from reactivex.subject import Subject
from typing import Awaitable, Callable, cast, Generic, List, Optional, TypeVar, Union

import asyncio
import functools
import reactivex.abc as abc


# With reactivex you subscribe observers to observables
TSource = TypeVar('TSource')
TResult = TypeVar('TResult')
TKey = TypeVar('TKey')
Any = TypeVar('Any')


class SuccessFailEnum(Enum):
    SUCCESS = 0
    FAIL = 1

    def __str__(self):
        if self.value == 0: return 'SUCCESS'
        if self.value == 1: return 'FAIL'


class AnonymousObserver(Observer[TSource]):
    def __init__(
        self,
        on_next: Optional[Callable[[TSource], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        on_completed: Optional[Callable[[], None]] = None
    ):
        self._on_next = on_next
        self._on_error = on_error
        self._on_completed = on_completed

    def on_next(self, value: TSource):
        if self._on_next is not None:
            self._on_next(value)

    def on_error(self, error: Exception):
        if self._on_error is not None:
            self._on_error(error)

    def on_completed(self):
        if self._on_completed is not None:
            self._on_completed()


class PrintObserver(AnonymousObserver):
    def __init__(self, prefix: str = ''):
        super().__init__(
            on_next=lambda x: print(prefix, x),
            on_error=lambda x: print(prefix, x),
            on_completed=lambda: print(prefix, 'completed')
        )


class SuccessFail(Generic[TSource]):
    def __init__(self, success_fail: SuccessFailEnum, error=None, exception=None, obj: Optional[TSource] = None, disposable=None):
        self.success_fail = success_fail
        self.error = error
        self.exception = exception
        self.obj: Optional[TSource] = obj
        self.disposable = disposable

    @staticmethod
    def success(obj: Optional[TSource] = None):
        return SuccessFail(SuccessFailEnum.SUCCESS, obj=obj)

    @staticmethod
    def fail(error=None, exception=None):
        return SuccessFail(SuccessFailEnum.FAIL, error=error, exception=exception)

    def __str__(self):
        return '{}: obj: {}, error: {}'.format(self.success_fail, self.obj, self.error)

    def __repr__(self):
        return self.__str__()

    def is_success(self):
        return self.success_fail == SuccessFailEnum.SUCCESS

_T_out = TypeVar('_T_out')

class SuccessFailObservable(Observable[SuccessFail], Disposable):
    def __init__(self, success_fail: Optional[SuccessFail] = None):
        super().__init__()
        self.success_fail = success_fail
        self.is_disposed: bool = False
        self.observer: Optional[Observer] = None

    def success(self):
        if not self.is_disposed and self.observer:
            # self.observer.on_next(SuccessFail(SuccessFailEnum.SUCCESS))
            # self.observer.on_completed()
            self.observer.on_next(SuccessFail(SuccessFailEnum.SUCCESS))
            self.observer.on_completed()

    def failure(self, success_fail: SuccessFail):
        if not self.is_disposed and self.observer:
            self.observer.on_next(success_fail)
            self.observer.on_completed()

    def subscribe(
        self,
        on_next: Optional[
            Union[abc.ObserverBase[SuccessFail], abc.OnNext[SuccessFail], None]
        ] = None,
        on_error: Optional[abc.OnError] = None,
        on_completed: Optional[abc.OnCompleted] = None,
        *,
        scheduler: Optional[abc.SchedulerBase] = None,
    ) -> abc.DisposableBase:

        def disposer() -> None:
            self.observer = None

        if self.success_fail:
            if isinstance(on_next, abc.ObservableBase):
                on_next.on_next(self.success_fail)
                on_next.on_completed()
            if on_next: on_next(self.success_fail)  # type: ignore
            if on_completed: on_completed()

            return Disposable()
        else:
            self.observer = on_next  # type: ignore
            return Disposable(disposer)


class EventSubject(Subject[TSource]):
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

    async def call_event_subscriber(self, awaitable_event_subscriber: Awaitable[TSource], asend_result: bool = True):
        result = await awaitable_event_subscriber
        # todo this doesn't feel right. I want isinstance(result, TSource) but that doesn't work
        if result and asend_result:
            self.on_next(result)
            return result
        if result:
            return result

    def call_event_subscriber_sync(self, callable_lambda: Callable, asend_result: bool = True) -> Optional[TSource]:
        result = callable_lambda()
        if result and asend_result:
            self.on_next(result)
            return result
        if result:
            return result
        return None

    async def call_cancel_subscription(self, awaitable_canceller: Awaitable):
        await awaitable_canceller
        self.on_completed()

    def call_cancel_subscription_sync(self, callable_lambda: Callable):
        callable_lambda()
        self.on_completed()

    async def on_eventkit_update(self, e: TSource, *args):
        self.on_next(e)


def awaitify(sync_func):
    @wraps(sync_func)
    async def async_func(*args, **kwargs):
        return sync_func(*args, **kwargs)
    return async_func


class ObservableIterHelper():
    def __init__(self, loop):
        self.loop = loop

    def from_aiter(self, iter):
        from reactivex import create

        def on_subscribe(observer, scheduler):
            async def _aio_sub():
                try:
                    async for i in iter:
                        observer.on_next(i)
                    self.loop.call_soon(observer.on_completed)
                except Exception as e:
                    self.loop.call_soon(functools.partial(observer.on_error, e))

            task = asyncio.ensure_future(_aio_sub(), loop=self.loop)
            return Disposable(lambda: task.cancel())  # type: ignore

        return create(on_subscribe)


