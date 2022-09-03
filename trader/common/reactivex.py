from enum import Enum
from eventkit import Event
from functools import wraps
from reactivex.disposable import Disposable
from reactivex.observable import Observable
from reactivex.observer import Observer
from reactivex.subject import Subject
from typing import Awaitable, Callable, cast, List, Optional, TypeVar, Union

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


class SuccessFail():
    def __init__(self, success_fail: SuccessFailEnum, error=None, exception=None, obj=None, disposable=None):
        self.success_fail = success_fail
        self.error = error
        self.exception = exception
        self.obj = obj
        self.disposable = disposable

    @staticmethod
    def success():
        return SuccessFail(SuccessFailEnum.SUCCESS)

    def __str__(self):
        return '{}: obj: {}, error: {}'.format(self.success_fail, self.obj, self.error)


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

    def call_event_subscriber_sync(self, callable_lambda: Callable, asend_result: bool = True):
        result = callable_lambda()
        if result and asend_result:
            self.on_next(result)
            return result
        if result:
            return result

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
