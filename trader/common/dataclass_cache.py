from reactivex import Observer, Subject
from reactivex.abc import SubjectBase
from reactivex.abc.disposable import DisposableBase
from reactivex.abc.observer import ObserverBase, OnCompleted, OnError, OnNext
from reactivex.abc.scheduler import SchedulerBase
from reactivex.disposable import Disposable
from reactivex.observable import Observable
from typing import Callable, Dict, Generic, List, Optional, TypeVar, Union

import datetime as dt


_T = TypeVar('_T')


class DataClassEvent(Generic[_T]):
    pass


class UpdateEvent(DataClassEvent[_T]):
    def __init__(self, item: _T):
        self.item = item


class RemoveEvent(DataClassEvent[_T]):
    def __init__(self, key: str):
        self.key = key


class DataClassCache(Generic[_T], SubjectBase[DataClassEvent[_T]]):
    def __init__(self, key_lookup: Callable[[_T], str]):
        self.cache: Dict[str, _T] = {}
        self.cache_last_updated: Dict[str, dt.datetime] = {}
        self.key_lookup = key_lookup
        self._completed = False
        self.subject = Subject[DataClassEvent[_T]]()

    def update(self, item: _T) -> None:
        self.cache.update({self.key_lookup(item): item})
        self.cache_last_updated.update({self.key_lookup(item): dt.datetime.now()})
        self.on_next(UpdateEvent(item))

    def remove(self, key: str) -> None:
        self.cache.pop(key)
        self.cache_last_updated.pop(key)
        self.on_next(RemoveEvent(key))

    def completed(self):
        self._completed = True
        self.on_completed()

    def exists(self, key: _T) -> bool:
        return self.key_lookup(key) in self.cache

    def get_all(self) -> List[_T]:
        return list(self.cache.values())

    def post_all(self) -> None:
        for item in self.cache.values():
            self.on_next(UpdateEvent(item))

    def create_observer(self, error_func) -> Observer[_T]:
        return Observer(
            on_next=self.update,
            on_completed=self.completed,
            on_error=error_func
        )

    def subscribe(
        self,
        on_next: Optional[Union[OnNext[DataClassEvent[_T]], ObserverBase[DataClassEvent[_T]]]] = None,
        on_error: Optional[OnError] = None,
        on_completed: Optional[OnCompleted] = None,
        *,
        scheduler: Optional[SchedulerBase] = None,
    ) -> DisposableBase:
        return self.subject.subscribe(on_next, on_error, on_completed, scheduler=scheduler)

    def on_next(self, value: DataClassEvent[_T]) -> None:
        return self.subject.on_next(value)

    def on_error(self, error: Exception) -> None:
        return self.subject.on_error(error)

    def on_completed(self) -> None:
        return self.subject.on_completed()
