from re import I, sub
import zmq
import rx
import click
import logging
import coloredlogs
import asyncio
import random
import functools
import json

from asyncio import AbstractEventLoop
from bson import json_util
from rx import operators as ops
from rx import Observable
from rx.subject import Subject
from rx.scheduler import ThreadPoolScheduler, CatchScheduler, CurrentThreadScheduler
from rx.scheduler.periodicscheduler import PeriodicScheduler
from rx.scheduler.eventloop import AsyncIOThreadSafeScheduler
from rx.core.typing import Observer, Scheduler, OnNext, OnError, OnCompleted
from rx.disposable import Disposable

# from zmq.sugar.context import Context
from zmq.sugar.socket import Socket
from zmq.asyncio import Context, Poller

from typing import Optional, Union, Dict
from trader.common.helpers import from_aiter


class MessagingPublisher(Observer):
    def __init__(self,
                 publish_ip_address: str = '127.0.0.1',
                 publish_port: int = 5001,
                 loop: AbstractEventLoop = None):
        super(Observer, self).__init__()
        self.publish_ip_address = publish_ip_address
        self.publish_port = publish_port
        self.loop = loop

        self.publish_context = Context()
        self.publish_socket = self.publish_context.socket(zmq.PUB)  # type: ignore
        self.publish_socket.bind('tcp://{}:{}'.format(self.publish_ip_address, self.publish_port))

    def on_next(self, message: Dict):
        if not type(message) == dict:
            raise ValueError('message should be of type dict')

        json_message = json.dumps(message, default=json_util.default)
        self.publish_socket.send_string(json_message)

    def on_completed(self):
        logging.info('MessagingPublisher completed')

    def on_error(self, error):
        logging.error(error)


class MessagingSubscriber(Observable):
    def __init__(self,
                 subscribe_ip_address: str = '127.0.0.1',
                 subscribe_port: int = 5002,
                 loop: AbstractEventLoop = None):
        super(Observable, self).__init__()
        self.subscribe_ip_address = subscribe_ip_address
        self.subscribe_port = subscribe_port
        self.loop = loop

        self.subscribe_context = Context()
        self.subscribe_socket = self.subscribe_context.socket(zmq.SUB)  # type: ignore
        self.subscribe_socket.connect('tcp://{}:{}'.format(self.subscribe_ip_address, self.subscribe_port))
        self.subscribe_socket.setsockopt_string(zmq.SUBSCRIBE, '')  # type: ignore
        self.finished = False
        self.disposable: rx.core.typing.Disposable

    async def listen_publisher(self):
        while not self.finished:
            json_message = await self.subscribe_socket.recv_string()
            message = json.loads(json_message, object_hook=json_util.object_hook)
            yield message

    def subscribe(self,
                  observer: Optional[Union[Observer, OnNext]] = None,
                  on_error: Optional[OnError] = None,
                  on_completed: Optional[OnCompleted] = None,
                  on_next: Optional[OnNext] = None,
                  *,
                  scheduler: Optional[Scheduler] = None) -> rx.core.typing.Disposable:
        disposable = from_aiter(self.listen_publisher(), self.loop)
        if observer:
            self.disposable = disposable.subscribe(observer=observer, scheduler=scheduler)
        else:
            self.disposable = disposable.subscribe(on_next=on_next,
                                                   on_error=on_error,
                                                   on_completed=on_completed,
                                                   scheduler=scheduler)
        return self.disposable

    def dispose(self):
        self.disposable.dispose()

def test(loop):
    async def main(loop):
        done = asyncio.Future()
        subscriber = MessagingSubscriber(loop=loop)
        subscriber.subscribe(on_next=lambda message: print(message))
        await done

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))

