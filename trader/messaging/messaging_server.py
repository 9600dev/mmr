import sys
import os

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import click
import logging
import coloredlogs
import asyncio

from asyncio import AbstractEventLoop
from rx.subject import Subject
from trader.messaging.messaging_client import MessagingSubscriber, MessagingPublisher
from typing import Dict

class MessagingServer():
    def __init__(self,
                 subscribe_ip_address: str,
                 subscribe_port: int,
                 publisher_ip_address: str,
                 publisher_port: int,
                 loop: AbstractEventLoop):

        self.subject: Subject = Subject()

        self.publisher = MessagingSubscriber(subscribe_ip_address=publisher_ip_address,
                                             subscribe_port=publisher_port,
                                             loop=loop)

        self.subscribers = MessagingPublisher(publish_ip_address=subscribe_ip_address,
                                              publish_port=subscribe_port,
                                              loop=loop)

        self.loop: AbstractEventLoop = loop

    async def start(self):
        def pretty_print(message: Dict):
            result = ''.join(['{0}: {1}, '.format(k, v) for k, v in message.items()])
            return result[:100] + ' ...'

        done = asyncio.Future()
        logging.info('starting...')

        # subscribe to the publisher, and push messages to the subscribers
        disposable = self.publisher.subscribe(self.subscribers)
        # write to the console for now
        self.publisher.subscribe(on_next=lambda message: logging.info(pretty_print(message)))

        logging.info('started.')

        await done
        disposable.dispose()


@click.command()
@click.option('--subscribe_ipaddress', default='127.0.0.1', required=False, help='ip to bind to')
@click.option('--subscribe_port', default=5002, required=False, help='port for subscribers')
@click.option('--publisher_ipaddress', default='127.0.0.1', required=False, help='ip address of publisher')
@click.option('--publisher_port', default=5001, required=False, help='port for publisher')
def main(subscribe_ipaddress: str,
         subscribe_port: int,
         publisher_ipaddress: str,
         publisher_port: int):
    logging.info('starting messaging_server subscribe_port: {} publisher_port: {}'.format(subscribe_port, publisher_port))

    loop = asyncio.get_event_loop()
    messaging_server = MessagingServer(subscribe_ipaddress,
                                       subscribe_port,
                                       publisher_ipaddress,
                                       publisher_port,
                                       loop)

    loop.run_until_complete(messaging_server.start())


if __name__ == '__main__':
    coloredlogs.install(level='INFO')
    main()
