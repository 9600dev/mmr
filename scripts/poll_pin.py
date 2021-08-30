import sys
import os

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import pyautogui as pag
import time
from ewmh import EWMH
import pandas as pd
import datetime as dt
import click

from typing import List, Dict, Tuple, Callable, Optional, Set, Generic, TypeVar, cast, Union
from trader.common.listener_helpers import Helpers
from trader.common.logging_helper import setup_logging

logging = setup_logging(module_name='poll_pin')

def poll_and_set_pin(ewmh: EWMH, window_name: str, png_location: str, pin: str):
    x = 0
    y = 0
    for e in ewmh.getClientList():
        if window_name in e.get_wm_name():
            new_width = e.get_geometry().width
            ewmh.setMoveResizeWindow(e, 0, x, y, e.get_geometry().width, e.get_geometry().height)
            ewmh.setWmState(e, 1, '_NET_WM_STATE_ABOVE')
            ewmh.display.flush()
            x = new_width
    box = pag.locateOnScreen(png_location)
    if box:
        logging.info('found IBKR pin box, clicking')
        pag.click(x=box.left, y=box.top)
        logging.info('typing IBKR pin')
        pag.typewrite(pin + '\n')
        logging.info('done')

@click.command()
@click.option('--title', required=False, default='IBKR', help='window title of pin entry window')
@click.option('--png_location', required=False, default='../configs/pin.png', help='png file for window id')
@click.option('--pin', required=False, default='0000', help='pin')
def main(title, png_location, pin):
    ewmh = EWMH()
    while True:
        poll_and_set_pin(ewmh, title, png_location, pin)
        time.sleep(15)


if __name__ == '__main__':
    main()
