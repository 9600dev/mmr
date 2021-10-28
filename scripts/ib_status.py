import sys
import os
import datetime as dt
import pytz

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import requests
from typing import List, Dict, Tuple, Callable, Optional, Set, Generic, TypeVar, cast, Union
from bs4 import BeautifulSoup

from trader.common.logging_helper import setup_logging, suppress_external
logging = setup_logging(module_name='ib_status')


def time_in_range(start, end, x):
    if start <= end:
        return start <= x <= end
    else:
        return start <= x or x <= end


def ib_maintenance() -> bool:
    # let's assume that the system availability times on IB stay the same
    tz = pytz.timezone('America/New_York')
    date_time = dt.datetime.now(tz=tz)

    # special case Friday/Sat morning 23:00 - 03:00 ET
    if (date_time.weekday() == 4 or date_time.weekday() == 5) and time_in_range(dt.time(23), dt.time(3), date_time.time()):
        return True
    # Sunday's there are no reboots
    elif (date_time.weekday() == 6):
        return False
    elif (date_time.weekday() == 0) and date_time.time() < dt.time(23):
        return False
    # otherwise, 23:45 - 00:45 ET
    elif time_in_range(dt.time(23, 45), dt.time(0, 45), date_time.time()):
        return True
    else:
        return False


def ib_status() -> bool:
    if os.getenv('TRADER_CHECK') and os.getenv('TRADER_CHECK') == 'False':
        return True
    if ib_maintenance():
        return False
    try:
        soup = BeautifulSoup(requests.get('https://www.interactivebrokers.com/en/index.php?f=2225').content, features='lxml')
        status_nodes = [
            node for node in
            soup.find_all('table', {'class': 'TableOutline'}) if 'System Availability' in node.text  # type: ignore
        ]
        status_nodes += [
            node for node in
            soup.find_all('table', {'class': 'TableOutline'}) if 'Exchange Availability' in node.text  # type: ignore
        ]

        if 'No problems' in soup.text:
            return True
        elif 'System status information currently not available' in soup.text:
            # todo figure out the correct thing to do here. Rarely this shows up, but
            # I'm betting the best thing to do is soldier on like nothing is happening.
            return True
        elif len(status_nodes) >= 1:
            for status in status_nodes:
                color_cell = status.find('td', {'class': 'centeritem'})  # type: ignore
                if color_cell:
                    # aqua color == General info, and should be return "True"
                    # todo, do the rest
                    return str(color_cell['bgcolor']) == '#99cccc' or str(color_cell['bgcolor']) == '#66cc33'
            return False
        else:
            return False
    except Exception as ex:
        logging.error('exception raised when checking interactive brokers status {}'.format(ex))
        return False


def main():
    print(ib_status())


if __name__ == '__main__':
    main()
