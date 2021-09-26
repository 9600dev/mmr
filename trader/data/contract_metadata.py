from ib_insync.contract import Contract
from typing import Tuple, List, Optional, Dict, TypeVar, Generic, Type, Union, cast
import datetime as dt

class ContractMetadata():
    def __init__(self,
                 contract: Contract,
                 history_no_data_dates: List[dt.datetime],
                 history_overlapping_data_dates: List[dt.datetime]):
        self.contract = contract
        self.history_no_data_dates = history_no_data_dates
        self.history_overlapping_data = history_overlapping_data_dates

    def to_dict(self):
        return vars(self)

    def add_no_data(self, date_time: dt.datetime):
        self.history_no_data_dates.append(date_time)

    def add_overlapping_data(self, date_time: dt.datetime):
        self.history_overlapping_data.append(date_time)

    def has_been_crawled(self, date_time: dt.datetime):
        return date_time in self.history_no_data_dates or date_time in self.history_overlapping_data

    def __str__(self):
        return '{} {} {}'.format(self.contract, self.history_no_data_dates, self.history_overlapping_data)
