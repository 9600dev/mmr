from abc import abstractmethod
from trader.common.helpers import best_fit_distribution, fit_distribution
from typing import Callable, cast, Dict, List, Optional

import numpy as np
import pandas as pd
import scipy.stats as st


class Distribution():
    def __init__(self, name: str, cache_size: int = 100000):
        self.name = name
        self.cache_size = cache_size

    @abstractmethod
    def sample(self) -> float:
        pass

class TestDistribution(Distribution):
    def __init__(self,
                 name: str,
                 csv_file: str,
                 cache_size: int = 365 * 10):
        super().__init__(name=name, cache_size=cache_size)
        self.cache = pd.read_csv(csv_file)['output'].tolist()
        self.cache_index = 0

    def sample(self) -> float:
        ret = self.cache[self.cache_index] / 100.0
        self.cache_index = self.cache_index + 1
        if self.cache_index >= len(self.cache):
            self.cache_index = 0
        return ret

class CsvContinuousDistribution(Distribution):
    def __init__(self,
                 name: str,
                 csv_file: str,
                 data_column: str,
                 cache_size: int = 365 * 10,
                 data_column_apply: Optional[Callable[[pd.DataFrame, str], pd.Series]] = None,
                 distribution: Optional[st.rv_continuous] = None):
        super().__init__(name=name, cache_size=cache_size)
        self.csv_file: str = csv_file
        self.data_column: str = data_column
        self.cache_index: int = 0
        self.cache: List[float] = []
        self.data_column_apply: Optional[Callable[[pd.DataFrame, str], pd.Series]] = data_column_apply
        self.distribution: Optional[st.rv_continuous] = distribution
        self.init()

    dist_singleton_cache: Dict[str, Distribution] = {}

    def init(self):
        # if we're reusing the same csv file, let's fastpath the creation of the distribution
        if self.csv_file in CsvContinuousDistribution.dist_singleton_cache:
            d: CsvContinuousDistribution = \
                cast(CsvContinuousDistribution, CsvContinuousDistribution.dist_singleton_cache[self.csv_file])
            if self.distribution == d.distribution and self.data_column_apply == d.data_column_apply:
                self.dist_x = d.dist_x
                self.dist_pdf = d.dist_pdf
                self.populate_cache()
                return

        df = pd.read_csv(self.csv_file)

        if self.data_column_apply:
            df[self.data_column] = self.data_column_apply(df, self.data_column)

        if not self.distribution:
            self.distribution, params = best_fit_distribution(df[self.data_column], bins=2000)

        x, pdf, params = fit_distribution(df[self.data_column], self.distribution, bins=2000)
        self.dist_x = x
        self.dist_pdf = pdf / pdf.sum()
        self.populate_cache()
        CsvContinuousDistribution.dist_singleton_cache[self.csv_file] = self

    def populate_cache(self):
        self.cache_index = 0
        self.cache = np.random.choice(self.dist_x, size=self.cache_size, p=self.dist_pdf)

    def sample(self) -> float:
        self.cache_index = self.cache_index + 1
        if self.cache_index >= self.cache_size:
            self.populate_cache()

        return self.cache[self.cache_index]
