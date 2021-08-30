import datetime as dt
import numpy as np
from life_simulator import Book, LifeSimulator, AssetCash, AssetStock

def test_cash():
    simulator = LifeSimulator(dt.datetime(1980, 1, 1), 1000000.0)

    book = Book(dt.datetime.now())
    book.assets.append(
        AssetCash(name='term deposit',
                  initial_value=1000000.0,
                  asset_init_date=dt.datetime(2020, 1, 1)))

    book = simulator.run_simulation(book,
                                    simulation_start_date=dt.datetime(2020, 1, 1),
                                    simulation_end_date=dt.datetime(2040, 1, 1))

    cash = book.assets[0]
    assert round(cash.ticks[365].value) == 1027923
    assert round(cash.ticks[-1].value) == 1651618


def test_stock():
    simulator = LifeSimulator(dt.datetime(1980, 1, 1), 1000000.0)

    nw = []

    for i in range(0, 50):
        book = Book(dt.datetime.now())
        book.assets.append(
            AssetCash(name='cash',
                      initial_value=100000.0,
                      asset_init_date=dt.datetime(2020, 1, 1)))
        book.assets.append(
            AssetStock(name='stocks',
                       initial_value=1000000.0,
                       initial_price=1.0,
                       asset_init_date=dt.datetime(2020, 1, 1)))

        book = simulator.run_simulation(book,
                                        simulation_start_date=dt.datetime(2020, 1, 1),
                                        simulation_end_date=dt.datetime(2040, 1, 1))

        nw.append(book.calculate_net_worth())

    assert np.average(nw) > 1000000.0
    assert np.average(nw) < 100000000.0
