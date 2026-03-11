# Make Me Rich!
![MMR logo from stable diffusion](docs/2022-11-17-14-43-24.png)

Python based algorithmic trading platform for [Interactive Brokers](https://interactivebrokers.com), similar to [QuantRocket](https://www.quantrocket.com) and others. It uses the Interactive Brokers brokerage API's to download historical data, and route/execute its trades.

You can use MMR in three ways:

1. Complete automated and algorithmic trading: MMR will subscribe to instrument ticks, pipe them to your algo, and you provide the signal for automatic trading.
2. Fully interactive terminal prompt: get quotes, create, cancel, change orders etc from a live terminal prompt.
2. Command line trading: get quotes, create, cancel and change orders etc, all from the Windows, MacOS or Linux command line.

MMR connects to Interactive Brokers via IB Gateway, maintains connection, ensures consistency and reliability between your Interactive Brokers account and your local trading book. It's opinionated about the programming abstractions you should use to program your algos, but will meet you at the level of abstraction you want.

It relies on:

* [RxPy 4.0](https://github.com/ReactiveX/RxPY) for asyncio pipelining and programming abstraction.
* Batch download of historical instrument data from [Interactive Brokers](https://www.interactivebrokers.com/en/home.php).
* [DuckDB](https://duckdb.org/) for tick data storage and retrieval.
* [ib_async](https://github.com/ib-api-reloaded/ib_async) — async wrapper around the TWS API.
* No fancy Web x.x technologies, just simple and easily extended Python services.
* Docker two-container model: IB Gateway + MMR.
* Python >= 3.12.

### Status

- [x] Basic technical architecture completed (asyncio, caching, db layer, backtesting, historical data collection, messaging etc)
- [x] Interactive brokers historical data collection
- [x] Login; logoff; get positions; get portfolio;
- [x] Subscribe to bars, subscribe to ticks
- [x] Place, cancel, update orders for all Interactive Brokers instruments
- [x] Stop loss, trailing stop loss
- [ ] Backtesting
- [ ] Algorithmic Strategy API and extensibility hooks (started)
- [ ] Strategy and portfolio risk analysis (started)
- [x] Add/remove strategies
- [ ] Hyperparameter search on strategies

## Want to Learn About Finance?

[Follow along here](docs/finance_notes/INDEX.md) as I take notes from books and web resources. Macro, micro, market structure, accounting, pricing, market making and trading systems.

I lean heavily on the following books for the design of this trading system, and my own algorithmic trading:

[<img src="docs/2022-11-16-15-30-42.png" height=300>](https://www.amazon.com/Systematic-Trading-designing-trading-investing/dp/0857194453)
[<img src="docs/2022-11-16-15-32-22.png" height=300>](https://www.amazon.com/Advances-Financial-Machine-Learning-Marcos/dp/1119482089)

## Installation

### Docker Installation

The simplest way to install and run MMR is via Docker. It uses a two-container model: an IB Gateway container (handles the IB connection) and the MMR container (runs trading services).

```
$ git clone https://github.com/9600dev/mmr.git
$ cd mmr/
$ ./docker.sh --go     # builds, deploys, and runs the docker container
```

or:

```
$ ./docker.sh --clean  # cleans images and containers
$ ./docker.sh --build  # builds image
$ ./docker.sh --run    # deploys container in docker, runs MMR
```

![Docker build](docs/2022-10-08-09-30-05.png)

The script will build the docker image and run a container instance for you. On first run it will prompt for your IB credentials and write a `.env` file.

Once it's built and running, ssh into the container:

```
$ ssh trader@localhost -p 2222
```

![](docs/2022-10-08-09-34-11.png)

After this has completed, it will call `start_mmr.sh` in the MMR root directory, which starts a [tmux](https://github.com/tmux/tmux/wiki) session with:

* `pycron` — process scheduler that manages the trading runtime services. Manually: `python3 pycron/pycron.py --config ./configs/pycron.yaml`
* `cli` — command line interface to interact with the trading system (check portfolio, check systems, manually create orders etc). Manually: `python3 -m trader.mmr_cli`
* `trader_service_log` — displays the trader service log in real time.
* `strategy_service_log` — displays the strategy service log in real time.

![](docs/2022-10-08-09-43-06.png)

When starting MMR for the first time, there are a couple of things you should do:

#### Checking MMR status:

* ```status```

![](docs/2022-10-08-09-52-53.png)

#### Bootstrapping the symbol universes

After ensuring everything is connected and functioning, you should bootstrap the population of the "symbol universe". This is MMR's cache of most of Interactive Brokers tradeable instruments, mapping symbol to IB contract ID (i.e. AMD -> 4931).

* ```universes bootstrap```

![universes cli](docs/universes.png)

This command will grab the symbols for NYSE, NASDAQ, ASX, LSE, CFE, GLOBEX, NYMEX, CBOE, NYBOT, and ICEUS and stash them in their respective "universes". The command typically takes a good couple of hours to complete.

A Universe (like "NASDAQ") is a collection of symbols and their respective Interactive Brokers contract id's to which you can apply your trading algo's to. You can resolve a symbol to universe and contract id via:

* ```resolve --symbol AMD```

![mmr resolve](docs/resolve.png)

From here you're good to go: either using the `cli` to push manual trades to IB, or by implementing an algo, through extending the `Strategy` abstract class. An example of a strategy implementation can be found [here](https://github.com/9600dev/mmr/blob/master/strategies/smi_crossover.py). There's still a lot to do here, and the implementation of the strategy runtime changes often.

### Trading Manually from the Command Line

There are two ways to perform trades and inspect the MMR runtime manually: from the command line, or through the CLI helper.

To fire up the CLI helper, type:

* ```python3 -m trader.mmr_cli```

![](docs/2022-09-30-11-07-06.png)

This gives you a range of options to interact with the MMR runtime: inspect orders, logs, trades etc, and manually submit trades from the CLI itself:

* ```mmr> buy AMD --limit 60.00 --amount 100.0```

While most command inputs take the string symbol (in this case "AMD") its best to use the unique contract identifier that Interactive Brokers supplies, and you can do this via the ```mmr> resolve``` command:

![](docs/2022-09-30-11-08-58.png)

Trading from your command line interface of choice is also supported:

* ```python3 -m trader.mmr_cli buy AMD --market --amount 100.0```

### CLI commands

You can use the REPL (read eval print loop) via ```python3 -m trader.mmr_cli``` or by invoking commands directly, e.g. ```python3 -m trader.mmr_cli buy AMD --market --amount 100.0```

| Command       | Sub Commands | Help |
| :-------------| ----:|:------|
| book          | cancel orders trades | shows the current order book |
| clear         | | clears the screen |
| exit          || exits the cli |
| history       | bar-sizes get-symbol-history-ib get-universe-history-ib jobs read security summary | retrieves historical price data from IB for a given security or universe. Use "history bar-sizes" for a list of bar sizes supported |
| option        | plot | gets option data for a given date and plots a histogram of future price |
| portfolio     | | shows the current portfolio |
| positions     | | shows current positions |
| pycron        | | shows pycron status |
| reconnect     | | reconnects to Interactive Brokers |
| resolve       | | resolves a symbol (i.e. AMD) to a universe and IB connection ID |
| snapshot      | | gets a price snapshot (delayed or realtime) for a given symbol |
| status        | | checks the status of all services and systems |
| strategy      | enable list | lists, enables and disables loaded strategies |
| subscribe     | list listen portfolio start universe | subscribes to tick data for a given symbol or universe, optionally displays tick changes to console |
| trade         | | creates buy and sell orders (market, limit) with or without stop loss |
| universes     | add-to-universe bootstrap create destroy get list | creates and deletes universes; adds securities to a given universe; bootstraps universes from IB |

## Implementing an Automated Algorithm/Strategy

This is all still in flux, but you can take a look at the source code to get a sense of the different algo implementation abstractions:

* Most flexible, most difficult: Using [trader/listeners/ibreactive.py](https://github.com/9600dev/mmr/blob/master/trader/listeners/ibreactive.py). This is a RxPY wrapper around the ib_async library to move from event driven to reactive/stream driven, which makes real-time algo coding easier.
* Less flexible: Adding/extending [trading_runtime.py](https://github.com/9600dev/mmr/blob/master/trader/trading/trading_runtime.py). The trader_service hosts trading_runtime, which spins up a ibreactive.py connection to IB Gateway, maintains trade and book state, and enables real-time streaming tick data subscription handling. It also provides a clean simple library to interact with the Tick Data database, risk analysis, financial math apis and more.
* Less flexible, but preferred method: extending [trader/trading/strategy.py](https://github.com/9600dev/mmr/blob/master/trader/trading/strategy.py). Build your own strategy by extending this base class. It will be hosted by the strategy_service, a separate process that enables tick subscriptions and routes trades through the trader_service. strategy_service will also handle backfilling historical data, ensuring reliability of order routing, and running risk safety checks.


## Debugging

All services log debug, info, critical and warn to the following log files:

* ```logs/trader.log``` for most module level debug output
* ```logs/error.log``` for critical/error logs
* ```logs/trader_service.log``` for trader_service
* ```logs/strategy_service.log``` for strategy_service

## Development Notes

* Something weird happening? Want to know more about a particular abstraction? Check out the [docs/DEV_NOTES.md](docs/DEV_NOTES.md) doc.
* Something weird in TWS happening? This link here: [Random notes about Trader Workstation TWS](https://dimon.ca/dmitrys-tws-api-faq/) might have you covered.
* Want a list of all the Interactive Brokers instruments in their trading universe: click [here](https://www.interactivebrokers.com/en/index.php?f=1563&p=fut)

# Direction

MMR's scalability target is many trades per minute to a couple of trades per month:

![Contrast diagram](docs/contrast_diagram.png)

Therefore, needs to support a wide range of strategy requirements:

* [low frequency] Risk analysis of portfolio compared to market conditions: re-balancing. Tax implications etc.
* [medium frequency] Trade strategies that span multiple days/weeks: mean reversion over time.
* [semi-high frequency] Momentum trades spanning minutes, quick exits from risky positions, market making (selling buying calls and puts) etc.
* **Non-goal is sub-second high frequency trading**.

# Block architecture

![Block diagram](docs/block_arch_2.png)

#### High level product requirements/architecture choices:

* Live market feeds for prices etc are saved and "re-playable" via simulation. (not done)
* Historical data can be use to "replay" previous days/months/years of trading (i.e. use bid, ask, price, last_price, volume, etc to simulate the IB market feed) (not done)
* Two dominant data-structures the platform is based on:
  * DataFrames that represent prices, which can be 'windowed' over time. (done)
  * Reactive Extensions for real time/reactive algorithmic programming. (done)
* *_service.py are independent processes, and multiple instances of them can be spun up for horizontal scalability. Each process communicates via ZeroMQ messages with msgpack as the compressed message container. See more [here](https://zeromq.org/messages/). Ticks arrive as pandas dataframes. (done)

### Pycron ```pycron/pycron.py```

Pycron deals with scheduling, starting, stopping and restarting processes, services, and tasks. It features:

* Sorting out the starting of process dependencies.
* Restarting processes that crash
* Scheduling the start/stop/restart of jobs for specific times/days
* Runs periodic health checks
* Has a small tornado based webservice that allows for remote control of processes

# License

This work is [fair-code](http://faircode.io/) distributed under [Apache 2.0 with Commons Clause](LICENSE.md) license.
The source code is open and everyone (individuals and organizations) can use it for free.
However, it is not allowed to sell products and services that are mostly just this software.

If you have any questions about this or want to apply for a license exception, please contact the author.

Installing optional dependencies may be subject to a more restrictive license.

# Disclaimer

This software is for educational purposes only. Do not risk money which you are afraid to lose.
USE THE SOFTWARE AT YOUR OWN RISK. THE AUTHORS AND ALL AFFILIATES ASSUME NO RESPONSIBILITY FOR YOUR TRADING RESULTS.
