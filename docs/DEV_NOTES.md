# Development Notes

* Largely following ![this guys approach](https://mitelman.engineering/blog/python-best-practice/automating-python-best-practices-for-a-new-project/#how-to-manage-python-versions-with-pyenv) to build/test/package management etc.
* Using poetry + pyenv for local dev, pinning to Python 3.5.9 (default Ubuntu 21.04 installation)
* Exporting requirements.txt from poetry for a Docker default pip install
* We set AcceptIncomingConnectionAction=accept in IBC's config.ini, which should automatically accept incoming API connections to TWS. This is insecure, so either set it to "manual", or configure it yourself when you fire up TWS for the first time.

## What to do when things don't work

* If TWS is failing to start via IBC, have a look at the IBC logs in /home/trader/ibc/logs.
* ib_status() returning False and you can't start TWS? `export TRADER_CHECK=False` and that call will be ignored. That function screen scrapes (https://www.interactivebrokers.com/en/?f=%2Fen%2Fsoftware%2FsystemStatus.php) and occasionally will get a red status that doesn't actually impact trading.

## Backlog

* getting portfolio stock history looks like it's blocking out RPC calls from lightbus.
* ib_status() needs to deal with the ambiguity of outages vs. outages of specific exchanges we don't actually care about:
* ![](2022-05-29-11-37-19.png)
* traitlets is broken on 5.2.1 and 5.2.2, version assertion error. Forced install of 5.2.0
* aioredis 1.2.0 is required for lightbus.
* ExitAfterSecondFactorAuthenticationTimeout=yes doesn't work (crash on startup). Setting to 'no' works fine.

## Asyncio Lifecycle

* ![](2022-06-21-09-20-44.png)
* [https://docs.python.org/3/library/asyncio-task.html](https://docs.python.org/3/library/asyncio-task.html)
* [https://python.plainenglish.io/how-to-manage-exceptions-when-waiting-on-multiple-asyncio-tasks-a5530ac10f02](Managing exceptions in the asyncio lifecycle)
* There are three main types of awaitable objects: coroutines, Tasks, and Futures.
    * Tasks are used to schedule coroutines concurrently. When a coroutine is wrapped into a Task with functions like asyncio.create_task() the coroutine is automatically scheduled to run soon.
    * ![](2022-06-21-10-46-01.png)
    * "Save a reference to the result of this function, to avoid a task disappearing mid execution. The event loop only keeps weak references to tasks. A task that isn’t referenced elsewhere may get garbage-collected at any time, even before it’s done. For reliable “fire-and-forget” background tasks, gather them in a collection"
    * ![](2022-06-21-10-56-12.png)
    * Python coroutines are awaitables and therefore can be awaited from other coroutines:
    * ![](2022-06-21-10-46-40.png)
    * ![](2022-06-21-10-47-52.png)
    * ![](2022-06-21-10-57-14.png)
    * ![](2022-06-21-10-58-52.png)
    * ![](2022-06-21-11-02-59.png)
    * ![](2022-06-21-11-03-33.png)
    * ![](2022-06-21-11-06-36.png)

## aioreactive

* Must dispose of disposables, otherwise the Mailbox processor used for processing messages passed through to Observables will continue to hold asyncio tasks. This causes the dreaded "Task was destroyed, but still PENDING" exception when closing down the loop.

## Devenv reading

* https://realpython.com/dependency-management-python-poetry/#add-poetry-to-an-existing-project
*