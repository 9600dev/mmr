# Development Notes

* Largely following ![this guys approach](https://mitelman.engineering/blog/python-best-practice/automating-python-best-practices-for-a-new-project/#how-to-manage-python-versions-with-pyenv) to build/test/package management etc.
* Using poetry + pyenv for local dev, pinning to Python 3.5.9 (default Ubuntu 21.04 installation)
* Exporting requirements.txt from poetry for a Docker default pip install

## What to do when things don't work

* If TWS is failing to start via IBC, have a look at the IBC logs in /home/trader/ibc/logs.
* ib_status() returning False and you can't start TWS? `export TRADER_CHECK=False` and that call will be ignored. That function screen scrapes (https://www.interactivebrokers.com/en/?f=%2Fen%2Fsoftware%2FsystemStatus.php) and occasionally will get a red status that doesn't actually impact trading.

## Backlog
* ib_status() needs to deal with the ambiguity of outages vs. outages of specific exchanges we don't actually care about:
* ![](2022-05-29-11-37-19.png)
* traitlets is broken on 5.2.1 and 5.2.2, version assertion error. Forced install of 5.2.0
* aioredis 1.2.0 is required for lightbus.
* ExitAfterSecondFactorAuthenticationTimeout=yes doesn't work (crash on startup). Setting to 'no' works fine.

## Reading for devenv

* https://realpython.com/dependency-management-python-poetry/#add-poetry-to-an-existing-project