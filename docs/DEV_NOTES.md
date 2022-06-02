# Development Notes

## What to do when things don't work

* If TWS is failing to start via IBC, have a look at the IBC logs in /home/trader/ibc/logs.
* ib_status() returning False and you can't start TWS? `export TRADER_CHECK=False` and that call will be ignored. That function screen scrapes (https://www.interactivebrokers.com/en/?f=%2Fen%2Fsoftware%2FsystemStatus.php) and occasionally will get a red status that doesn't actually impact trading.

## Backlog
* ib_status() needs to deal with the ambiguity of outages vs. outages of specific exchanges we don't actually care about:
* ![](2022-05-29-11-37-19.png)
* traitlets is broken on 5.2.1 and 5.2.2, version assertion error. Forced install of 5.2.0
* aioredis 1.2.0 is required for lightbus.
* ExitAfterSecondFactorAuthenticationTimeout=yes doesn't work (crash on startup). Setting to 'no' works fine.