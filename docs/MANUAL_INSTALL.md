# Manual Install

** Not recommended **

If you want to YOLO the install, and you're on Ubuntu or a derivative, you can try running the ```native_install.sh``` script. Otherwise, this might get you close enough:

* Clone this repository into /home/trader/mmr directory
* Create a user 'trader' (or chown a new /home/trader directory)
* Install [Trader Workstation](https://download2.interactivebrokers.com/installers/tws/latest-standalone/tws-latest-standalone-linux-x64.sh)
* Install IBC: https://github.com/IbcAlpha/IBC into /home/trader/ibc
* Copy ``scripts/installation/config.ini`` and ``scripts/installation/twstart.sh`` into /home/trader/ibc directory. Adjust both of those files to reflect the TWS version you installed.
* Make sure ~/ibc/logs and ~/mmr/logs directories are created
* Install MongoDB: https://docs.mongodb.com/manual/installation/
* Install Arctic Database (Mongo wrapper basically): https://github.com/man-group/arctic
* Install Python >= 3.9.5
* Install Pip and then ```pip3 install -r requirements.txt```
* Install Redis via ```sudo apt install redis-server```
* Start pycron:
 	* ```cd /home/trader/mmr/pycron```
 	* ```python3 pycron.py ../configs/pycron.yaml```

