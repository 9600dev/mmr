#!/bin/bash

# start TWS installer
/home/trader/mmr/third_party/tws-latest-standalone-linux-x64.sh

# once that's finished, +x on ibcstart.sh and displaybannerandlaunch.sh
chmod +x /home/trader/mmr/third_party/ibc/scripts/displaybannerandlaunch.sh
chmod +x /home/trader/mmr/third_party/ibc/scripts/ibcstart.sh
