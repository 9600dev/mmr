#!/bin/bash

export TRADER_CONFIG="/home/trader/mmr/configs/trader.yaml"
RED=`tput setaf 1`
GREEN=`tput setaf 2`
BLUE=`tput setaf 4`
WHITE=`tput setaf 7`
BLACK=`tput setaf 0`
CYAN=`tput setaf 6`
RESET=`tput sgr0`

echo ""
echo " $(tput setab 2)${BLACK}----------------------------------------${RESET}"
echo " $(tput setab 2)${BLACK}|${RESET}     start_trader.sh started          $(tput setab 2)${BLACK}|${RESET}"
echo " $(tput setab 2)${BLACK}----------------------------------------${RESET}"
echo ""

set -e

if [ -t 0 ] ; then
    echo ""
else
    echo "You must start the container with 'interactive mode' enabled (docker run -it ...) or ssh into container"
    exit 1
fi

# check to see if we've installed tws
if [ ! -d /home/trader/Jts ]; then
    # likely first time start
    echo "First time running, let's configure Interactive Brokers!"
    echo -n "Please enter Interactive Brokers username: "
    read USERNAME;

    echo -n "Please enter Interactive Brokers password: "
    read -s PASSWORD;

    if [ -z "$USERNAME" ]
    then
        echo "Username is empty, script will fail, exiting"
        exit 1
    fi

    if [ -z "$PASSWORD" ]
    then
        echo "Password is empty, script will fail, exiting"
        exit 1
    fi

    # we already do this in Dockerfile and native_installation.sh but we should redo it anyway
    /bin/cp /home/trader/mmr/scripts/installation/config.ini /home/trader/ibc/config.ini
    /bin/cp /home/trader/mmr/scripts/installation/twsstart.sh /home/trader/ibc/twsstart.sh

    /bin/sed -i "s/{username}/$USERNAME/g" /home/trader/ibc/twsstart.sh
    /bin/sed -i "s/{username}/$USERNAME/g" /home/trader/ibc/config.ini

    /bin/sed -i "s/{password}/$PASSWORD/g" /home/trader/ibc/twsstart.sh
    /bin/sed -i "s/{password}/$PASSWORD/g" /home/trader/ibc/config.ini

    echo ""
    echo "Automating the installation of Trader Workstation to /home/trader/Jts..."
    echo ""
    /usr/bin/expect /home/trader/mmr/scripts/installation/tws-install.exp
    echo ""
    echo "Installed. Hit enter to start the pycron tmux session, which starts all"
    echo "trader services (Arctic DB, Redis, pycron, X windows, VNC Server, etc."
    echo ""
    chmod +x /home/trader/ibc/scripts/displaybannerandlaunch.sh
    chmod +x /home/trader/ibc/scripts/ibcstart.sh

    if [ -d ~/Jts ] && [ ! -d /home/trader/Jts ]
    then
        # default install went to running users directory
        mv ~/Jts /home/trader
    fi

    TWS_VERSION=$(/bin/ls -m /home/trader/Jts | /bin/head -n 1 | /bin/sed 's/,.*$//')
    /bin/sed -i "s/{tws_version}/$TWS_VERSION/g" /home/trader/ibc/twsstart.sh

    # move the default jts.ini settings over
    # this prevents the 'do you want to use SSL dialog' from popping
    cp /home/trader/mmr/scripts/installation/jts.ini /home/trader/Jts

    read NULL;
fi

echo "Starting or attaching to tmux session to host pycron and start the command line interface."
cd /home/trader/mmr

if [ -z "${TMUX}" ]
then
        echo "starting new tmux session for mmr trader"
        cd /home/trader/mmr
        tmux new-session -d -n pycron 'echo; echo "Ctrl-b + n [next window], Ctrl-b + p [previous window]"; echo; python3 pycron/pycron.py --config ./configs/pycron.yaml' \; new-window -d -n cli python3 cli.py \; new-window -d -n dashboard python3 info.py \; new-window -d -n trader_service_log lnav logs/trader_service.log \; new-window -d -n strategy_service_log lnav logs/strategy_service.log \; attach
fi