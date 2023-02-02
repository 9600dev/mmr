#!/bin/bash

export JTS_DIR="/home/trader/mmr/third_party/Jts"
export IBC_DIR="/home/trader/mmr/third_party/ibc"
export MMR_DIR="/home/trader/mmr"
if [[ -z "{TRADER_CONFIG}" ]]; then
    export TRADER_CONFIG="${TRADER_CONFIG}"
else
    export TRADER_CONFIG="$MMR_DIR/configs/trader.yaml"
fi
export TMUX_START=true

RED=`tput setaf 1`
GREEN=`tput setaf 2`
BLUE=`tput setaf 4`
WHITE=`tput setaf 7`
BLACK=`tput setaf 0`
CYAN=`tput setaf 6`
RESET=`tput sgr0`

# parse arguments
VALID_ARGS=$(getopt -o tlp --long no-tmux,live,paper -- "$@")
eval set -- "$VALID_ARGS"
while [ : ]; do
  case "$1" in
    -t | --no-tmux)
        echo ""
        echo "will not start with tmux session"
        TMUX_START=false
        shift
        ;;
    -p | --paper)
        echo ""
        echo "updating configurations in $TRADER_CONFIG and IBC config.ini to trade paper account"
        PAPER=true
        shift
        ;;
    -l | --live)
        echo ""
        echo "updating configurations in $TRADER_CONFIG and IBC config.ini to trade live account"
        LIVE=true
        shift
        ;;
    --) shift;
        break
        ;;
  esac
done

function parse_yaml {
   local prefix=$2
   local s='[[:space:]]*' w='[a-zA-Z0-9_]*' fs=$(echo @|tr @ '\034')
   sed -ne "s|^\($s\):|\1|" \
        -e "s|^\($s\)\($w\)$s:$s[\"']\(.*\)[\"']$s\$|\1$fs\2$fs\3|p" \
        -e "s|^\($s\)\($w\)$s:$s\(.*\)$s\$|\1$fs\2$fs\3|p"  $1 |
   awk -F$fs '{
      indent = length($1)/2;
      vname[indent] = $2;
      for (i in vname) {if (i > indent) {delete vname[i]}}
      if (length($3) > 0) {
         vn=""; for (i=0; i<indent; i++) {vn=(vn)(vname[i])("_")}
         printf("%s%s%s=\"%s\"\n", "'$prefix'",vn, $2, $3);
      }
   }'
}

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

# deal with --live and --paper arguments
if [ -n "$LIVE" ]; then
    CONF_trading_mode="live"
    sed -i "s/^TradingMode=.*/TradingMode=$CONF_trading_mode/" $IBC_DIR/config.ini
    sed -i "s/^trading_mode:.*/trading_mode: $CONF_trading_mode/" $TRADER_CONFIG
fi

if [ -n "$PAPER" ]; then
    CONF_trading_mode="paper"
    sed -i "s/^TradingMode=.*/TradingMode=$CONF_trading_mode/" $IBC_DIR/config.ini
    sed -i "s/^trading_mode:.*/trading_mode: $CONF_trading_mode/" $TRADER_CONFIG
fi

# check to see if we've installed IBC
if [ ! -d $IBC_DIR ] || [ ! "$(ls $IBC_DIR)" ]; then
    echo ""
    echo "Can't find IBC in $IBC_DIR. Either a non-docker install, or it's misconfigured?"
    echo "Let's try and download and install it anyway..."
    echo ""
    LATEST_IBC=$(curl -sL https://api.github.com/repos/IbcAlpha/IBC/releases/latest | jq -r ".tag_name")
    mkdir -p $IBC_DIR
    wget https://github.com/IbcAlpha/IBC/releases/download/$LATEST_IBC/IBCLinux-$LATEST_IBC.zip -P $IBC_DIR
    cd $IBC_DIR
    unzip IBCLinux-$LATEST_IBC.zip
    chmod +x $IBC_DIR/*.sh
    rm -f $IBC_DIR/IBCLinux-$LATEST_IBC.zip
    echo ""
    echo "Finished unzipping IBC"
    echo ""
fi

# check to see if we've installed tws
if [ ! -d $JTS_DIR ] || [ ! "$(ls -I *.ini $JTS_DIR)" ]; then
    # likely first time start
    echo "Can't find TWS, first time running? Let's download, install and configure Interactive Brokers!"

    if [ ! -f $MMR_DIR/tws-latest-standalone-linux-x64.sh ]; then
        echo "latest TWS linux installer not found, downloading"
        rm -f $MMR_DIR/tws-latest-standalone-linux-x64.sh
        wget https://download2.interactivebrokers.com/installers/tws/latest-standalone/tws-latest-standalone-linux-x64.sh -P $MMR_DIR/third_party
        chmod +x $MMR_DIR/third_party/tws-latest-standalone-linux-x64.sh
        chmod +x $MMR_DIR/scripts/installation/install_tws.sh
    fi

    echo ""
    echo ""
    echo "Automating the installation of Trader Workstation to $JTS_DIR..."
    echo ""
    expect $MMR_DIR/scripts/installation/tws-install.exp $MMR_DIR $JTS_DIR

    # move the default jts.ini settings over
    # this prevents the 'do you want to use SSL dialog' from popping
    cp $MMR_DIR/scripts/installation/jts.ini $JTS_DIR

    if [ -f $MMR_DIR/third_party/tws-latest-standalone-linux-x64.sh ]; then
        rm -f $MMR_DIR/third_party/tws-latest-standalone-linux-x64.sh
    fi
fi

# now make sure the config files are properly set up
if [ "$(grep -Fx TWSUSERID= $IBC_DIR/twsstart.sh)" ]; then
    echo ""
    echo ""
    echo "[Can't find a username set in $IBC_DIR/twsstart.sh, asking for credentials]:"
    echo -n "Please enter Interactive Brokers username: "
    read USERNAME;

    echo ""
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

    # deal with trading mode option
    eval $(parse_yaml $TRADER_CONFIG "CONF_")

    echo "TWS trading mode configured in $TRADER_CONFIG is: $CONF_trading_mode"
    echo "Hit enter to keep, or type 'paper' or 'live' to change: "
    read -s TRADING_MODE;

    if [ -z "$TRADING_MODE" ]
    then
        echo "Using trading mode from $TRADER_CONFIG"
    else
        echo "Using trading mode from user input: $TRADING_MODE"
        CONF_trading_mode=$TRADING_MODE
    fi

    sed -i "s/^TWSUSERID=.*/TWSUSERID=$USERNAME/" $IBC_DIR/twsstart.sh
    sed -i "s/^TWSPASSWORD=.*/TWSPASSWORD=$PASSWORD/" $IBC_DIR/twsstart.sh

    sed -i "s/^IbLoginId=.*/IbLoginId=$USERNAME/" $IBC_DIR/config.ini
    sed -i "s/^IbPassword=.*/IbPassword=$PASSWORD/" $IBC_DIR/config.ini

    sed -i "s/^TradingMode=.*/TradingMode=$CONF_trading_mode/" $IBC_DIR/config.ini
    sed -i "s/^trading_mode:.*/trading_mode: $CONF_trading_mode/" $TRADER_CONFIG

    TWS_VERSION=$(ls -m $JTS_DIR | head -n 1 | sed 's/,.*$//')
    sed -i "s/^TWS_MAJOR_VRSN.*/TWS_MAJOR_VRSN=$TWS_VERSION/" $IBC_DIR/twsstart.sh

    IBC_DIR_ESCAPED=${IBC_DIR//\//\\/}
    JTS_DIR_ESCAPED=${JTS_DIR//\//\\/}

    sed -i "s/^IBC_PATH=.*/IBC_PATH=$IBC_DIR_ESCAPED/" $IBC_DIR/twsstart.sh
    sed -i "s/^TWS_PATH=.*/TWS_PATH=$JTS_DIR_ESCAPED/" $IBC_DIR/twsstart.sh
    sed -i "s/^LOG_PATH=.*/LOG_PATH=$IBC_DIR_ESCAPED\/logs/" $IBC_DIR/twsstart.sh
    sed -i "s/^IBC_INI=.*/IBC_INI=$IBC_DIR_ESCAPED\/config.ini/" $IBC_DIR/twsstart.sh

    echo ""
    echo ""
    echo "Installed. Hit enter to start the pycron tmux session, which starts all"
    echo "trader services (Arctic DB, Redis, pycron, X windows, VNC Server, etc."
    echo ""
    echo ""
    chmod +x $IBC_DIR/scripts/displaybannerandlaunch.sh
    chmod +x $IBC_DIR/scripts/ibcstart.sh\

    read NULL;
fi





echo "Starting or attaching to tmux session to host pycron and start the command line interface."
cd $MMR_DIR

if [ ! "$(grep -Fx TWSUSERID= $IBC_DIR/twsstart.sh)" ] && [ -z "${TMUX}" ] && [ TMUX_START = true ]; then
    echo "starting new tmux session for mmr trader"
    cd $MMR_DIR
    tmux new-session -d -n pycron 'echo; echo "Ctrl-b + n [next window], Ctrl-b + p [previous window]"; echo; python3 pycron/pycron.py --config ./configs/pycron.yaml' \; new-window -d -n cli python3 cli.py \; new-window -d -n dashboard python3 info.py \; new-window -d -n trader_service_log lnav logs/trader_service.log \; new-window -d -n strategy_service_log lnav logs/strategy_service.log \; attach
elif [ ! "$(grep -Fx TWSUSERID= $IBC_DIR/twsstart.sh)" ]; then
    echo ""
    echo "starting pycron directly"
    echo "> python3 pycron/pycron.py --config $IBC_DIR/configs/pycron.yaml"
    echo ""
    python3 pycron/pycron.py --config $IBC_DIR/configs/pycron.yaml
else
    echo "There doesn't seem to be a password set in $IBC_DIR/twsstart.sh, which may mean the installation script failed. Aborting."
fi