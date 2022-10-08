RED=`tput setaf 1`
GREEN=`tput setaf 2`
BLUE=`tput setaf 4`
WHITE=`tput setaf 7`
BLACK=`tput setaf 0`
CYAN=`tput setaf 6`
RESET=`tput sgr0`

echo ""
echo " $(tput setab 2)${BLACK}----------------------------------------${RESET}"
echo " $(tput setab 2)${BLACK}|${RESET}     trader installer                 $(tput setab 2)${BLACK}|${RESET}"
echo " $(tput setab 2)${BLACK}----------------------------------------${RESET}"
echo ""

echo "This script will install the dependences for mmr trader on this machine."
echo "Docker install is the preferred installation method: scripts/build_docker.sh"
echo ""
echo "We recommend you install pyenv and the Python 3.9.5 runtime before continuing."
echo ""
echo "Press enter to continue."

read NULL;

# check to see if we've got the /home/trader directory
if [ ! -d /home/trader ]; then
    echo "/home/trader directory not found. This directory should exist,"
    echo "be chown'ed to you, and have the 'mmr' git repository in /home/trader/mmr"
    echo "exiting install."
    exit 1
fi

echo "apt-get installing prereqs"
sudo apt-get install dialog apt-utils -y
sudo apt-get install -y python3
sudo apt-get install -y python3-pip
sudo apt-get install -y git
sudo apt-get install -y wget

sudo wget -qO - https://www.mongodb.org/static/pgp/server-5.0.asc | sudo apt-key add -
sudo echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-5.0.list
sudo apt-get update -y
sudo apt-get install -y mongodb-org
sudo apt-get install -y linux-headers-generic
sudo apt-get install -y lzip curl
sudo apt-get install -y redis-server
sudo apt-get install -y unzip
sudo apt-get install -y expect
sudo apt-get install -y iputils-ping
sudo apt-get install -y language-pack-en-base
sudo apt-get install -y tmux
sudo apt-get install -y expect
sudo apt-get install -y iproute2
sudo apt-get install -y net-tools
sudo apt-get install -y rsync
sudo apt-get install -y iputils-ping
sudo apt-get install -y lnav
# required for pyenv to build 3.9.5 properly
sudo apt-get install -y libbz2-dev
sudo apt-get install -y libsqlite3-dev
sudo apt-get install -y libreadline-dev
sudo apt-get install -y python3-venv


# install poetry
echo "installing poetry"
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python3 -

echo "creating required directories in /home/trader"
mkdir /home/trader/ibc
mkdir /home/trader/ibc/logs
mkdir /home/trader/mmr/data
mkdir /home/trader/mmr/data/redis
mkdir /home/trader/mmr/data/mongodb
mkdir /home/trader/mmr/logs

echo "installing IBC (Trader Workstation login automation)"
# install IBC
wget https://github.com/IbcAlpha/IBC/releases/download/3.14.0/IBCLinux-3.14.0.zip -P /home/trader
unzip /home/trader/IBCLinux-3.14.0.zip -d /home/trader/ibc
rm /home/trader/IBCLinux-3.14.0.zip
chmod +x /home/trader/ibc/*.sh

# download latest TWS offline installer
echo "downloading Interactive Brokers Trader Workstation"
wget https://download2.interactivebrokers.com/installers/tws/latest-standalone/tws-latest-standalone-linux-x64.sh -P /home/trader
chmod +x /home/trader/tws-latest-standalone-linux-x64.sh
chmod +x /home/trader/mmr/scripts/installation/install_tws.sh

# before installing requirements, we need to sudo install rq to put it in /usr/local/bin/rq
sudo pip3 install rq

# pip install packages
echo "installing python requirements from poetry file"
poetry install

# install window managers, Xvfb and vnc
echo "installing xvfb frame buffer window manager to run TWS"
sudo apt-get install -y tigervnc-scraping-server
# sudo apt-get install -y awesome
sudo apt-get install -y python3-cairocffi
sudo apt-get install -y xvfb
pip3 install qtile

mkdir /home/trader/.vnc
echo 'trader' | vncpasswd -f > /home/trader/.vnc/passwd

cp ./scripts/installation/twsstart.sh /home/trader/ibc/twsstart.sh
cp ./scripts/installation/config.ini /home/trader/ibc/config.ini
cp ./scripts/installation/config.py /home/trader/.configs/qtile
cp ./scripts/installation/start_trader.sh /home/trader

cd /home/trader
./start_trader.sh




