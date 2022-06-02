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
sudo wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | apt-key add -
sudo echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/4.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.4.list
sudo apt-get update -y
sudo apt-get install -y mongodb-org
sudo apt-get install -y lzip curl
sudo apt-get install -y redis-server
sudo apt-get install -y unzip
sudo apt-get install -y expect
sudo apt-get install -y iputils-ping
sudo apt-get install -y language-pack-en-base

echo "creating required directories in /home/trader"
mkdir /home/trader/ibc
mkdir /home/trader/ibc/logs
mkdir /home/trader/mmr/data
mkdir /home/trader/mmr/data/redis
mkdir /home/trader/mmr/data/mongodb
mkdir /home/trader/mmr/logs

echo "installing IBC (Trader Workstation login automation)"
# install IBC
wget https://github.com/IbcAlpha/IBC/releases/download/3.8.7/IBCLinux-3.8.7.zip -P /home/trader
unzip /home/trader/IBCLinux-3.8.7.zip -d /home/trader/ibc
rm /home/trader/IBCLinux-3.8.7.zip
chmod +x /home/trader/ibc/*.sh

# download latest TWS offline installer
echo "downloading Interactive Brokers Trader Workstation"
wget https://download2.interactivebrokers.com/installers/tws/latest-standalone/tws-latest-standalone-linux-x64.sh -P /home/trader
chmod +x /home/trader/tws-latest-standalone-linux-x64.sh
chmod +x /home/trader/mmr/scripts/installation/install_tws.sh

# before installing requirements, we need to sudo install rq to put it in /usr/local/bin/rq
sudo pip3 install rq

# pip install packages
echo "installing python requirements from requirements.txt"
pip3 install -r requirements.txt

# install window managers, Xvfb and vnc
echo "installing xvfb frame buffer window manager to run TWS"
sudo apt-get install -y tigervnc-scraping-server
# sudo apt-get install -y awesome
sudo apt-get install -y python3-cairocffi
sudo apt-get install -y xvfb
pip3 install qtile

mkdir /home/trader/.vnc
echo 'trader' | vncpasswd -f > /home/trader/.vnc/passwd

cp ./configs/twsstart.sh /home/trader/ibc/twsstart.sh
cp ./configs/config.ini /home/trader/ibc/config.ini

scripts/installation/start_trader.sh




