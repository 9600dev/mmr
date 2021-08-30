FROM ubuntu:21.04
WORKDIR /home/trader/mmr
ENV container docker
ENV PATH "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

# RUN useradd -rm -d /home/trader -s /bin/bash -g root -G sudo -u 1000 trader
RUN useradd -m -d /home/trader -s /bin/bash -G sudo trader
RUN mkdir -p /var/run/sshd
RUN mkdir -p /run/sshd
RUN apt-get update
RUN apt-get install dialog apt-utils -y
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN apt-get install -y git
RUN apt-get install -y wget
RUN apt-get install -y vim
RUN wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | apt-key add -
RUN echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/4.4 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-4.4.list
RUN apt-get update -y
RUN apt-get install -y mongodb-org
RUN apt-get install -y linux-headers-generic
RUN apt-get install -y lzip curl
RUN apt-get install -y redis-server
RUN apt-get install -y openssh-server sudo
RUN apt-get install -y unzip
RUN apt-get install -y tmux
RUN apt-get install -y expect
RUN apt-get install -y iproute2
RUN apt-get install -y net-tools
RUN apt-get install -y rsync
RUN apt-get install -y iputils-ping

RUN echo 'trader:trader' | chpasswd
RUN service ssh start

# ports
# ssh
EXPOSE 22
# tws
EXPOSE 7496
# redis
EXPOSE 6379
# mongo/arctic
EXPOSE 27017
# pycron
EXPOSE 8081
# vnc
EXPOSE 5900
EXPOSE 5901

# copy over the source and data
COPY ./ /home/trader/mmr/

# spin up the directories required
RUN mkdir /home/trader/ibc
RUN mkdir /home/trader/ibc/logs
# RUN mkdir /home/trader/mmr/data
RUN mkdir /home/trader/mmr/data/redis
RUN mkdir /home/trader/mmr/data/mongodb
RUN mkdir /home/trader/mmr/logs

# install IBC
RUN wget https://github.com/IbcAlpha/IBC/releases/download/3.8.7/IBCLinux-3.8.7.zip -P /home/trader
RUN unzip /home/trader/IBCLinux-3.8.7.zip -d /home/trader/ibc
RUN rm /home/trader/IBCLinux-3.8.7.zip
RUN chmod +x /home/trader/ibc/*.sh

# download TWS offline installer
RUN wget https://download2.interactivebrokers.com/installers/tws/latest-standalone/tws-latest-standalone-linux-x64.sh -P /home/trader
RUN chmod +x /home/trader/tws-latest-standalone-linux-x64.sh
RUN chmod +x /home/trader/mmr/scripts/installation/install_tws.sh

# TWS needs JavaFX to be able to fully start
RUN apt-get install -y openjfx

# pip install packages
RUN --mount=type=cache,target=/root/.cache \
   pip3 install -r /home/trader/mmr/requirements.txt

# install window managers, Xvfb and vnc
RUN apt-get install -y tigervnc-scraping-server
RUN apt-get install -y awesome
RUN apt-get install -y xvfb
RUN apt-get install -y xterm
RUN apt-get install -y language-pack-en-base

RUN mkdir /home/trader/.vnc
RUN echo 'trader' | vncpasswd -f > /home/trader/.vnc/passwd

RUN mkdir /home/trader/.config
RUN mkdir /home/trader/.config/awesome

COPY ./scripts/installation/.bash_profile /home/trader
COPY ./scripts/installation/start_trader.sh /home/trader
COPY ./configs/twsstart.sh /home/trader/ibc/twsstart.sh
COPY ./configs/config.ini /home/trader/ibc/config.ini

# data needs to be copied manually?
# COPY ./data/ib_symbols_nyse_nasdaq.csv /home/trader/mmr/data/ib_symbols_nyse_nasdaq.csv
# COPY ./data/symbols_historical.csv /home/trader/mmr/data/symbols_historical.csv


RUN chown -R trader:trader /home/trader
WORKDIR /home/trader

#ENTRYPOINT /home/trader/mmr/scripts/docker_startup.sh
#CMD tail -f /dev/null
ENTRYPOINT service ssh restart && tail -f /dev/null