FROM debian:11.5
WORKDIR /home/trader/mmr
ENV container docker
ENV PATH "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

RUN useradd -m -d /home/trader -s /bin/bash -G sudo trader
RUN mkdir -p /var/run/sshd
RUN mkdir -p /run/sshd
RUN mkdir -p /tmp

# if you update the FROM ubuntu:20.04 to some later ubuntu distribution, you may need the following line:
# RUN cp /etc/apt/sources.list /etc/apt/sources.list.bak && sed -i -re 's/([a-z]{2}\.)?archive.ubuntu.com|security.ubuntu.com/old-releases.ubuntu.com/g' /etc/apt/sources.list

RUN apt-get update
RUN apt-get install -y dialog apt-utils
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN apt-get install -y git
RUN apt-get install -y wget
RUN apt-get install -y vim
RUN apt-get install -y dpkg
RUN apt-get install -y build-essential

# set to New York timezone, can override with docker run -e TZ=Europe/London etc.
ENV TZ=America/New_York
RUN apt-get install -y tzdata
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# mongo
RUN wget -qO - https://www.mongodb.org/static/pgp/server-5.0.asc | apt-key add -
RUN echo "deb http://repo.mongodb.org/apt/debian bullseye/mongodb-org/5.0 main" | tee /etc/apt/sources.list.d/mongodb-org-5.0.list
RUN apt-get update -y
RUN apt-get install -y mongodb-org

RUN apt-get install -y linux-headers-generic
RUN apt-get install -y lzip curl
RUN apt-get install -y locales-all
RUN apt-get install -y redis-server
RUN apt-get install -y openssh-server
RUN apt-get install -y sudo
RUN apt-get install -y unzip
RUN apt-get install -y tmux
RUN apt-get install -y expect
RUN apt-get install -y iproute2
RUN apt-get install -y net-tools
RUN apt-get install -y rsync
RUN apt-get install -y iputils-ping
RUN apt-get install -y lnav

# required for pyenv to build 3.9.5 properly
RUN apt-get install -y libbz2-dev
RUN apt-get install -y libsqlite3-dev
RUN apt-get install -y libreadline-dev

# python version upgrade
RUN apt-get install -y python3-venv
RUN apt-get install -y libedit-dev
RUN apt-get install -y libncurses5-dev
RUN apt-get install -y libssl-dev
RUN apt-get install -y libffi-dev
RUN apt-get install -y liblzma-dev

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
RUN wget https://github.com/IbcAlpha/IBC/releases/download/3.15.2/IBCLinux-3.15.2.zip -P /home/trader
RUN unzip /home/trader/IBCLinux-3.15.2.zip -d /home/trader/ibc
RUN rm /home/trader/IBCLinux-3.15.2.zip
RUN chmod +x /home/trader/ibc/*.sh

# download TWS offline installer
RUN wget https://download2.interactivebrokers.com/installers/tws/latest-standalone/tws-latest-standalone-linux-x64.sh -P /home/trader/mmr
RUN chmod +x /home/trader/mmr/tws-latest-standalone-linux-x64.sh
RUN chmod +x /home/trader/mmr/scripts/installation/install_tws.sh

RUN apt-get update -y

# libs needed for the TWS JDK to function correctly
RUN apt-get install -y libtiff5-dev
RUN apt-get install -y libtiff5
RUN apt-get install -y libjbig-dev
RUN apt-get install -y libpango-1.0-0
RUN apt-get install -y libpangocairo-1.0-0
RUN apt-get install -y libcairo2-dev
RUN apt-get install -y libgdk-pixbuf2.0-0

# install window managers, Xvfb and vnc
RUN apt-get install -y tigervnc-scraping-server
# RUN apt-get install -y python3-cairocffi
RUN apt-get install -y xvfb
RUN apt-get install -y xterm

RUN mkdir /home/trader/.vnc
RUN echo 'trader' | vncpasswd -f > /home/trader/.vnc/passwd

RUN mkdir /home/trader/.config
RUN mkdir /home/trader/.config/qtile
RUN mkdir /home/trader/.tmp
RUN mkdir /home/trader/.cache

COPY ./scripts/installation/.bash_profile /home/trader
COPY ./scripts/installation/twsstart.sh /home/trader/ibc/twsstart.sh
COPY ./scripts/installation/config.ini /home/trader/ibc/config.ini
COPY ./scripts/installation/config.py /home/trader/.configs/qtile

RUN touch /home/trader/.hushlogin
RUN touch /home/trader/mmr/logs/trader_service.log
RUN touch /home/trader/mmr/logs/strategy_service.log

RUN chown -R trader:trader /home/trader

# install all the python 3.9.5 runtimes and packages
USER trader

ENV HOME /home/trader
ENV PYENV_ROOT $HOME/.pyenv
ENV PATH $PYENV_ROOT/shims:$PYENV_ROOT/bin:$HOME/.local/bin:$PATH
ENV TMPDIR $HOME/.tmp

RUN curl https://pyenv.run | bash

WORKDIR /home/trader/mmr

RUN pyenv install 3.9.5
RUN pyenv local 3.9.5
RUN pyenv rehash

# window manager for vnc
RUN pip3 install xcffib
RUN pip3 install --no-cache-dir cairocffi

RUN --mount=type=cache,target=/root/.cache \
   pip3 install qtile

# pip install packages
RUN --mount=type=cache,target=/root/.cache \
   pip3 install -r /home/trader/mmr/requirements.txt

# spin back to root, to start sshd
USER root

WORKDIR /home/trader
ENTRYPOINT service ssh restart && tail -f /dev/null
