FROM debian:bookworm-slim
WORKDIR /home/trader/mmr
ENV container=docker
ENV PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

RUN useradd -m -d /home/trader -s /bin/bash -G sudo trader \
    && mkdir -p /tmp

# System packages (no TWS/VNC/X11 — IB Gateway runs in a separate container)
RUN apt-get update && apt-get install -y --no-install-recommends \
    dialog apt-utils ca-certificates python3 python3-pip python3-venv python3-dev \
    git wget vim dpkg build-essential \
    curl locales-all sudo unzip tmux \
    iproute2 net-tools rsync iputils-ping lnav jq \
    # required for native Python packages that compile C extensions
    libssl-dev libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# set to New York timezone, can override with docker run -e TZ=Europe/London etc.
ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# ports: pycron, zmq
EXPOSE 8081

# spin up the directories required
RUN mkdir -p /home/trader/mmr/data \
    && mkdir -p /home/trader/mmr/logs \
    && mkdir -p /home/trader/.config /home/trader/.tmp /home/trader/.cache /home/trader/.local/bin \
    && chown -R trader:trader /home/trader

RUN touch /home/trader/.hushlogin

# create Python virtualenv (changes rarely — cached layer)
USER trader

ENV HOME=/home/trader
ENV TMPDIR=$HOME/.tmp
ENV PATH=$HOME/.venv/bin:$HOME/.local/bin:$PATH

WORKDIR /home/trader/mmr

RUN python3 -m venv $HOME/.venv

# Copy ONLY requirements.txt first — this layer is cached unless deps change
COPY --chown=trader:trader requirements.txt /home/trader/mmr/requirements.txt

# pip install packages (cached unless requirements.txt changes)
RUN --mount=type=cache,target=/home/trader/.cache/pip \
    pip3 install -r /home/trader/mmr/requirements.txt

# NOW copy the rest of the source (this layer busts on every code change,
# but everything above is cached)
USER root
COPY --chown=trader:trader ./ /home/trader/mmr/
RUN printf '%s\n' \
    'export HOME=/home/trader' \
    'export PATH="$HOME/.venv/bin:$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin"' \
    'export TMPDIR="$HOME/.tmp"' \
    'export TRADER_CONFIG="$HOME/.config/mmr/trader.yaml"' \
    '' \
    '# Source IB Gateway env vars written by docker-entrypoint.sh' \
    '[ -f "$HOME/.mmr_env" ] && . "$HOME/.mmr_env"' \
    '' \
    'cd $HOME/mmr' \
    '$HOME/mmr/start_mmr.sh' \
    > /home/trader/.bash_profile \
    && chown trader:trader /home/trader/.bash_profile

# Pre-populate user config from bundled defaults so TRADER_CONFIG resolves
RUN mkdir -p /home/trader/.config/mmr \
    && cp /home/trader/mmr/configs/*.yaml /home/trader/.config/mmr/ \
    && chown -R trader:trader /home/trader/.config/mmr

RUN touch /home/trader/mmr/logs/trader_service.log \
    && touch /home/trader/mmr/logs/strategy_service.log \
    && touch /home/trader/mmr/logs/data_service.log \
    && touch /home/trader/mmr/logs/trader.log \
    && touch /home/trader/mmr/logs/errors.log \
    && chown -R trader:trader /home/trader/mmr/logs \
    && chmod +x /home/trader/mmr/scripts/docker-entrypoint.sh

WORKDIR /home/trader
ENTRYPOINT ["/home/trader/mmr/scripts/docker-entrypoint.sh"]
