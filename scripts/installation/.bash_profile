export PATH="$PATH:/home/trader/.local/bin"
export PYENV_ROOT="$HOME/.pyenv"
export TMPDIR="/home/trader/.tmp"
command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"

export TRADER_CONFIG="/home/trader/mmr/configs/trader.yaml"

# Source Docker env vars (IB_SERVER_ADDRESS, IB_SERVER_PORT, etc.)
if [ -f "$HOME/.mmr_env" ]; then
    source "$HOME/.mmr_env"
fi

cd /home/trader/mmr