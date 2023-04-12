export PATH="$PATH:/home/trader/.local/bin"
export PYENV_ROOT="$HOME/.pyenv"
export TMPDIR="/home/trader/.tmp"
command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"

export TRADER_CONFIG="/home/trader/mmr/configs/trader.yaml"
cd /home/trader/mmr
/home/trader/mmr/start_mmr.sh