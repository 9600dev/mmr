#!/bin/bash
# shellcheck disable=SC2317
# Don't warn about unreachable commands in this file
#
# Patched run.sh for IB Gateway container.
# Local changes: configurable Xvfb resolution, installed-JRE path repair,
# and native-API recovery supervised by the existing container lifecycle.

set -Eeo pipefail

echo "*************************************************************************"
echo ".> Starting IBC/IB gateway"
echo "*************************************************************************"

# shellcheck disable=SC1091
source "${SCRIPT_PATH}/common.sh"

# Capture the unsuffixed settings path before dual mode changes it below.
RECOVERY_STATE_DIR="${TWS_SETTINGS_PATH:-$TWS_PATH}/.mmr-recovery"
pid=()
watchdog_pid=""
stopping=false

stop_ibc() {
    if [ "$stopping" = true ]; then return; fi
    stopping=true
    trap '' SIGINT SIGTERM
    echo ".> Shutting down IB Gateway."
    if [ -n "$watchdog_pid" ]; then kill -TERM "$watchdog_pid" 2>/dev/null || true; fi
    for child in "${pid[@]}"; do kill -TERM "$child" 2>/dev/null || true; done
    # IBC forwards TERM to Java. Give it a bounded opportunity to close;
    # PID1 exit then lets Docker dispose of remaining container processes.
    for ((attempt = 0; attempt < 10; attempt++)); do
        alive=false
        for child in "${pid[@]}"; do
            if kill -0 "$child" 2>/dev/null; then alive=true; fi
        done
        if [ "$alive" = false ]; then break; fi
        sleep 1
    done
    for process in x11vnc Xvfb run_ssh.sh ssh run_socat.sh socat; do
        pkill -TERM -x "$process" 2>/dev/null || true
    done
}

start_watchdog() {
    # An unexpected monitor failure only restarts the monitor after a delay;
    # it is not evidence that the broker/container needs restarting.
    local delay=$1
    (
        if [ "$delay" -gt 0 ]; then sleep "$delay"; fi
        exec bash "${SCRIPT_PATH}/mmr-watchdog.sh" \
            "${SCRIPT_PATH}/mmr-healthcheck.sh" "$RECOVERY_STATE_DIR"
    ) &
    watchdog_pid=$!
}

trap 'exit 0' SIGINT SIGTERM
trap stop_ibc EXIT

start_xvfb() {
	# start Xvfb
	echo ".> Starting Xvfb server (screen: ${XVFB_SCREEN:-1920x1080x24})"
	DISPLAY=:1
	export DISPLAY
	rm -f /tmp/.X1-lock
	Xvfb $DISPLAY -ac -screen 0 ${XVFB_SCREEN:-1920x1080x24} &
}

start_vnc() {
	# wait for X11 socket to be ready
	wait_x_socket
	# start VNC server
	file_env 'VNC_SERVER_PASSWORD'
	if [ -n "$VNC_SERVER_PASSWORD" ]; then
		echo ".> Starting VNC server"
		x11vnc -ncache_cr -display :1 -forever -shared -bg -noipv6 -passwd "$VNC_SERVER_PASSWORD" &
		unset_env 'VNC_SERVER_PASSWORD'
	else
		echo ".> VNC server disabled"
	fi
}

start_IBC() {
	echo ".> Starting IBC in ${TRADING_MODE} mode, with params:"
	echo ".>		Version: ${TWS_MAJOR_VRSN}"
	echo ".>		program: ${IBC_COMMAND:-gateway}"
	echo ".>		tws-path: ${TWS_PATH}"
	echo ".>		ibc-path: ${IBC_PATH}"
	echo ".>		ibc-init: ${IBC_INI}"
	echo ".>		tws-settings-path: ${TWS_SETTINGS_PATH:-$TWS_PATH}"
	echo ".>		on2fatimeout: ${TWOFA_TIMEOUT_ACTION}"
	# start IBC -g for gateway
	"${IBC_PATH}/scripts/ibcstart.sh" "${TWS_MAJOR_VRSN}" -g \
		"--tws-path=${TWS_PATH}" \
		"--ibc-path=${IBC_PATH}" "--ibc-ini=${IBC_INI}" \
		"--on2fatimeout=${TWOFA_TIMEOUT_ACTION}" \
		"--tws-settings-path=${TWS_SETTINGS_PATH:-}" &
	_p="$!"
	pid+=("$_p")
	export pid
	echo "$_p" >"/tmp/pid_${TRADING_MODE}"
}

start_process() {
	# set API and socat ports
	set_ports
	# apply settings
	apply_settings
	# forward ports, socat/ssh
	port_forwarding

	start_IBC
}

###############################################################################
#####		Common Start
###############################################################################

# run start scripts
if [ -n "$START_SCRIPTS" ]; then
	run_scripts "$HOME/$START_SCRIPTS"
fi

# start Xvfb
start_xvfb

# setup SSH Tunnel
setup_ssh

# Workaround for ghcr.io/gnzsnz/ib-gateway upstream bug: inst_jre.cfg is
# baked at build time with /tmp/setup/<build-pid>.dir/jre, which is gone
# at runtime, so install4j can't find Java. Rewrite to the colocated JRE.
for cfg in /home/ibgateway/Jts/ibgateway/*/.install4j/inst_jre.cfg; do [ -f "$cfg" ] && echo "${cfg%/.install4j/inst_jre.cfg}/jre" > "$cfg"; done

# Java heap size
set_java_heap

# start VNC server
start_vnc

# run scripts once X environment is up
if [ -n "$X_SCRIPTS" ]; then
	wait_x_socket
	run_scripts "$HOME/$X_SCRIPTS"
fi

###############################################################################
#####		Paper, Live or both start process
###############################################################################

if [ "$TRADING_MODE" == "both" ] || [ "$DUAL_MODE" == "yes" ]; then
	# start live and paper
	DUAL_MODE=yes
	export DUAL_MODE
	# start live first
	TRADING_MODE=live
	# add _live subfix
	_IBC_INI="${IBC_INI}"
	export _IBC_INI
	IBC_INI="${_IBC_INI}_${TRADING_MODE}"
	if [ -n "$TWS_SETTINGS_PATH" ]; then
		_TWS_SETTINGS_PATH="${TWS_SETTINGS_PATH}"
		export _TWS_SETTINGS_PATH
		TWS_SETTINGS_PATH="${_TWS_SETTINGS_PATH}_${TRADING_MODE}"
	else
		# no TWS settings
		_TWS_SETTINGS_PATH="${TWS_PATH}"
		export _TWS_SETTINGS_PATH
		TWS_SETTINGS_PATH="${_TWS_SETTINGS_PATH}_${TRADING_MODE}"
	fi
fi

start_process

if [ "$DUAL_MODE" == "yes" ]; then
	# running dual mode, start paper
	TRADING_MODE=paper
	TWS_USERID="${TWS_USERID_PAPER}"
	export TWS_USERID

	# handle password for dual mode
	if [ -n "${TWS_PASSWORD_PAPER_FILE}" ]; then
		TWS_PASSWORD_FILE="${TWS_PASSWORD_PAPER_FILE}"
		export TWS_PASSWORD_FILE
	else
		TWS_PASSWORD="${TWS_PASSWORD_PAPER}"
		export TWS_PASSWORD
	fi
	# disable duplicate ssh for vnc/rdp
	SSH_VNC_PORT=
	export SSH_VNC_PORT
	# in dual mode, ssh remote always == api port
	SSH_REMOTE_PORT=
	export SSH_REMOTE_PORT
	#
	IBC_INI="${_IBC_INI}_${TRADING_MODE}"
	TWS_SETTINGS_PATH="${_TWS_SETTINGS_PATH}_${TRADING_MODE}"

	sleep 15
	start_process
fi

# run scripts once IBC is running
if [ -n "$IBC_SCRIPTS" ]; then
	run_scripts "$HOME/$IBC_SCRIPTS"
fi

start_watchdog 0
while true; do
    completed_pid=""
    status=0
    wait -n -p completed_pid "${pid[@]}" "$watchdog_pid" || status=$?
    if [ "${completed_pid:-}" = "$watchdog_pid" ] && [ "$status" -ne 42 ]; then
        echo ".> Recovery monitor exited ($status); restarting monitor after 300s."
        start_watchdog 300
        continue
    fi
    # A completed IBC child, or the monitor's confirmed recovery request,
    # ends PID1. Docker's existing unless-stopped policy restarts the gateway.
    exit "$status"
done
