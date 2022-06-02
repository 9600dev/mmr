#!/bin/sh

set -e

CONTNAME=trader-container
IMGNAME=trader
RELEASE=21.04

BUILDDIR=$(cd $(dirname "$0")/..; pwd)

usage() {
    echo "usage: $(basename $0) [options]"
    echo
    echo "  -c|--containername <name> (default: trader-container)"
    echo "  -i|--imagename <name> (default: trader)"
    echo "  -r (run container and bash into it)"
    rm_builddir
}

print_info() {
    echo
    echo "to remove the container use: docker rm -f $CONTNAME"
    echo "to remove the related image use: docker rmi $IMGNAME"
    echo ""
}

clean_up() {
    echo "clean_up called"
    sleep 1
    docker rm -f $CONTNAME >/dev/null 2>&1 || true
    rm_builddir
}

rm_builddir() {
    exit 0
}

container_clean() {
    if [ -n "$(docker ps -f name=$CONTNAME -q)" ]; then
        echo "Container $CONTNAME already running, removing"
        docker rm -f $CONTNAME >/dev/null
    fi

    if [ -n "$(docker container ls -a | grep $CONTNAME)" ]; then
        echo "Container $CONTNAME exists, removing"
        docker rm -f $CONTNAME >/dev/null
    fi
}

run_docker() {
    echo "running container $CONTNAME with this command:"
    echo " -- docker run --name $CONTNAME -ti -e TRADER_CONFIG='/home/trader/mmr/configs/trader.yaml -p 2222:22 -p starting -p 7496:7496 -p 6379:6379 -p 27017:27017 -p 5910:5900 -p 5911:5901 -p 8081:8081 --tmpfs /run --tmpfs /run/lock --tmpfs /tmp -v /lib/modules:/lib/modules:ro -d $IMGNAME"
    echo ""

    docker run \
        --name $CONTNAME \
        -ti \
        -p 2222:22 \
        -p 7496:7496 \
        -p 6379:6379 \
        -p 27017:27017 \
        -p 5900:5900 \
        -p 5901:5901 \
        -p 8081:8081 \
        --tmpfs /run \
        --tmpfs /run/lock \
        --tmpfs /tmp \
        -v /lib/modules:/lib/modules:ro \
        -d $IMGNAME || clean_up

    echo ""
    # echo ssh into container via ssh trader:trader@$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $CONTNAME)
    echo "ip address: $(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $CONTNAME)"
    echo "ssh into container via ssh trader@localhost -p 2222, password is 'trader'"
    echo ""
}

trap clean_up 1 2 3 4 9 15

while [ $# -gt 0 ]; do
       case "$1" in
               -c|--containername)
                       [ -n "$2" ] && CONTNAME=$2 shift || usage
                       ;;
               -i|--imagename)
                       [ -n "$2" ] && IMGNAME=$2 shift || usage
                       ;;
               -r)
                       container_clean
                       run_docker
                       exit 0
                       ;;
               -h|--help)
                       usage
                       ;;
       esac
       shift
done

# remove existing containers
container_clean

# build
DOCKER_BUILDKIT=1 docker build -t $IMGNAME --force-rm=true --rm=true $BUILDDIR $1 || clean_up

print_info

# start the detached container
run_docker


