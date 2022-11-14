#!/bin/bash

set -o errexit -o pipefail -o noclobber -o nounset

CONTNAME=mmr-container
IMGNAME=mmr-image
RELEASE=20.04

# usually /home/trader/mmr
BUILDDIR=$(cd $(dirname "$0"); pwd)

echo_usage() {
    echo "usage: docker.sh -- helper script to manage building and deploying MMR into docker containers"
    echo
    echo "  -b (build image from Dockerfile)"
    echo "  -c (clean docker [remove all images named mmr-image and containers named mmr-container])"
    echo "  -r (run container)"
    echo "  -s (sync code to running container)"
    echo "  -a (sync all files in mmr to running container)"
    echo "  -r (run container and ssh into it)"
    echo "  -g (go: clean, build, run and ssh into container)"
    echo "  -n|--container_name <name> (default: mmr-container)"
    echo "  -i|--image_name <name> (default: mmr-trader)"
    echo ""
}

b=n c=n r=n s=n a=n g=n

while [[ $# -gt 0 ]]; do
  case $1 in
    -b|--build)
      b=y
      shift # past argument
      ;;
    -c|--clean)
      c=y
      shift # past argument
      ;;
    -r|--run)
      r=y
      shift # past argument
      ;;
    -g|--go)
      g=y
      shift # past argument
      ;;
    -s|--sync)
      s=y
      shift
      ;;
    -a|--sync_all)
      a=y
      shift
      ;;
    -i|--image_name)
      IMGNAME="$2"
      shift
      shift
      ;;
    -n|--container_name)
      CONTNAME="$2"
      shift
      shift
      ;;
    -*|--*)
      echo "Unknown option $1"
      echo_usage
      exit 1
      ;;
    *)
      POSITIONAL_ARGS+=("$1") # save positional arg
      shift # past argument
      ;;
  esac
done

# handle non-option arguments
if [[ $# -ne 1 ]]; then
    echo_usage
fi

clean() {
    echo "Cleaning images and containers"
    if [ -n "$(docker ps -f name=$CONTNAME -q)" ]; then
        echo "Container $CONTNAME already running, removing anyway"
        docker rm -f $CONTNAME
    fi

    if [ -n "$(docker container ls -a | grep $CONTNAME)" ]; then
        echo "Container $CONTNAME exists, removing"
        docker rm -f $CONTNAME
    fi

    if [ -n "$(docker image ls -a | grep $IMGNAME)" ]; then
        echo "Image $IMGNAME exists, removing"
        docker image prune -f
        docker image rm -f $IMGNAME
    fi
}

run() {
    echo "running container $CONTNAME with this command:"
    echo ""
    echo " $ docker run --name $CONTNAME -ti -p 2222:22 -p starting -p 7496:7496 -p 6379:6379 -p 27017:27017 -p 5900:5900 -p 5901:5901 -p 8081:8081 --tmpfs /run --tmpfs /run/lock --tmpfs /tmp -v /lib/modules:/lib/modules:ro -d $IMGNAME"
    echo ""

    if [ ! "$(docker image ls -a | grep $IMGNAME)" ]; then
      echo "cant find image named $IMGNAME to run"
      exit 1
    fi

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
        -d $IMGNAME

    echo ""
    # echo ssh into container via ssh trader:trader@$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $CONTNAME)
    echo "container: $CONTNAME"
    echo "ip address: $(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $CONTNAME)"
    echo "ssh'ing into container via ssh trader@localhost -p 2222, password is 'trader'"
    echo ""
    ssh trader@localhost -p 2222
}

build() {
    echo "building mmr into image $IMGNAME and container $CONTNAME"
    echo ""
    echo "DOCKER_BUILDKIT=1 docker buildx build --platform linux/amd64 -t $IMGNAME --force-rm=true --rm=true $BUILDDIR"
    echo ""
    DOCKER_BUILDKIT=1 docker buildx build --platform linux/amd64 -t $IMGNAME --force-rm=true --rm=true $BUILDDIR
}

sync() {
    echo "syncing code directory to $CONTNAME"
    echo ""
    CONTID="$(docker ps -aqf name=$CONTNAME)"
    if [[ $CONTID == "" ]]; then
      echo "cant find running container that matches name $CONTNAME"
      exit 1
    fi
    echo "container id: $CONTID"
    echo " $ rsync -e 'docker exec -i' -av --delete $BUILDDIR/ $CONTID:/home/trader/mmr/ --exclude='.git' --filter=\"dir-merge,- .gitignore\""
    echo ""
    rsync -e 'docker exec -i' -av --delete $BUILDDIR/ $CONTID:/home/trader/mmr/ --exclude='.git' --filter="dir-merge,- .gitignore"
}

sync_all() {
    echo "syncing entire mmr directory to $CONTNAME"
    echo ""
    CONTID="$(docker ps -aqf name=$CONTNAME)"
    if [[ $CONTID == "" ]]; then
      echo "cant find running container that matches name $CONTNAME"
      exit 1
    fi
    echo "container id: $CONTID"
    echo " $ rsync -e 'docker exec -i' -av $BUILDDIR/ $CONTID:/home/trader/mmr/ --exclude='.git'"
    echo ""
    rsync -e 'docker exec -i' -av --delete $BUILDDIR/ $CONTID:/home/trader/mmr/ --exclude='.git'
}

echo "build: $b, clean: $c, run: $r, sync: $s, sync_all: $a, go: $g, image_name: $IMGNAME, container_name: $CONTNAME"

if [[ $b == "y" ]]; then
    build
fi
if [[ $c == "y" ]]; then
    clean
fi
if [[ $r == "y" ]]; then
    run
fi
if [[ $s == "y" ]]; then
    sync
fi
if [[ $a == "y" ]]; then
    sync_all
fi
if [[ $g == "y" ]]; then
    clean; build; run
fi