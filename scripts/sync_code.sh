#!/bin/bash

HOST=`/sbin/ip route|awk '/default/ { print $3 }'`

rsync -aP $1@$HOST:/home/trader/mmr/ /home/trader/mmr/
