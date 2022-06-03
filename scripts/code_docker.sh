#!/bin/bash

CONTNAME=trader-container
echo "ip address: $(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $CONTNAME)"
