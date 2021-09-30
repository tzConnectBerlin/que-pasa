#!/bin/sh

ENV_FILE=env.example.sh
echo "starting with env settings from file $ENV_FILE: `cat $ENV_FILE`"

docker build -t que_pasa . || exit 1

docker run \
       --network host \
       -e $ENV_FILE \
       que_pasa "$@"
