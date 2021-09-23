#!/bin/sh


docker build -t que_pasa .

env_file=$1
docker run \
       --network host \
       -e $env_file \
       que_pasa \
       que-pasa ${@:2}
