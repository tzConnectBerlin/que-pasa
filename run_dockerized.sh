#!/bin/sh

docker run \
       --network host \
       ghcr.io/tzconnectberlin/que-pasa:1.3.0 \
       "$@"
