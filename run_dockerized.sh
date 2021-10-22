#!/bin/sh

docker run \
       --network host \
       ghcr.io/tzconnectberlin/que-pasa:latest \
       "$@"
