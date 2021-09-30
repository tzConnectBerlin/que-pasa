#!/bin/bash

cd $(git rev-parse --show-toplevel)

docker build -t que-pasa . || exit 1
docker tag que-pasa ghcr.io/tzconnectberlin/que-pasa:latest
docker push ghcr.io/tzconnectberlin/que-pasa:latest
