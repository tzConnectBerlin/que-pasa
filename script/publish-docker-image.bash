#!/bin/bash

cd $(git rev-parse --show-toplevel)

docker build -t que-pasa . || exit 1
docker push ghcr.io/tzconnectberlin/que-pasa:latest
