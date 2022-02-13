#!/usr/bin/env bash

cd $(git rev-parse --show-toplevel)

echo "starting quepasa to get the version"
VERSION=`cargo run -- --version | awk '{print $NF}'`
[ $? -eq "0" ] || exit 1
echo "publishing docker under tags quepasa:$VERSION and quepasa:latest.."

docker build -t que-pasa . || exit 1

docker tag que-pasa ghcr.io/tzconnectberlin/que-pasa:latest || exit 1
docker tag que-pasa ghcr.io/tzconnectberlin/que-pasa:$VERSION || exit 1

# docker push ghcr.io/tzconnectberlin/que-pasa:latest || exit 1
docker push ghcr.io/tzconnectberlin/que-pasa:$VERSION
