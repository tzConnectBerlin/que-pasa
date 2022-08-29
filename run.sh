#!/bin/bash

. env.sh


while true; do
    cargo run -- \
        --node-url $NODE_URL \
        --database-url $DATABASE_URL \
        --contracts contract_alias=$CONTRACT_ID \
        --bcd-url https://api.better-call.dev/v1 \
        --bcd-network ghostnet \
        2>&1 | tee $CONTRACT_ID.$TIMESTAMP.log
    sleep 10s
done
