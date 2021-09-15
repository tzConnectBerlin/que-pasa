#!/bin/bash

. env.sh


while true; do
    cargo run -- \
        --node-url $NODE_URL \
        --database-url $DATABASE_URL \
        -c $CONTRACT_ID \
        --bcd-url https://api.better-call.dev/v1 \
        --network granadanet \
        2>&1 | tee $CONTRACT_ID.$TIMESTAMP.log

    sleep 10s
done
