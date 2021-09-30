#!/bin/bash

. env.sh

while true; do
    cargo run -- \
        --node-url $NODE_URL \
        --database-url $DATABASE_URL \
        --index-all-contracts \
	"${@}" \
        2>&1 | tee $CONTRACT_ID.$TIMESTAMP.log
    exit

    sleep 10s
done
