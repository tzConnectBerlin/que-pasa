#!/bin/bash

. env.sh


while true; do
    cargo run -- --node-url $NODE_URL --database-url $DATABASE_URL -c $CONTRACT_ID 2>&1 |tee $CONTRACT_ID.$TIMESTAMP.log
done
