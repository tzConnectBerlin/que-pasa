#!/bin/bash

set -e

. env.sh

psql -c "drop database $DATABASE_NAME"
psql -c "create database $DATABASE_NAME"
cargo run -- -c $CONTRACT_ID --node-url $NODE_URL --database-url $DATABASE_URL --bcd-url https://api.better-call.dev/v1 --network granadanet generate-sql | psql f1
cargo run -- -c $CONTRACT_ID --node-url $NODE_URL --database-url $DATABASE_URL --bcd-url https://api.better-call.dev/v1 --network granadanet --init
