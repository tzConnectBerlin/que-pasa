#!/bin/bash

. env.sh

psql -c "drop database $DATABASE_NAME"
psql -c "create database $DATABASE_NAME" || exit 1

START_BLOCK=1400000
cargo run -- \
    --node-url $NODE_URL \
    --database-url $DATABASE_URL \
    --index-all-contracts \
    -l $START_BLOCK || exit 1
