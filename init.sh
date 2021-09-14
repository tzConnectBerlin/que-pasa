#!/bin/bash

set -e

. env.sh

cargo run -- --node-url $NODE_URL --database-url $DATABASE_URL --init
