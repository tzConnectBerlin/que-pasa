#!/bin/bash

export NODE_URL=http://florence.newby.org:8732
. .env
psql $DATABASE_URL -c 'drop database tezos'
psql $DATABASE_URL -c 'create database tezos'
cargo run -- --contracts pmm=KT1LYbgNsG2GYMfChaVCXunjECqY59UJRWBf -l 182160,182123,147816,147814,147813,147812,147811,147810,147809,147808,147807,147806,147805
