#!/bin/bash

psql -c 'drop database tezos'
psql -c 'create database tezos'
cargo run -- -c KT1LYbgNsG2GYMfChaVCXunjECqY59UJRWBf generate-sql|psql tezos
cargo run -- -c KT1LYbgNsG2GYMfChaVCXunjECqY59UJRWBf -l 147816,147814,147813,147812,147811,147810,147809,147808,147807,147806,147805
