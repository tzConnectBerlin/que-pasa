#!/bin/bash

[ -z $PGPORT ] && export PGPORT=5432
[ -z $PGPASSWORD ] && export PGPASSWORD=quepasa
[ -z $PGUSER ] && export PGUSER=quepasa
[ -z $PGDATABASE ] && export PGDATABASE=tezos

docker run $DOCKER_ARGS  \
    -p $PGPORT:5432 \
    -e POSTGRES_PASSWORD=$PGPASSWORD \
    -e POSTGRES_USER=$PGUSER \
    -e POSTGRES_DB=$PGDATABASE \
    postgres "$@"
