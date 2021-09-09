#!/bin/bash

[ -z $PGPORT ] && export PGPORT=5432
[ -z $PGPASS ] && export PGPASS=quepasa
[ -z $PGUSER ] && export PGUSER=quepasa
[ -z $PGDATABASE ] && export PGDATABASE=tezos

docker run \
    -p $PGPORT:5432 \
    -e POSTGRES_PASSWORD=$PGPASS \
    -e POSTGRES_USER=$PGUSER \
    -e POSTGRES_DB=$PGDATABASE \
    postgres "$@"
