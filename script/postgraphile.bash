#!/usr/bin/env bash
cd $(git rev-parse --show-toplevel)

if [ -z $DATABASE_URL ]; then
    echo "required variable DATABASE_URL unset"
    exit 1
fi

postgraphile \
    --live \
    --append-plugins `pwd`/graphql/multiple.js,@graphile/subscriptions-lds,@graphile-contrib/pg-order-by-related \
    --owner-connection $DATABASE_URL \
    --connection $DATABASE_URL \
    "${@}"
