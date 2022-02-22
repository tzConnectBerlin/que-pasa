#!/usr/bin/env bash
cd $(git rev-parse --show-toplevel)/script

command -v stunnel >/dev/null 2>&1 || {
    echo >&2 'stunnel is a required dependency, it is currently not installed.'
    exit 1
}

[ -z $PGPORT ] && export PGPORT=5432

[ -z $PGPORT_REMOTE ] && export PGPORT_REMOTE=5432
[ -z $PGHOST_REMOTE ] && {
    echo 'expected defined PGHOST_REMOTE env var'
    exit 1
}


conf_file=`mktemp`
echo "
foreground = yes

[pgcli]
client  = yes
protocol = pgsql
accept  = localhost:$PGPORT
connect = $PGHOST_REMOTE:$PGPORT_REMOTE
debug   = 3
options = NO_TICKET
" > $conf_file

stunnel $conf_file
