#!/usr/bin/env bash

cli=${1:?Please provide the commandline interface executable as first argument}
db=${2:?Please provide the image name for the cassandra database}

source "$(dirname $0)/../lib/utilities.sh"

set -e

if [ -z "$TRAVIS" ]; then
    CASSANDRA_PORT=12423
else
    CASSANDRA_PORT=9042
fi


trap stop_dependencies 0 1 2 5 15
start_dependencies "$db" $CASSANDRA_PORT "$cli test-connection"

$cli test-connection $CASSANDRA_HOST $CASSANDRA_PORT

