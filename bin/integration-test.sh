#!/usr/bin/env bash

cli=${1:?Please provide the commandline interface executable as first argument}

set -eu

CONTAINER_NAME=db
CASSANDRA_HOST=127.0.0.1
CASSANDRA_PORT=12423

function start_dependencies() {
    echo starting dependencies
    docker run --rm --name $CONTAINER_NAME -d -p $CASSANDRA_HOST:$CASSANDRA_PORT:9042 --expose 9042 cassandra:2.1 1>&2
    while ! nc -d $CASSANDRA_HOST $CASSANDRA_PORT -w 1 -G 1; do
        echo "Waiting for cassandra on $CASSANDRA_HOST:$CASSANDRA_PORT" 1>&2
        sleep 1
    done
    echo "Cassandra up on $CASSANDRA_HOST:$CASSANDRA_PORT" 1>&2
}

function stop_dependencies() {
    echo stopping dependencies ...
    docker rm --force $CONTAINER_NAME >/dev/null
}

trap stop_dependencies 0 1 2 5 15
start_dependencies

$cli test-connection $CASSANDRA_HOST $CASSANDRA_PORT

