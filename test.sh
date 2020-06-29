#!/usr/bin/env bash

set -e
# set -x
wait_for_healthy() {
    while [ $(docker inspect --format "{{json .State.Health.Status }}" $1) != "\"healthy\"" ]; do
        printf "."
        sleep 1
    done
    printf "\n"
}


# sqlite
go test -v -run Sqlite
rm ./.test.db
