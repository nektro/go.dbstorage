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

#
# postgres
#
echo
docker run --name some -e POSTGRES_PASSWORD=mysecretpassword -e POSTGRES_DB=dbname -p 5432:5432 -d postgres
sleep 10s
POSTGRES_URL='localhost:5432' POSTGRES_USER='postgres' POSTGRES_PASSWORD='mysecretpassword' POSTGRES_DATABASE='dbname' POSTGRES_SSLMODE='disable' go test -v -run Postgres
docker stop some
docker rm some
