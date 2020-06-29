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

#
# mysql
#
echo
docker run --name some --health-cmd='mysqladmin ping --silent' -e MYSQL_ROOT_PASSWORD=mysecretpassword -e MYSQL_DATABASE=dbname -p 3306:3306 -d mysql
wait_for_healthy some
MYSQL_URL='localhost:3306' MYSQL_USER='root' MYSQL_PASSWORD='mysecretpassword' MYSQL_DATABASE='dbname' go test -v -run Mysql
docker stop some
docker rm some
