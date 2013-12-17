#!/bin/bash
BASE_DIR=`dirname $0`
. $BASE_DIR/mongo-env.sh

DEFAULT_PORT=27019
DEFAULT_ARGUMENTS="--fork --rest --logappend --quiet"

port=$DEFAULT_PORT
arguments="$DEFAULT_ARGUMENTS"

function show_usage() {
    if [ -n "$1" ]
    then
        echo "$(basename $0): $1"
    fi
    echo -e "usage: $(basename $0) [-p port] [-a arguments]"
}

while getopts ":p:a:" opt; do
  case $opt in
    p)
        port=$OPTARG
    ;;
    a)
        arguments=$OPTARG
    ;;
    \?)
        (show_usage "invalid option -$OPTARG")
        exit $EXIT_INVALID_OPTION
    ;;
    :)
        (show_usage "option -$OPTARG requires an argument")
        exit $EXIT_ARGUMENT_REQUIRED
    ;;
  esac
done

if [ ! -z "$port" ]
then
  arguments="--port $port $arguments"
fi

dbpath="$MONDO_DB_DIR/mongod-configsvr-$port"
mkdir -p $dbpath

logpath="$MONGO_LOG_DIR/mongod-configsvr-$port.log"
mkdir -p $MONGO_LOG_DIR

# start mongod config server
$MONGO_HOME/bin/mongod --configsvr --dbpath $dbpath --logpath $logpath $arguments

tail -n 100 $logpath