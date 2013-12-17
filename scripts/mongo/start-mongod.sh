#!/bin/bash
BASE_DIR=`dirname $0`
. $BASE_DIR/mongo-env.sh

EXIT_INVALID_OPTION=1
EXIT_ARGUMENT_REQUIRED=2

DEFAULT_PORT=27017
DEFAULT_ARGUMENTS="--fork --logappend --rest"

port=$DEFAULT_PORT
arguments="$DEFAULT_ARGUMENTS"

function show_usage() {
    if [ -n "$1" ]
    then
        echo "$(basename $0): $1"
    fi
    echo -e "usage: $(basename $0) [-r replica set name] [-p port] [-d data path] [-a arguments]"
}

while getopts ":r:p:d:a:" opt; do
  case $opt in
    r)
        replica_set=$OPTARG
    ;;
    p)
        port=$OPTARG
    ;;
    d)
        dbpath=$OPTARG
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

if [ ! -z "$replica_set" ]
then
  arguments="--replSet $replica_set $arguments"
fi

if [ -z "$dbpath" ]
then
  dbpath="$MONDO_DB_DIR/mongod-$port"
  mkdir -p $dbpath
fi

if [ ! -z "$port" ]
then
  arguments="--port $port $arguments"
fi

logpath="$MONGO_LOG_DIR/mongod-$port.log"
mkdir -p $MONGO_LOG_DIR

$MONGO_HOME/bin/mongod --dbpath $dbpath --logpath $logpath $arguments

tail -n 100 $logpath
