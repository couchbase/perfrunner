#!/bin/bash
BASE_DIR=`dirname $0`
. $BASE_DIR/mongo-env.sh

DEFAULT_PORT=27018
DEFAULT_ARGUMENTS="--fork --logappend"

function show_usage() {
    if [ -n "$1" ]
    then
        echo "$(basename $0): $1"
    fi
    echo -e "usage: $(basename $0) [-p port] [-a arguments]"
}

port=$DEFAULT_PORT
arguments=$DEFAULT_ARGUMENTS

while getopts ":p:c:a:" opt; do
  case $opt in
    p)
        port=$OPTARG
    ;;
    c)
        configdb=$OPTARG
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

if [ -z "$configdb" ]
then
  configdb=$MONGO_CONFIGDB
fi

logpath="$MONGO_LOG_DIR/mongos-$port.log"
mkdir -p $MONGO_LOG_DIR

# run mongos router with --configdb parameter indicating the location of the config database(s)
$MONGO_HOME/bin/mongos --configdb $configdb --logpath $logpath $arguments

tail -n 100 $logpath