#!/bin/bash
. mongo-env.sh

hostname=`hostname`

case $hostname in
    node1 )
        $MONGO_HOME/bin/mongo $MONGO_ROUTER/UserDatabase --quiet js/init-index.js ;;
    node3 )
        $MONGO_HOME/bin/mongo $MONGO_ROUTER/UserDatabase --quiet js/init-index.js ;;
esac
