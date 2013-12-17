#!/bin/bash
. mongo-env.sh

$MONGO_HOME/bin/mongo --quiet $MONGO_ROUTER/admin js/init-sharding-4.js
