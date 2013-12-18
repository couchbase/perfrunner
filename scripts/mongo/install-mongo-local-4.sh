#!/bin/bash
. mongo-env.sh

wget -q http://downloads.mongodb.org/linux/mongodb-linux-x86_64-${MONGO_VERSION}.tgz -P /tmp
tar xzf /tmp/mongodb-linux-x86_64-${MONGO_VERSION}.tgz -C /tmp
mv -f /tmp/mongodb-linux-x86_64-${MONGO_VERSION}/ /usr/lib/mongodb
rm -f /tmp/mongodb-linux-x86_64-${MONGO_VERSION}.tgz

hostname=`hostname`

case $hostname in
    node1 )
        ./start-mongod.sh -r rs1 -p 27018 -d /data
        ./start-mongod.sh -r rs4 -p 27019 -d /data1
        ./start-mongod-configsvr.sh -p 27017 ;;
    node2 )
        ./start-mongod.sh -r rs2 -p 27018 -d /data
        ./start-mongod.sh -r rs1 -p 27019 -d /data1
        ./start-mongod-configsvr.sh -p 27017 ;;
    node3 )
        ./start-mongod.sh -r rs3 -p 27018 -d /data
        ./start-mongod.sh -r rs2 -p 27019 -d /data1
        ./start-mongod-configsvr.sh -p 27017 ;;
    node4 )
        ./start-mongod.sh -r rs4 -p 27018 -d /data
        ./start-mongod.sh -r rs3 -p 27019 -d /data1 ;;
esac
