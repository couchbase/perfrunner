#!/bin/bash
hostname=`hostname`

case $hostname in
    node1 )
        ./start-mongos.sh -c node1:27017,node2:27017,node3:27017 -p 27020;;
    node2 )
        ./start-mongos.sh -c node1:27017,node2:27017,node3:27017 -p 27020;;
    node3 )
        ./start-mongos.sh -c node1:27017,node2:27017,node3:27017 -p 27020;;
    node4 )
        ./start-mongos.sh -c node1:27017,node2:27017,node3:27017 -p 27020;;
esac
