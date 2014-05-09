#!/bin/bash
. sgw_test_config.sh

# Need to have seriesly process running in order to clean the databases
if [ ! -n  “`ps aux | grep \”seriesly\” | grep -v grep`”  ]; then
    nohup seriesly -flushDelay=1s -queryWorkers=4 -root=/root/seriesly-data &
fi

# Flush seriesly databases for gateway and gateload machines
index=0
for ip in ${gateways_ip}; do
    index=`expr $index + 1`
    echo "--- gateway_${index} $ip"
    curl -v -XDELETE http://localhost:3133/gateway_${index}
    curl -v -XPUT http://localhost:3133/gateway_${index}
done

index=0
for ip in ${gateloads_ip}; do
    index=`expr $index + 1`
    echo "--- gateload_${index} $ip"
    curl -v -XDELETE http://localhost:3133/gateload_${index}
    curl -v -XPUT http://localhost:3133/gateload_${index}
done

