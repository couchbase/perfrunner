#!/bin/bash
. sgw_test_config.sh

index=0
for ip in ${gateways_ip}; do
    index=`expr $index + 1`
    echo "--- gateway_${index} $ip"
    nohup sample -v http://${ip}:4985/_expvar http://localhost:3133/gateway_${index}  &
done

index=0
for ip in ${gateloads_ip}; do
    index=`expr $index + 1`
    echo "--- gateload_${index} $ip"
    nohup sample -v http://${ip}:9876/debug/vars http://localhost:3133/gateload_${index}  &
    # To get over problem that the last one does not work
    sleep 1
done