#!/bin/sh

retry_count_max=40
sleeptime_between_retry=3
myip=`ifconfig | sed -En 's/127.0.0.1//;s/.*inet (addr:)?(([0-9]*\.){3}[0-9]*).*/\2/p'`

echo "Checking sync_gateway processes"
retry_count=0
while true; do
    result=`curl http://localhost:4985/db?n=1`
    if [ -z "$result" ]; then
        echo "Sync-gateway process is not running yet.  Retrying."
        retry_count=`expr $retry_count + 1`
        if [ $retry_count -gt $retry_count_max ]; then
          echo "Sync-gateway process is not running on ${myip} yet after $retry_count_max retries.   Giving up."
          exit 9
        fi
        sleep $sleeptime_between_retry
    else
        echo "gateway_${index} sync_gateway is running"
        break
    fi
done
