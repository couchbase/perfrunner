#!/bin/sh

retry_count_max=20
sleeptime_between_retry=3

echo "Checking sync_gateway processes"
retry_count=0
while true; do
    result=`curl http://localhost:4985/db?n=1`
    if [ -z "$result" ]; then
        echo "Sync-gateway process is not running yet.  Retrying."
        retry_count=`expr $retry_count + 1`
        if [ $retry_count -gt $retry_count_max ]; then
          echo "Sync-gateway process is not running yet after $retry_count_max retries.   Giving up."
          exit 9
        fi
        sleep $sleeptime_between_retry
    else
        echo "gateway_${index} sync_gateway is running"
        break
    fi
done
