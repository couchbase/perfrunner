#!/bin/bash

type=$1
if [ $# -eq 0 ]; then
    echo 'Require parameter to indicate gateway or gateload'
fi

panic_count=`egrep  -n "panic"  /root/${type}.log | wc -l`
if [ $panic_count -eq 0 ]; then
    echo 'No panic in the log'
else
    echo "$panic_count panic in the log:"
    egrep -i  "panic"  /root/${type}.log
fi

error_count=`egrep  -n "error"  /root/${type}.log | wc -l`
if [ $error_count -eq 0 ]; then
    echo 'No error in the log'
else
    echo "$error_count panic in the log:"
    echo "First 5 errors in the log"
    egrep -i -m 5  "error"  /root/${type}.log
    echo "Last 5 errors in the log"
    tac /root/${type}.log | egrep -i -m 5  "error"  /root/${type}.log
fi

if [ $type == "gateway" ]; then
    out_of_order_count=`egrep  -n "Received out-of-order"  /root/${type}.log | wc -l`
    if [ $out_of_order_count -eq 0 ]; then
        echo 'No "Received out-of-order change" in the log'
    else
        echo "$out_of_order_count \"Received out-of-order change\" in the log:"
        egrep -i  -m 5 "Received out-of-order"  /root/${type}.log
    fi
fi