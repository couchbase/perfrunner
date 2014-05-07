#!/bin/bash
. sgw_test_config.sh

outfile=test_info.txt
sleep_time=30

echo "Turning off sync-gateway logging"
myip=`ifconfig | sed -En 's/127.0.0.1//;s/.*inet (addr:)?(([0-9]*\.){3}[0-9]*).*/\2/p'`
echo "Before"
curl http://${myip}:4985/_logging
curl -XPUT http://localhost:4985/_logging -d '{}'
echo
echo "After"
curl http://localhost:4985/_logging
echo

pid=`pgrep sync_gateway`
if [ -n "$pid" ]
then
        # build the command to get socketsToDB
        cmd="egrep \""
        for ip in ${dbs_ip}; do
            cmd="${cmd}${ip}|"
        done
        cmd=${cmd%?}
        cmd="$cmd\""

        while :
        do
            memcpu=`top -bn1d1 -p $pid | grep $pid | awk '{print $6, $9}' | sed "s/m//"`
            swap=`cat /proc/${pid}/status | grep VmSwap | awk '{print $2}'`

            netstat -lpnta > tmp_info.txt
            sockets=`cat tmp_info.txt | wc -l`
            socketsToDB=`cat tmp_info.txt | $cmd | wc -l`
            # View is using port 8091, or 8092
            socketsToDB_view=`cat tmp_info.txt | $cmd | grep :809 | wc -l`
            socketsToOthers=`expr $sockets - $socketsToDB`
            output_line="$(date +"%Y%m%d-%H%M%S"): sockets:$sockets - toDB:$socketsToDB - view:$socketsToDB_view - toOthers:$socketsToOthers  - mem/cpu:$memcpu - swap:$swap"
            echo  $output_line >> $outfile

            sleep $sleep_time
        done
else
  echo "=== ERROR - sync_gateway process is not running"  >> $outfile
  echo "=== ERROR - sync_gateway process is not running"
fi