#!/bin/bash
. sgw_test_config.sh

if [ ${profiling_freq} -eq 0 ]; then
    echo "=== ERROR - profiling_seq is 0.  This script should not be called"  >> $outfile
    echo "=== ERROR - profiling_seq is 0.  This script should not be called"
    exit;
fi

profile_duration_cpu=10
profile_duration_heap=10
outfile=test_info.txt
sleep_time=`expr ${profiling_freq} - ${profile_duration_cpu}`
sleep_time=`expr ${sleep_time} - ${profile_duration_heap}`
loop_count=`expr ${run_time} / ${profiling_freq}`
profile_sequence=1
rm -rf *.prof

pid=`pgrep sync_gateway`
if [ -n "$pid" ]
then
        while :
        do
            sleep ${sleep_time}

            # Profiling
            echo "$(date +"%Y%m%d-%H%M%S"): Start profiling: $profile_sequence"
            echo "$(date +"%Y%m%d-%H%M%S"): Start profiling: $profile_sequence" >> $outfile

            curl -v -XPOST http://localhost:4985/_profile -d '{"file":"/root/profile_cpu.prof"}'
            sleep ${profile_duration_cpu}
            curl -v -XPOST http://localhost:4985/_profile -d '{}'
            cp profile_cpu.prof profile_cpu_${profile_sequence}.prof

            curl -v -XPOST http://localhost:4985/_profile/heap -d '{"file":"/root/profile_heap.prof"}'
            sleep ${profile_duration_heap}
            curl -v -XPOST http://localhost:4985/_profile -d '{}'
            cp profile_heap.prof profile_heap_${profile_sequence}.prof

            echo "$(date +"%Y%m%d-%H%M%S"): Done profiling: $profile_sequence"
            echo "$(date +"%Y%m%d-%H%M%S"): Done profiling: $profile_sequence" >> $outfile

            if [ $profile_sequence -ge $loop_count ]
            then
                echo "Done profiling and save results in profiling.tar.gz.  To untar: tar xvfz profiling.tar.gz"
                echo "Done profiling and save results in profiling.tar.gz.  To untar: tar xvfz profiling.tar.gz" >> $outfile
                tar cvzf profiling.tar.gz profile_*.prof
                killall -9 sgw_profiling.sh
            else
                profile_sequence=`expr $profile_sequence + 1`
            fi
        done
else
  echo "=== ERROR - sync_gateway process is not running"  >> $outfile
  echo "=== ERROR - sync_gateway process is not running"
fi