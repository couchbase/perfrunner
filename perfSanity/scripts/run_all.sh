#!/bin/bash
lines=(`cat tests/perf_sanity.conf`)
len=${#lines[@]}
arrLen=$((len / 3))

failure_count=0
pass_count=0

for (( i=0; i<${arrLen}; i++ )); do
    test_config=tests/`echo ${lines[$i*3]}`
    echo;echo;echo The test is $test_config
    spec_file=`echo ${lines[$i*3+1]}`
    kpis=`echo ${lines[$i*3+2]}`

    cp clusters/$spec_file perf-regression.spec
    ./scripts/setup.sh
    ./scripts/workload_dev.sh >/tmp/perf-sanity.log

    res=`python scripts/perf_analyzer.py -f /tmp/perf-sanity.log -p $kpis`
    size=${#res}
    echo size is $size
    if [ $size -ne 0 ]; then
       echo The test failed
       failures[$failure_count]="$test_config: $res"
       failure_count=$((failure_count+1))
    else
       echo The test passed
       passes[$pass_count]="$test_config"
       pass_count=$((failure_count+1))
    fi
done


echo Pass count is $pass_count
echo Failure count is $failure_count
echo;echo
if [ "$pass_count" -gt 0 ]; then
    echo -e '\n\nThe following tests passed:'
    for i in "${passes[@]}"
    do
        echo $i
    done
fi

if [ "$failure_count" -gt 0 ]; then
    echo -e '\n\nThe following failures occurred:'
    for i in "${failures[@]}"
    do
        echo
        echo $i
    done
    echo;echo
    exit 1

else
   echo No failures
   echo;echo
   exit 0
fi
