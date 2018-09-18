#!/usr/bin/env bash

function getTiming(){
    start=$1
    end=$2

    start_s=`echo $start | cut -d '.' -f 1`
    start_ns=`echo $start | cut -d '.' -f 2`
    end_s=`echo $end | cut -d '.' -f 1`
    end_ns=`echo $end | cut -d '.' -f 2`

    time_micro=$(( (10#$end_s-10#$start_s)*1000000 + (10#$end_ns/1000 - 10#$start_ns/1000) ))
    time_ms=`expr $time_micro/1000  | bc `

    echo `date +%Y-%m-%d_%T`": update using time: $time_ms ms" >> update_strategy_shell_log.`date +%Y%m%d`
}

echo `date +%Y-%m-%d_%T` ": start to update strategy." >> update_strategy_shell_log.`date +%Y%m%d`;
begin_time=`date +%s.%N`

RESULT=`curl http://127.0.0.1:7002/update-all-redirect-strategy`;

end_time=`date +%s.%N`
getTiming $begin_time $end_time

echo -e `date +%Y-%m-%d_%T` ": update result :" $RESULT >> update_strategy_shell_log.`date +%Y%m%d`;

exit 0;
