#!/bin/bash

#-------------------------------------
# Define script variables and exports
#-------------------------------------

MASTER_SYNC_TYPE=8000002
WORKER_SYNC_TYPE=96669
WORKER_TASKS_TYPE=8000010


mainTraceFile=$1
baseDir=$(pwd)
traceDir=$(dirname $(find . -name $mainTraceFile))

getTaskOffsetAndId(){
    taskFile=$1

    # Task trace info
    task_line=$(grep -h "$WORKER_SYNC_TYPE:[1-9][0-9]*" ${taskFile})

    rest=${task_line#*${WORKER_SYNC_TYPE}}
    typeIndex=$(( ${#task_line} - ${#rest} - ${#WORKER_SYNC_TYPE} ))
    type_first=${task_line:$typeIndex}
    task_id=`expr match ${type_first#$WORKER_SYNC_TYPE:} '\([0-9]\+\)'`

    sync_event=$(echo `expr "$task_line" : '\([0-9]\+:[0-9]\+:[0-9]\+:[0-9]\+:[0-9]\+:[0-9]\+\)'`)

    task_timestamp=${sync_event##[0-9]*:}

    # Getting start time timestam (task)
    start_task_line=$(grep -h "[0-9]\+:[0-9]\+:1:1:1.*40000001:1:*:40000050:" ${taskFile})
    sync_header=${start_task_line##[0-9]*:0:40000001:1:40000050:}
    task_start_time=${sync_header%%:*[0-9]}

    # Master trace info

    # Getting start time timestamp (master)
    main_line=$(grep -h "[0-9]\+:[0-9]\+:1:1:1.*40000001:1:*:40000050:" ${mainTraceFile})
    sync_header=${main_line##[0-9]*:0:40000001:1:40000050:}
    start_time=${sync_header%%:*[0-9]}

    # Getting resouce id for task
    main_line=$(grep -h "$MASTER_SYNC_TYPE:$task_id" ${mainTraceFile})
    sync_header=$(echo `expr "$main_line" : '\([0-9]\+:[0-9]\+:[0-9]\+:[0-9]\+:[0-9]\+:[0-9]\+\)'`)
    resource_id=${sync_header%:*[0-9]}


    offset=$(($task_start_time-$start_time+$task_timestamp))

    indices=$(sed -n /${WORKER_SYNC_TYPE}:/= ${taskFile})
    indices=$(echo $indices | tr " " ",")
}

extract_task_info(){

    task_header=$(echo `expr "$event" : '\([0-9]\+:[0-9]\+:[0-9]\+:[0-9]\+:[0-9]\+:\)'`)
    task_old_info=${event#$task_header}

    old_timestamp="$( expr match $task_old_info '\([0-9]\+\)')"
    new_timestamp=$((old_timestamp+offset))

    task_info_wo_timestamp=${task_old_info#$old_timestamp}
    new_task_info=":${new_timestamp}${task_info_wo_timestamp}"

}

cd $traceDir

taskFiles=$(find tasks/*.prv)
for taskPrv in ${taskFiles[*]}; do
    getTaskOffsetAndId $taskPrv
    events=$(sed -n ${indices}p $taskPrv)
    for event in ${events}; do
        extract_task_info "$event"
        new_event="${resource_id}${new_task_info}"
        echo $new_event >> $mainTraceFile
    done
done
cd $baseDir





