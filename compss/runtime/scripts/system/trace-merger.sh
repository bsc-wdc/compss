#!/bin/bash

#-------------------------------------
# Define script variables and exports
#-------------------------------------

MASTER_SYNC_TYPE=8000002
WORKER_SYNC_TYPE=96669
WORKER_TASKS_TYPE=8000010


mainTraceFile=$1
baseDir=$(pwd)
traceDir="trace"

getTaskOffsetAndId(){
    taskFile=$1
  
    # Task trace info
    task_line=$(grep -h "$WORKER_SYNC_TYPE:[1-9][0-9]*" $taskFile)
 
    rest=${task_line#*${WORKER_SYNC_TYPE}}
    typeIndex=$(( ${#task_line} - ${#rest} - ${#WORKER_SYNC_TYPE} ))
    type_first=${task_line:$typeIndex}
    task_id=`expr match ${type_first#$WORKER_SYNC_TYPE:} '\([0-9]\+\)'`
 

    # Use event finish to sync
    sync_event=$(echo `expr "$task_line" : '\([0-9]\+:[0-9]\+:[0-9]\+:[0-9]\+:[0-9]\+:[0-9]\+\)'`)

    task_timestamp=${sync_event##[0-9]*:}

    # Master trace info
    
    main_line=$(grep -h "$MASTER_SYNC_TYPE:$task_id" $mainTraceFile)

    sync_header=$(echo `expr "$main_line" : '\([0-9]\+:[0-9]\+:[0-9]\+:[0-9]\+:[0-9]\+:[0-9]\+\)'`)
    main_timestamp=${sync_header##[0-9]*:}
    resource_id=${sync_header%:*[0-9]}


    offset=$(($main_timestamp-$task_timestamp))
}

extract_task_info(){
    event=$1

    task_header=$(echo `expr "$event" : '\([0-9]\+:[0-9]\+:[0-9]\+:[0-9]\+:[0-9]\+:\)'`)
    task_old_info=${event#$task_header}

    old_timestamp="$( expr match $task_old_info '\([0-9]\+\)')"
    new_timestamp=$((old_timestamp+offset))

    task_info_wo_timestamp=${task_old_info#$old_timestamp}
    new_task_info=":${new_timestamp}${task_info_wo_timestamp}"

}

cd $traceDir

taskFiles=$(find tmp.*/*.prv)
for taskPrv in ${taskFiles[*]}; do
    getTaskOffsetAndId $taskPrv
    events=$(grep $WORKER_TASKS_TYPE $taskPrv)
    events+="
$(grep $WORKER_SYNC_TYPE $taskPrv)"
    for event in ${events}; do
        extract_task_info "$event"
        new_event="${resource_id}${new_task_info}"
    done
done
cd $baseDir





