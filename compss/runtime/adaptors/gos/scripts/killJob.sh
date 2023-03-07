#!/bin/bash


# shellcheck disable=SC2154


#-------------------------------------
# Initialize Response Files
#-------------------------------------
isInteractive=$1
responsePath=$2
idJob=$(cat $responsePath | awk '{print $1}')

if [ isInteractive = "true" ]; then
  echo "kill -9 $idJob"
  exit 1
fi

cfgPath=$3
# shellcheck disable=SC1090
source "$cfgPath"
if [ -n "$QUEUE_JOB_CANCEL_CMD" ];then
  # shellcheck disable=SC1090
  source "${cfgPath}/../queue_systems/${QUEUE_SYSTEM}.cfg"
fi
echo "eval $QUEUE_JOB_CANCEL_CMD $idJob"




