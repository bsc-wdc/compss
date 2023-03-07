#!/bin/bash


# shellcheck disable=SC2154

#-------------------------------------
# Call this script to check the status of the given jobs
# usage:
# check.sh [DIR_WITH_JOBS_STATUS] [JOB_ID_1] [JOB_ID_2] [...] [JOB_ID_N]
# output: (program_id can be the pid if the execution is interactive and batch id if it is batch)
# [JOB_ID_1] [PROGRAM_ID] [STATUS]
# [JOB_ID_2] [PROGRAM_ID] [STATUS]
# [....]
# [JOB_ID_N] [PROGRAM_ID] [STATUS]
# -------------------------------------

checkAlljobs(){
  if [ -z "$ResponseDir" ]; then
    exit 127
  fi
  for id in "$@" ; do
      checkJob "$id"
      echo ""
  done
}

checkJob(){
  local jobId=$1
  if [ -f "${ResponseDir}/${jobId}" ]; then
     echo "${jobId} $(cat "${ResponseDir}/${jobId}")"
  else
    echo "${jobId} UNASSIGNED NOT_EXISTS"
  fi

}

#-------------------------------------
# Check Response Files from given jobs
#-------------------------------------
ResponseDir=$1
shift 1
checkAlljobs "$@"