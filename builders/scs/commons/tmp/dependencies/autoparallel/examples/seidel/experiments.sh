#!/bin/bash -e

  #
  # HELPER FUNCTIONS
  #

  wait_and_get_jobID() {
    sleep 6s
    job_dependency=$(squeue | grep "$(whoami)" | sort -n - | tail -n 1 | awk '{ print $1 }')
    echo "Last Job ID: ${job_dependency}"
  }

  log_information() {
    local job_log_file=$1
    local job_id=$2
    local version=$3
    local n_size=$4
    local t_size=$5
    local tracing=$6

    echo "$job_id	$version	$n_size	$t_size	$tracing" >> "${job_log_file}"
  }

  #
  # MAIN
  #

  # Script global variables
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

  # Script arguments
  job_log_file=${1:-experiments.log}

  # Initialize job log file
  echo "JOB_ID	VERSION		NSIZE	TSIZE	TRACING" > "${job_log_file}"

  # Application variables
  graph=false
  log_level=off

  #        REFERENCE  MAX_PAR
  NSIZES=(     100     40)
  TSIZES=(     5       5)
  NUM_NODES=(  2       2)
  EXEC_TIMES=( 30      30)

  cpus_per_node=48

  job_dependency=None
  for i in "${!NSIZES[@]}"; do
    nsize=${NSIZES[$i]}
    tsize=${TSIZES[$i]}
    num_nodes=${NUM_NODES[$i]}
    execution_time=${EXEC_TIMES[$i]}

    for app_path in "${SCRIPT_DIR}"/*/; do
      app_version=$(basename "${app_path}")
      if [ "${app_version}" == "autoparallel" ] || [ "${app_version}" == "userparallel" ]; then
        echo "--- Enqueueing ${app_version}"
        # With tracing
        tracing=true
        ./enqueue.sh "${app_version}" "${job_dependency}" "${num_nodes}" "${execution_time}" "${cpus_per_node}" "${tracing}" "${graph}" "${log_level}" "${nsize}" "${tsize}"
        wait_and_get_jobID
        log_information "${job_log_file}" "${job_dependency}" "${app_version}" "${nsize}" "${tsize}" "${tracing}"
        # Without tracing
        tracing=false
        ./enqueue.sh "${app_version}" "${job_dependency}" "${num_nodes}" "${execution_time}" "${cpus_per_node}" "${tracing}" "${graph}" "${log_level}" "${nsize}" "${tsize}"
        wait_and_get_jobID
        log_information "${job_log_file}" "${job_dependency}" "${app_version}" "${nsize}" "${tsize}" "${tracing}"
      fi
    done
  done
