#!/bin/bash -e

  # Script arguments
  if [ $# -ne 0 ] && [ $# -ne 10 ]; then
    echo "ERROR: Incorrect number of parameters"
    exit 1
  fi
  app_version=${1:-autoparallel}
  job_dependency=${2:-None}
  num_nodes=${3:-2}
  execution_time=${4:-15}
  cpus_per_node=${5:-48}
  tracing=${6:-true}
  graph=${7:-false}
  log_level=${8:-off}

  nsize=${9:-1000}
  msize=${10:-1000}

  # Script variables                                                                                                                                                                                                                                                           
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  EXEC_FILE=${SCRIPT_DIR}/${app_version}/ep.py
  WORK_DIR=${SCRIPT_DIR}/results/mn/${app_version}
  LOCAL_PYTHONPATH=${SCRIPT_DIR}/${app_version}


  # Setup Execution environment
  if [ ! -d "${WORK_DIR}" ]; then
    mkdir -p "${WORK_DIR}"
  fi
  tile_file=${SCRIPT_DIR}/${app_version}/tile.sizes
  if [ -f "${tile_file}" ]; then
    cp "${tile_file}" "${WORK_DIR}"
  fi

  export ComputingUnits=1

  # Enqueue job
  enqueue_compss \
    --job_dependency="${job_dependency}" \
    --exec_time="${execution_time}" \
    --num_nodes="${num_nodes}" \
    \
    --cpus_per_node="${cpus_per_node}" \
    --worker_in_master_cpus=0 \
    --node_memory=50000 \
    --scheduler=es.bsc.compss.scheduler.fifoDataScheduler.FIFODataScheduler \
    \
    --tracing="${tracing}" \
    --graph="${graph}" \
    --summary \
    --log_level="${log_level}" \
    \
    --master_working_dir="${WORK_DIR}" \
    --worker_working_dir=/gpfs/projects/bsc19/COMPSs_SandBox \
    --base_log_dir="${WORK_DIR}" \
    --pythonpath="${LOCAL_PYTHONPATH}" \
    --lang=python \
    \
    "$EXEC_FILE" "$nsize" "$msize"

  # Params: job_dependency version num_nodes execution_time cpus_per_node tracing graph log_level NSIZE MSIZE
  # Example: ./enqueue.sh autoparallel None 2 15 48 true false off 1000 1000

