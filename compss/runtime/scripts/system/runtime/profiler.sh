#!/bin/bash

#########################################
# PROFILING FUNCTIONS LOCAL ENVIRONMENT #
#########################################
start_profiling_local() {
  if [ -z "${logDir}" ]; then
    # Master node
    master_working_dir="$(dirname "${wdir_in_master}")/stats"
    is_master="true"
    mkdir -p "${master_working_dir}"
    if [ -z "${worker_in_master_cpus}" ] || [ "${worker_in_master_cpus}" -eq 0 ]; then
      launch_profiling_script "${master_working_dir}" "${is_master}"
    fi
  else
    # Worker node
    is_master="false"
    launch_profiling_script "${logDir}" "${is_master}"
  fi
}

launch_profiling_script() {
  local working_dir="$1"
  local is_master="$2"

  # Launch the profiling script
  if [ ! -z "${COMPSS_PROV_DEBUG}" ]; then
    python3 "${COMPSS_HOME}/Runtime/scripts/system/profiling/profiler.py" "${working_dir}" "${is_master}" &
  else
    python3 -O "${COMPSS_HOME}/Runtime/scripts/system/profiling/profiler.py" "${working_dir}" "${is_master}" &
  fi
  PROFILING_PID=$!
}

stop_profiling_local() {
  # check if profiling is stopped before
  if [ "${PROFILING_STOPPED}" = true ]; then
    return
  fi

  # check if the profiling process is still running before trying to stop it
  if [ -n "${PROFILING_PID}" ] && kill -0 "${PROFILING_PID}" 2>/dev/null; then
    # gracefully stop the profiling process
    kill -SIGUSR1 "${PROFILING_PID}" 2>/dev/null

    # wait for the profiling process to stop, with a timeout
    local timeout=5 # seconds
    while [ ${timeout} -gt 0 ] && kill -0 "${PROFILING_PID}" 2>/dev/null; do
      sleep 1
      timeout=$((timeout - 1))
    done

    # if the profiling process is still running after the timeout, force kill it
    if kill -0 "${PROFILING_PID}" 2>/dev/null; then
      echo "PROVENANCE | PROFILING | Warning: profiler did not stop after SIGUSR1. Sending SIGKILL."
      kill -9 "${PROFILING_PID}" 2>/dev/null
    fi
  fi

  PROFILING_STOPPED=true
  unset PROFILING_PID
}

#########################################
# PROFILING FUNCTIONS SLURM ENVIRONMENT #
#########################################
start_profiling_slurm() {
  local slurm_node=""

  for possible_node in ${COMPSS_MASTER_NODE} ${COMPSS_WORKER_NODES}; do
      if [ "${hostName#${possible_node}}" != "${hostName}" ]; then
          slurm_node="${possible_node}"
          break
      fi
  done
  [ -z "${slurm_node}" ] && slurm_node="${hostName}"  # fallback: no suffix

  if [ -z "${logDir}" ]; then
    # Master node
    is_master="true"
    master_working_dir="$(dirname ${wdir_in_master})/stats"
    mkdir -p "${master_working_dir}"

    if [ -z "${worker_in_master_cpus}" ] || [ "${worker_in_master_cpus}" -eq 0 ]; then
      launch_profiling_srun "${COMPSS_MASTER_NODE}" "${master_working_dir}" "${is_master}"
      PROFILING_PID=$!
      echo "PROVENANCE | PROFILING | Profiler started."
    fi
  else
    # Worker node
    is_master="false"
    launch_profiling_srun "${slurm_node}" "${logDir}" "${is_master}"
    PROFILING_PID=$!
  fi
}

launch_profiling_srun() {
  local node="$1"
  local working_directory="$2"
  local is_master="$3"
  # local output_csv="$3"

  srun --overlap \
       --nodelist="${node}" \
       --nodes=1 \
       --ntasks=1 \
       --cpus-per-task=1 \
       --exact \
       --cpu-bind=none \
       --chdir="${working_directory}" \
       --export=ALL \
       bash -c "python3 -O ${COMPSS_HOME}/Runtime/scripts/system/profiling/profiler.py \
            ${working_directory} ${is_master}" &
}

stop_profiling_slurm() {
  # check if profiling is stopped before
  if [ "${PROFILING_STOPPED}" = true ]; then
    return
  fi

  # check if the profiling process is still running before trying to stop it
  if [ -n "${PROFILING_PID}" ] && kill -0 "${PROFILING_PID}" 2>/dev/null; then
    kill -SIGUSR1 "${PROFILING_PID}" 2>/dev/null
    wait "${PROFILING_PID}" 2>/dev/null
  fi

  PROFILING_STOPPED=true
  unset PROFILING_PID
}


#########################
# ENTRY POINT FUNCTIONS #
#########################
start_profiling() {
  if [ "${provenance}" != false ]; then
    if [ -z "${COMPSS_PROFILING_INTERVAL}" ]; then
      export COMPSS_PROFILING_INTERVAL=5
    fi
    # write the message in every case
    echo "PROVENANCE | PROFILING | Profiling interval set to ${COMPSS_PROFILING_INTERVAL} second(s)"

    if [[ -z "${ENQUEUE_COMPSS_ARGS}" ]]; then
      # LOCAL LAUNCH
      start_profiling_local
    else
      # SLURM LAUNCH
      start_profiling_slurm
    fi
  fi
}

stop_profiling() {
  if [[ -z "${ENQUEUE_COMPSS_ARGS}" ]]; then
    # STOP LOCAL PROFILER
    stop_profiling_local
  else
    # STOP SLURM PROFILER
    stop_profiling_slurm
  fi
}

