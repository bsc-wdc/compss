#!/bin/bash

start_profiling() {
  local debug_mode="$1"

  if [ "${provenance}" != false ]; then
    if [ -z "${COMPSS_PROFILING_INTERVAL}" ]; then
      export COMPSS_PROFILING_INTERVAL=5
    fi

    if [ -z "${logDir}" ]; then
      echo "PROVENANCE | Profiling interval set to ${COMPSS_PROFILING_INTERVAL} second(s)"
      working_directory="$(dirname ${wdir_in_master})/stats"
      mkdir -p "$working_directory"
      if [ -z "${worker_in_master_cpus}" ] || [ "${worker_in_master_cpus}" -eq 0 ]; then
        launch_profiling_script "$debug_mode"
      fi
    else
      working_directory="${logDir}"
      launch_profiling_script "$debug_mode"
    fi
  fi
}


launch_profiling_script() {
  local debug_mode="$1"

  # Launch the profiling script
  if [ "$debug_mode" == "true" ]; then
    python3 "${COMPSS_HOME}/Runtime/scripts/system/profiling/profiler.py" "${working_directory}" &
  else
    python3 -O "${COMPSS_HOME}/Runtime/scripts/system/profiling/profiler.py" "${working_directory}" &
  fi
  PROFILING_PID=$!
}