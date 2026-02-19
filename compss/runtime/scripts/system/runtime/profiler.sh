#!/bin/bash

start_profiling() {
  if [ "${provenance}" != false ]; then
    if [ -z "${COMPSS_PROFILING_INTERVAL}" ]; then
      export COMPSS_PROFILING_INTERVAL=5
    fi

    if [ -z "${logDir}" ]; then
      echo "PROVENANCE | PROFILING | Profiling interval set to ${COMPSS_PROFILING_INTERVAL} second(s)"
      working_directory="$(dirname ${wdir_in_master})/stats"
      mkdir -p "$working_directory"
      if [ -z "${worker_in_master_cpus}" ] || [ "${worker_in_master_cpus}" -eq 0 ]; then
        launch_profiling_script
      fi
    else
      working_directory="${logDir}"
      launch_profiling_script
    fi
  fi
}


launch_profiling_script() {
  # Launch the profiling script
  if [ ! -z "${COMPSS_PROV_DEBUG}" ]; then
    python3 "${COMPSS_HOME}/Runtime/scripts/system/profiling/profiler.py" "${COMPSS_HOME}/Runtime/scripts/system/profiling/profiler_config.json" "${working_directory}" &
  else
    python3 -O "${COMPSS_HOME}/Runtime/scripts/system/profiling/profiler.py" "${COMPSS_HOME}/Runtime/scripts/system/profiling/profiler_config.json" "${working_directory}" &
  fi
  PROFILING_PID=$!
}