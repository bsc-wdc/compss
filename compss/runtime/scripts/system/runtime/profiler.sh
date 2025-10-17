#!/bin/bash

start_profiling() {
  if [ "${provenance}" != false ]; then
    if [ -z "${COMPSS_PROFILING_INTERVAL}" ]; then
      export COMPSS_PROFILING_INTERVAL=5
    fi
    echo "PROVENANCE | Profiling interval set to ${COMPSS_PROFILING_INTERVAL} second(s)"

    if [ -z "${logDir}" ]; then
      working_directory="$(dirname ${wdir_in_master})/stats"
      mkdir $working_directory
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
  # launch the profiling script
  python3 "${COMPSS_HOME}/Runtime/scripts/system/profiling/profiler.py" "${working_directory}" &
  PROFILING_PID=$!
}