#!/bin/bash -e

  # Script global variables
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
  RESULTS_DIR="${SCRIPT_DIR}"/../results/local/userparallel
  APP_NAME=lu_userparallel
  LOG_DIR=$HOME/.COMPSs/${APP_NAME}

  rm -rf "${LOG_DIR}"
  rm -rf "${RESULTS_DIR}"
  mkdir -p "${LOG_DIR}"
  mkdir -p "${RESULTS_DIR}"

  # Script parameters
  if [ $# -eq 0 ]; then
    user_flags=""
  else
    user_flags=$*
  fi

  # COMPSs parameters
  DEBUG_FLAGS="--summary"
  TOOLS_FLAGS="-g -t"

  # Application arguments
  MSIZE=4
  BSIZE=8

  export ComputingUnits=1

  # Run application
  # shellcheck disable=SC2086
  runcompss \
          ${DEBUG_FLAGS} \
          ${TOOLS_FLAGS} \
          ${user_flags} \
          --specific_log_dir="${LOG_DIR}" \
          --lang=python \
          --project=../../xml/project.xml \
          --resources=../../xml/resources.xml \
          lu.py $MSIZE $BSIZE

  # Copy results
  if [ -f "${LOG_DIR}/monitor/complete_graph.dot" ]; then
    cp "${LOG_DIR}"/monitor/complete_graph.dot "${RESULTS_DIR}"
    gengraph "${RESULTS_DIR}"/complete_graph.dot
    dot -Tpng -Gnewrank=true "${RESULTS_DIR}"/complete_graph.dot > "${RESULTS_DIR}"/complete_graph.png
  fi
  if [ -d "${LOG_DIR}/trace/" ]; then
    mkdir -p "${RESULTS_DIR}"/trace
    cp "${LOG_DIR}"/trace/*.prv "${RESULTS_DIR}"/trace/${APP_NAME}.prv
    cp "${LOG_DIR}"/trace/*.pcf "${RESULTS_DIR}"/trace/${APP_NAME}.pcf
    cp "${LOG_DIR}"/trace/*.row "${RESULTS_DIR}"/trace/${APP_NAME}.row
  fi
