#!/bin/bash


#
# HELPER METHODS
#

get_args() {
  julia_executor=$1
  julia_args=$2
  julia_script=$3
  working_dir=$4
  fail_by_ev=$5
  num_nodes=$6
  is_debug_enabled=$7
  args=${8}
}

main() {
  # Retrieve arguments
  if [[ ${is_debug_enabled} == "true" ]]; then
    echo "[JULIA] Retrieve JULIA arguments"
    echo "[JULIA] Arguments: $@"
  fi
  get_args "$@"

  # Execute Julia scripts
  if [[ ${is_debug_enabled} == "true" ]]; then
    echo "[JULIA] Executing Julia script"
    echo "[JULIA] CMD: ${julia_executor} ${julia_args} ${julia_script} ${args}"

    if [ ${num_nodes} -eq 1 ]; then
      echo "[JULIA] Executing in single node"
    elif [ ${num_nodes} -gt 1 ]; then
      echo "[JULIA] Executing in multiple nodes: ${num_nodes}"
    else
      echo "[JULIA] Error: unexpected value for multiple nodes: ${num_nodes}" 
    fi
  fi

  "${julia_executor}" ${julia_args} "${julia_script}" ${args}
  ev=$?  # execution exit code

  # But before exiting, check if there are processes that need to be cat to job out.
  if [[ ${is_debug_enabled} == "true" ]]; then
    for out_f in ./julia-*.out; do
      echo "[JULIA] Processing output file: ${out_f}"
      if [[ -f "${out_f}" ]]; then
          cat ${out_f}
      else
          echo "[JULIA] File: ${out_f} does not exist"
      fi
    done
  fi

  if [ $ev -ne 0 ]; then
    echo "Error running Julia script"
    exit $ev
  fi
}


#
# ENTRY POINT
#

main "$@"
