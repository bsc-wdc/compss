#!/bin/bash


#
# HELPER METHODS
#

get_args() {
  julia_executor=$1
  julia_script=$2
  working_dir=$3
  fail_by_ev=$4
  num_nodes=$5
  is_debug_enabled=$6
  args=${7}
}

main() {
  # Retrieve arguments
  if [ ${is_debug_enabled} == "true" ]; then
    echo "[JULIA] Retrieve JULIA arguments"
    echo "[JULIA] Arguments: $@"
  fi
  get_args "$@"

  # Execute Julia scripts
  if [ ${is_debug_enabled} == "true" ]; then
    echo "[JULIA] Executing Julia script"
    echo "[JULIA] CMD: ${julia_executor} ${julia_script} ${args}"

    if [ ${num_nodes} -eq 1 ]; then
      echo "[JULIA] Executing in single node"
    elif [ ${num_nodes} -gt 1 ]; then
      echo "[JULIA] Executing in multiple nodes: ${num_nodes}"
    else
      echo "[JULIA] Error: unexpected value for multiple nodes: ${num_nodes}" 
    fi
  fi

  "${julia_executor}" "${julia_script}" ${args}
  ev=$?  # execution exit code

  # But before exiting, check if there are processes that need to be cat to job out.
  if [ ${is_debug_enabled} == "true" ]; then
    for out_f in ./julia-*.out; do
      echo "[JULIA] Processing output file: ${out_f}"
      cat ${out_f}
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
