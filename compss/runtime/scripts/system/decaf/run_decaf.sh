#!/bin/bash


#
# HELPER METHODS
#

get_args() {
  df_script=$1
  df_executor=$2
  #df_lib=$3
  #mpirunner=$4
  num_nodes=$6
  hostfile=$8
  args=${@: 10}
}

main() {
  # Retrieve arguments
  echo "[DECAF] Retrieve Decaf arguments"
  get_args "$@"

  # Execute generator
  echo "[DECAF] Executing Decaf data-flow generator"
  echo "[DECAF] CMD: python3 ${df_script} -n ${num_nodes} --hostfile ${hostfile} --args \"${args}\""

  python3 "${df_script}" -n "${num_nodes}" --hostfile "${hostfile}" --args "${args}"
  ev=$?
  if [ $ev -ne 0 ]; then
    echo "Error running data-flow generator"
    exit $ev
  fi
  
  # Execute data-flow
  echo "[DECAF] Executing Decaf data-flow"
  echo "[DECAF] CMD: ${df_executor}"

  ${df_executor}
  ev=$?
  if [ $ev -ne 0 ]; then
    echo "Error running data-flow"
    exit $ev
  fi
}


#
# ENTRY POINT
#

main "$@"
