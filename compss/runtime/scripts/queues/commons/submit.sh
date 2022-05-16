#!/bin/bash

###############################################
# Function to clean TMP files
###############################################
cleanup() {
  rm -rf "${TMP_SUBMIT_SCRIPT}".*
}

###############################################
# Function to submit the script
###############################################
ERROR_SUBMIT="Error submiting script to queue system"

submit() {
  # Submit the job to the queue

  # shellcheck disable=SC2086
  eval ${SUBMISSION_CMD} ${SUBMISSION_PIPE}${TMP_SUBMIT_SCRIPT}
  result=$?

  # Check if submission failed
  if [ $result -ne 0 ]; then
    submit_err=$(cat "${TMP_SUBMIT_SCRIPT}.err")
    echo "${ERROR_SUBMIT}${submit_err}"
    exit 1
  fi
}


#---------------------------------------------------
# MAIN EXECUTION
#---------------------------------------------------
  # shellcheck source=common.sh
  # shellcheck disable=SC1091
  source "${COMPSS_HOME}Runtime/scripts/queues/commons/common.sh"

  # Get command args (loads args from commons.sh, specially sc_cfg)
  get_args "$@"

  if [ -f "${sc_cfg}" ]; then
     source "${sc_cfg}"
  else
     # Load specific queue system variables
     # shellcheck source=../supercomputers/default.cfg
     # shellcheck disable=SC1091
     # shellcheck disable=SC2154
     source "${COMPSS_HOME}Runtime/scripts/queues/supercomputers/${sc_cfg}"
  fi

  # Check parameters
  check_args

  # Load specific queue system flags
  # shellcheck source=../queue_systems/slurm.cfg
  # shellcheck disable=SC1091
  source "${COMPSS_HOME}Runtime/scripts/queues/queue_systems/${QUEUE_SYSTEM}.cfg"

  # Set wall clock time
  set_time

  # Log received arguments
  log_args

  # Create TMP submit script
  create_normal_tmp_submit

  # Trap cleanup
  trap cleanup EXIT

  # Submit
  submit
