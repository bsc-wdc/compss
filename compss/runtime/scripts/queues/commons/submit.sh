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
  #eval ${SUBMISSION_CMD} ${SUBMISSION_PIPE}${TMP_SUBMIT_SCRIPT} 1>${TMP_SUBMIT_SCRIPT}.out 2>${TMP_SUBMIT_SCRIPT}.err
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
  if [ -z "${COMPSS_HOME}" ]; then
     SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
     COMPSS_HOME=${SCRIPT_DIR}/../../../../  
  else 
     SCRIPT_DIR="${COMPSS_HOME}/Runtime/scripts/queues/commons"
  fi
  # shellcheck source=common.sh
  source "${SCRIPT_DIR}"/common.sh

  # Get command args
  get_args "$@"

  # Load specific queue system variables
  # shellcheck source=../cfgs/default.cfg
  source "${SCRIPT_DIR}/../cfgs/${sc_cfg}"

  # Check parameters
  check_args

  # Load specific queue system flags
  # shellcheck source=../slurm/slurm.cfg
  source "${SCRIPT_DIR}/../${QUEUE_SYSTEM}/${QUEUE_SYSTEM}.cfg"

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
