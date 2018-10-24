#!/bin/bash

###############################################
# Function to clean TMP files
###############################################
cleanup() {
  rm -rf ${TMP_SUBMIT_SCRIPT}.*
}

###############################################
# Function to submit the script
###############################################
submit() {
  # Submit the job to the queue
  #eval ${SUBMISSION_CMD} ${SUBMISSION_PIPE}${TMP_SUBMIT_SCRIPT} 1>${TMP_SUBMIT_SCRIPT}.out 2>${TMP_SUBMIT_SCRIPT}.err
  eval ${SUBMISSION_CMD} ${SUBMISSION_PIPE}${TMP_SUBMIT_SCRIPT}
  result=$?

  # Check if submission failed
  if [ $result -ne 0 ]; then
    submit_err=$(cat ${TMP_SUBMIT_SCRIPT}.err)
    echo "${ERROR_SUBMIT}${submit_err}"
    exit 1
  fi
}


#---------------------------------------------------
# MAIN EXECUTION
#---------------------------------------------------
  if [ -z "${COMPSS_HOME}" ]; then
     scriptDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
     COMPSS_HOME=${SCRIPT_DIR}/../../../../  
  else 
     scriptDir="${COMPSS_HOME}/Runtime/scripts/queues/commons"
  fi
  source  ${scriptDir}/common.sh

  # Get command args
  get_args "$@"

  # Load specific queue system variables
  source ${scriptDir}/../cfgs/${sc_cfg}

  # Check parameters
  check_args

  # Load specific queue system flags
  source ${scriptDir}/../${QUEUE_SYSTEM}/${QUEUE_SYSTEM}.cfg

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
