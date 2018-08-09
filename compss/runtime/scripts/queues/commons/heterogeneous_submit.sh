#!/bin/bash

#---------------------------------------------------
# ERROR CONSTANTS DECLARATION
#---------------------------------------------------
ERROR_NO_TYPES_CFG_FILE="Types configuration file not defined"
ERROR_NO_MASTER_TYPE="Master type not defined"
ERROR_NO_WORKER_TYPES="Worker types not defined"

check_heterogeneous_args(){
  if [ -z "${types_cfg_file}" ]; then
     display_error "${ERROR_NO_TYPES_CFG_FILE}" 1
  fi
  if [ -z "${master_type}" ]; then
     display_error "${ERROR_NO_MASTER_TYPE}" 1
  fi
  if [ -z "${worker_types}" ]; then
     display_error "${ERROR_NO_WORKER_TYPES}" 1
  fi
}

update_args_tp_pass(){
  args_pass="${orig_args_pass}"
  if [ ! -z "${cpus_per_node}" ]; then
  	args_pass="${args_pass} ${cpus_per_node}"
  fi
  if [ ! -z "${gpus_per_node}" ]; then
        args_pass="${args_pass} ${gpus_per_node}"
  fi
  # TODO: Add other parameters to pass
     
}

unset_type_vars(){
  unset cpus_per_node gpus_per_node num_nodes node_memory
  # TODO: Unset other changed parameters
}

create_master_submit(){
  create_tmp_submit
  add_submission_headers
  add_only_master_node
  add_launch
}

create_worker_submit(){
  create_tmp_submit
  add_submission_headers
  add_only_worker_nodes
  add_launch
}
###############################################
# Function to clean TMP files
###############################################
cleanup() {
  rm -rf $submit_files
}

###############################################
# Function to submit the script
###############################################
submit() {
  # Submit the job to the queue
  #eval ${SUBMISSION_CMD} ${SUBMISSION_PIPE}${TMP_SUBMIT_SCRIPT} 1>${TMP_SUBMIT_SCRIPT}.out 2>${TMP_SUBMIT_SCRIPT}.err
  SUBMIT=$(echo $submit_files | tr " " "${SUBMISSION_HET_SEPARATOR}")
  eval ${SUBMISSION_CMD}${SUBMISSION_HET_PIPE}${SUBMIT}
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
  
  original_args_pass="${args_pass}"
  # Load specific queue system variables
  source ${scriptDir}/../cfgs/${sc_cfg}

  # Load specific queue system flags
  source ${scriptDir}/../${QUEUE_SYSTEM}/${QUEUE_SYSTEM}.cfg
  
  check_heterogeneous_args
  
  source ${types_cfg_file}
 
  eval $master_type
  
  num_nodes=1
  # Check parameters
  check_args

  # Set wall clock time
  set_time
  
  update_args_to_pass

  # Log received arguments
  log_args

  # Create TMP submit script
  create_master_submit
  
  submit_files="${TMP_SUBMIT_FILE}"

  uset_type_vars  

  workers=$(echo "${workers_types}" | tr "," " ")  
  for worker in ${workers}; do
    worker_desc=$(echo "${worker}" | tr ":" " ")
    
    eval ${worker_desc[0]}
    
    num_nodes=${worker_desc[1]}
    
    check_args

    log_args

    create_worker_submit
    
    submit_files="$submit_files ${TMP_SUBMIT_FILE}" 
  done
  
  # Trap cleanup
  trap cleanup EXIT

  # Submit
  submit
