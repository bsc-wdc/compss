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

update_args_to_pass(){
  args_pass="${orig_args_pass}"
  if [ ! -z "${cpus_per_node}" ]; then
  	args_pass="${args_pass} ${cpus_per_node}"
  fi
  if [ ! -z "${gpus_per_node}" ]; then
        args_pass="${args_pass} ${gpus_per_node}"
  fi
  if [ ! -z "${constraint}" ]; then
        args_pass="${args_pass} ${contraint}"
  fi
  # TODO: Add other parameters to pass
     
}

unset_type_vars(){
  unset cpus_per_node gpus_per_node num_nodes node_memory
  # TODO: Unset other changed parameters
}

write_master_submit(){
  add_submission_headers
  add_only_master_node
  add_launch
}

write_worker_submit(){
  add_submission_headers
  if [ "${HETEROGENEOUS_MULTIJOB}" == "true" ]; then
     add_only_worker_nodes
     add_launch
  fi
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
  
  echo "Submit command: ${SUBMISSION_CMD}${SUBMISSION_HET_PIPE}${submit_files}"
  eval ${SUBMISSION_CMD}${SUBMISSION_HET_PIPE}${submit_files}
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
 
  # Create TMP submit script
  create_tmp_submit
  echo "submit files is set in ${TMP_SUBMIT_SCRIPT}"
  submit_files="${TMP_SUBMIT_SCRIPT}"

  unset_type_vars  
  echo " Parsing workers ${worker_types}"
  workers=$(echo "${worker_types}" | tr ',' ' ')  
  for worker in ${workers}; do
    echo " submitting worker ${worker}"
    worker_desc=$(echo "${worker}" | tr ':' ' ')
    
    eval ${worker_desc[0]}
    
    num_nodes=${worker_desc[1]}
    
    check_args

    set_time

    log_args

    write_worker_submit
    
    unset_type_vars
    
    if [ "${HETEROGENEOUS_MULTIJOB}" == "true" ]; then
    	create_tmp_submit
    	echo "submit files is set in ${TMP_SUBMIT_SCRIPT}"
    	submit_files="${submit_files}${SUBMISSION_HET_SEPARATOR}${TMP_SUBMIT_SCRIPT}"
    else
	add_packjob_separator 
    fi
  done
  # Write master
  eval $master_type

  num_nodes=1
  # Check parameters
  check_args

  # Set wall clock time
  set_time

  update_args_to_pass

  # Log received arguments
  log_args
  
  write_master_submit
 
  # Trap cleanup
  #trap cleanup EXIT

  # Submit
  submit
