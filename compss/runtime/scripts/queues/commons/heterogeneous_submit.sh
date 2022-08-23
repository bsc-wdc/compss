#!/bin/bash

#---------------------------------------------------
# ERROR CONSTANTS DECLARATION
#---------------------------------------------------
ERROR_NO_TYPES_CFG_FILE="Types configuration file not defined"
ERROR_NO_MASTER_TYPE="Master type not defined"
ERROR_NO_WORKER_TYPES="Worker types not defined"

check_heterogeneous_args(){
  # WARN: variable are loaded from common.sh get_args
  # shellcheck disable=SC2154
  if [ -z "${types_cfg_file}" ]; then
     display_error "${ERROR_NO_TYPES_CFG_FILE}" 1
  fi
  # shellcheck disable=SC2154
  if [ -z "${master_type}" ]; then
     display_error "${ERROR_NO_MASTER_TYPE}" 1
  fi
  # shellcheck disable=SC2154
  if [ -z "${worker_types}" ]; then
     display_error "${ERROR_NO_WORKER_TYPES}" 1
  fi
}

update_args_to_pass(){
  local xml_phase=$1
  local xml_suffix=$2
  local initial_hostid=$3
  local app_uuid=$4
  args_pass="${original_args_pass}"

  # WARN: Checked variables are loaded from common.sh get_args
  # shellcheck disable=SC2154
  if [ ! -z "${cpus_per_node}" ]; then
  	args_pass="--cpus_per_node=${cpus_per_node} ${args_pass}"
  fi
  # shellcheck disable=SC2154
  if [ ! -z "${gpus_per_node}" ]; then
        args_pass="--gpus_per_node=${gpus_per_node} ${args_pass}"
  fi
  # shellcheck disable=SC2154
  if [ ! -z "${constraints}" ]; then
        args_pass="--constraints=${constraints} ${args_pass}"
  fi
  # shellcheck disable=SC2154
  if [ ! -z "${licenses}" ]; then
        args_pass="--licenses=${licenses} ${args_pass}"
  fi
  # shellcheck disable=SC2154
  if [ ! -z "${node_memory}" ]; then
        args_pass="--node_memory=${node_memory} ${args_pass}"
  fi
  if [ ! -z "${xml_phase}" ]; then
        args_pass="--xmls_phase=${xml_phase} ${args_pass}"
  fi
  if [ ! -z "${xml_suffix}" ]; then
        args_pass="--xmls_suffix=${xml_suffix} ${args_pass}"
  fi
  if [ ! -z "${initial_hostid}" ]; then
        args_pass="--initial_hostid=${initial_hostid} ${args_pass}"
  fi
  if [ ! -z "${app_uuid}" ]; then
       args_pass="--uuid=${app_uuid} ${args_pass}"
  fi
  # TODO: Add other parameters to pass
}

unset_type_vars(){
  unset cpus_per_node gpus_per_node constraints licenses num_nodes node_memory
  # TODO: Unset other changed parameters
}

write_master_submit(){
  add_submission_headers
  add_env_script
  add_only_master_node
  add_launch
}

write_worker_submit(){
  add_submission_headers
  add_env_script
  add_only_worker_nodes
  add_launch
}

###############################################
# Function to clean TMP files
###############################################
cleanup() {
  # shellcheck disable=SC2086
  rm -rf $submit_files
}

###############################################
# Function to submit the script
###############################################
submit() {
  # Submit the job to the queue
  #eval ${SUBMISSION_CMD} ${SUBMISSION_PIPE}${TMP_SUBMIT_SCRIPT} 1>${TMP_SUBMIT_SCRIPT}.out 2>${TMP_SUBMIT_SCRIPT}.err

  echo "Submit command: ${SUBMISSION_CMD}${SUBMISSION_HET_PIPE}${submit_files}"
  # shellcheck disable=SC2086
  eval ${SUBMISSION_CMD}${SUBMISSION_HET_PIPE}${submit_files}
  result=$?

  # Check if submission failed
  if [ $result -ne 0 ]; then
    submit_err=$(cat "${TMP_SUBMIT_SCRIPT}".err)
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

  # Get command args (from common.sh, includes sc_cfg)
  get_args "$@"

  # Storing original arguments to pass
  original_args_pass="${args_pass}"
  if [ -f "${sc_cfg}" ]; then
     source "${sc_cfg}"
  else
     # Load specific queue system variables
     # shellcheck source=../cfgs/default.cfg
     # shellcheck disable=SC1091
     # shellcheck disable=SC2154
     source "${COMPSS_HOME}Runtime/scripts/queues/commons/../supercomputers/${sc_cfg}"
  fi

  # Load specific queue system flags
  # shellcheck source=../slurm/slurm.cfg
  # shellcheck disable=SC1091
  source "${COMPSS_HOME}Runtime/scripts/queues/commons/../queue_systems/${QUEUE_SYSTEM}.cfg"

  check_heterogeneous_args
  # shellcheck source=./user/defined/file
  # shellcheck disable=SC1091
  source "${types_cfg_file}"
  # create application uuid
  uuid=$(cat /proc/sys/kernel/random/uuid)

  # Create TMP submit script
  create_tmp_submit
  echo "submit files is set in ${TMP_SUBMIT_SCRIPT}"
  submit_files="${TMP_SUBMIT_SCRIPT}"

  suffix=$(date +%s)
  if [ -z "${HETEROGENEOUS_MULTIJOB}" ] || [ "${HETEROGENEOUS_MULTIJOB}" = "false" ]; then
        echo "adding master node request headers ${TMP_SUBMIT_SCRIPT}"
        eval $master_type
        num_nodes=1
        check_args
        set_time
        add_submission_headers
        add_packjob_separator
        unset_type_vars
  fi
  echo " Parsing workers ${worker_types}"
  worker_num=1
  hostid=1
  workers=$(echo "${worker_types}" | tr ',' ' ')
  for worker in ${workers}; do
    # Create tmp file or add packjob
    if [ ${worker_num} -gt 1 ]; then
       if [ "${HETEROGENEOUS_MULTIJOB}" == "true" ]; then
          create_tmp_submit
          echo "submit files is set in ${TMP_SUBMIT_SCRIPT}"
          submit_files="${submit_files}${SUBMISSION_HET_SEPARATOR}${TMP_SUBMIT_SCRIPT}"
       else
          add_packjob_separator
       fi
    fi
    # Eval worker description
    worker_desc=$(echo "${worker}" | tr ':' ' ')
    eval ${worker_desc[0]}
    num_nodes=${worker_desc[1]}
    check_args
    set_time
    if [ "${HETEROGENEOUS_MULTIJOB}" == "true" ]; then
        if [ ${worker_num} -eq 1 ]; then
           update_args_to_pass "init" "${suffix}" "${hostid}" "${uuid}"
        else
           update_args_to_pass "add" "${suffix}" "${hostid}" "${uuid}"
        fi
        log_args
        write_worker_submit ${worker_num}
    else
        add_submission_headers
    fi
    worker_num=$((worker_num + 1))
    hostid=$((hostid + num_nodes))
    unset_type_vars
  done

  # Write worker launch
  if [ -z "${HETEROGENEOUS_MULTIJOB}" ] || [ "${HETEROGENEOUS_MULTIJOB}" = "false" ]; then
     worker_num=1
     hostid=1
     workers=$(echo "${worker_types}" | tr ',' ' ')
     for worker in ${workers}; do
        worker_desc=$(echo "${worker}" | tr ':' ' ')
        eval ${worker_desc[0]}
        num_nodes=${worker_desc[1]}
        check_args
        set_time
        if [ ${worker_num} -eq 1 ]; then
           update_args_to_pass "init" "${suffix}" "${hostid}" "${uuid}"
        else
           update_args_to_pass "add" "${suffix}" "${hostid}" "${uuid}"
        fi
        log_args
        add_only_worker_nodes "_PACK_GROUP_${worker_num}"
        add_launch
        worker_num=$((worker_num + 1))
        hostid=$((hostid + num_nodes))
        unset_type_vars
     done
  fi

  # Write master
  eval ${master_type}

  num_nodes=1
  # Check parameters
  check_args
  set_time
  update_args_to_pass "fini" "${suffix}" "${hostid}" "${uuid}"
  log_args
  if [ "${HETEROGENEOUS_MULTIJOB}" == "true" ]; then
     create_tmp_submit
     echo "submit files is set in ${TMP_SUBMIT_SCRIPT}"
     submit_files="${submit_files}${SUBMISSION_HET_SEPARATOR}${TMP_SUBMIT_SCRIPT}"
     write_master_submit
  else
     add_only_master_node
     add_launch
  fi
  # Trap cleanup
  #trap cleanup EXIT

  # Submit
  submit
