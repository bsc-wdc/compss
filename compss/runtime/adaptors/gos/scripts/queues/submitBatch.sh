#!/bin/bash


on_error(){
    if [ -z "$batchID" ]; then
        batchID="NOY_YET_GIVEN"
    fi
    echo "[SUBMITBATCH.SH] ${ERROR_SUBMIT}. Aborting SUBMIT."
    mark_as_fail "$responseDir" "$responsePath" "$batchID"
    exit 1
}

trap 'on_error' ERR


###############################################
# Function to clean TMP files
###############################################
cleanup() {
  if [ ! -z "${TMP_SUBMIT_SCRIPT}" ]; then
      #echo "DO NO CLEANUP"
       rm -rf "${TMP_SUBMIT_SCRIPT}".*
       rm -rf "${TMP_SUBMIT_SCRIPT}"
  fi
}



###############################################
# Function to submit the script
###############################################
ERROR_SUBMIT="Error submitting script to queue system"
ERROR_IN_SCRIPT="Error during the script to worker"

submit() {
  # Submit the job to the queue

  echo "eval ${SUBMISSION_CMD} ${SUBMISSION_PIPE}${TMP_SUBMIT_SCRIPT} 2>${TMP_SUBMIT_SCRIPT}.err"
  echo "----------------------------BATCH SCRIPT----------------------------"
  cat "$TMP_SUBMIT_SCRIPT"
  echo "---------------------------------------------------------------------"

  # shellcheck disable=SC2086
  #eval ${SUBMISSION_CMD} ${SUBMISSION_PIPE}${TMP_SUBMIT_SCRIPT}
  batchOutput=$(eval ${SUBMISSION_CMD} ${SUBMISSION_PIPE}${TMP_SUBMIT_SCRIPT} 2>${TMP_SUBMIT_SCRIPT}.err)
  result=$?

  batchID=$(eval "echo $batchOutput $QUEUE_EXTRACTOR_ID")
  echo "$batchOutput"
  echo "$batchID"
  if [ -z "$batchID" ]; then
      #echo "DO NO CLEANUP"
      batchID="NOT_ACCESSIBLE"
  fi
  mark_as_submit "$responseDir" "$responsePath" "$batchID"


  # Check if submission failed
  if [ $result -ne 0 ]; then
    submit_err=$(cat "${TMP_SUBMIT_SCRIPT}.err")
    echo "${ERROR_SUBMIT}${submit_err}"
    mark_as_fail "$responseDir" "$responsePath" "$batchID"
    exit 1
  fi
  local killCommand=$QUEUE_JOB_CANCEL_CMD
  #substitute %JOBID% with the batchId
  killCommand="${killCommand/"%JOBID%"/"$batchID"}""; echo $batchID CANCEL > $responsePath" 
  create_kill_script_batch "$killScriptDir" "$killScriptPath" "$killCommand"
}

create_normal_task_submit(){

  create_tmp_submit
  add_submission_headers

  add_trap_to_script
  #export all nodes to host_list var
  #export master node to master_node var
  #export worker nodes to worker_nodes var
  add_master_and_worker_nodes_custom

  add_env_source
  add_task_launch_parameters #args for worker.sh
}



add_list_nodes(){
    # Host list parsing
  cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
    host_list=\$(${HOSTLIST_CMD} \$${ENV_VAR_NODE_LIST} ${HOSTLIST_TREATMENT})
    export host_list
EOT
}

add_master_and_worker_nodes_custom(){
  add_cd_master_wd
  # Host list parsing
  cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
  if [ "${HOSTLIST_CMD}" == "nodes.sh" ]; then
    source "${COMPSS_HOME}/Runtime/scripts/system/${HOSTLIST_CMD}"
  fi
  host_list=\$(${HOSTLIST_CMD} \$${ENV_VAR_NODE_LIST} ${HOSTLIST_TREATMENT})
  master_node=\$(${MASTER_NAME_CMD})
  worker_nodes=\$(echo \${host_list} | sed -e "s/\${master_node}//g")
  batchID_var=\$${ENV_VAR_JOB_ID}
  export master_node
  export host_list
  export worker_nodes
  export batchID_var

EOT
}

add_trap_to_script(){
       cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
  source "${GOS_SCRIPTS_DIR}response.sh"
  on_error_pre(){
        if [ "${REDIRECT_ERROR_TRAP}" = "true" ]; then
                exit 1
        fi
        if [ -z "\$batchID_var" ]; then
                batchID_var="NOY_YET_GIVEN"
        fi

        echo "[SUBMITBATCH.SH] ${ERROR_IN_SCRIPT}. Aborting SUBMIT."
        mark_as_fail "$responseDir" "$responsePath" "\$batchID_var"
        exit 1
  }

  export REDIRECT_ERROR_TRAP= "false";
  trap 'on_error_pre' ERR

EOT
}

add_task_launch_parameters(){
     cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
$submitWorker
EOT
}

get_task_launch_parameters(){
  echo "All_args"
  echo $all_args{*}
  submitWorker=""
  # shellcheck disable=SC2154
  for str in $args_pass; do
        if [ "${str::2}" != "--" ]; then
            submitWorker="${submitWorker} ${str}"
        fi
  done
  get_response_file "$submitWorker"
}

get_response_file(){
        stringArray=($@)
        taskID=${stringArray[1]}
        isBatch=${stringArray[2]}
        responseDir=${stringArray[3]}
        killScriptDir=${stringArray[4]}
        workingDir=${stringArray[10]}

        responsePath="${responseDir}/${taskID}"
        killScriptPath="${killScriptDir}/${taskID}"
}



#---------------------------------------------------
# MAIN EXECUTION
#---------------------------------------------------

  #Store all arguments as given in this variable
  all_args=$*

  if [ -z "${COMPSS_HOME}" ]; then
    COMPSS_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/../../../../../.. && pwd )/"
  fi
  if [ ! "${COMPSS_HOME: -1}" = "/" ]; then
    COMPSS_HOME="${COMPSS_HOME}/"
  fi
  export COMPSS_HOME=${COMPSS_HOME}
  GOS_SCRIPTS_DIR="${COMPSS_HOME}Runtime/scripts/system/adaptors/gos/"

  # shellcheck source=common.sh
  # shellcheck disable=SC1091
  source "${GOS_SCRIPTS_DIR}queues/common.sh"
  # shellcheck source=response.sh
  # shellcheck disable=SC1091
  source "${GOS_SCRIPTS_DIR}response.sh"


  # Get command args (loads args from commons.sh, specially sc_cfg)
  get_args "$@"

  # shellcheck disable=SC2154
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

  get_task_launch_parameters

  NEW_OUTPUT="${workingDir}BatchOutput/${taskID}"
  echo "[GOSWorker submitBatch.sh] Redirecting batch compss-output inside the workingDir: ${NEW_OUTPUT}"
  export REDIRECT_OUTPUT="true"
  # Set wall clock time
  set_time

  # Log received arguments
  log_args

  # Create TMP submit script
  create_normal_task_submit



  trap cleanup EXIT

  # Submit
  submit
