#!/bin/bash

  ###############################################
  # ERROR Constants
  ###############################################
  ERROR_TMP_FILE="Cannnot create tmp file"
  ERROR_SUBMIT="Cannot submit the job"
  ERROR_NUM_NODES="Incorrect number of nodes requested. MININUM ${MINIMUM_NUM_NODES}."
  ERROR_TASKS_PER_NODE="Incorrect number of tasks per node. MINIMUM ${MINIMUM_TASKS_PER_NODE}."
  

  #---------------------------------------------------------------------------------------
  # FIRST ORDER FUNCTIONS
  #---------------------------------------------------------------------------------------
  
  ###############################################
  # Function to get and log parameters, and to define script variables
  ###############################################
  get_and_log_parameters() {
    # Get script parameters
    sc_cfg=$1
    queue=$2
    reservation=$3
    convert_to_wc $4
    dependencyJob=$5
    num_nodes=$6
    num_switches=$7
    tasks_per_node=$8
    node_memory=$9
    network=${10}
    master_port=${11}
    master_working_dir=${12}
    jvm_master_opts=${13}
    worker_working_dir=${14}
    jvm_workers_opts=${15}
    worker_in_master_tasks=${16}
    worker_in_master_memory=${17}
    jvm_worker_in_master_opts=${18}
    library_path=${19}
    cp=${20}
    pythonpath=${21}
    lang=${22}
    log_level=${23}
    tracing=${24}
    comm=${25}
    storageName=${26}
    storageConf=${27}
    taskExecution=${28}
    paramsToShift=28
    shift ${paramsToShift}
    toCOMPSs=$@
  
    # Display arguments
    echo "SC Configuration:          ${sc_cfg}"
    echo "Queue:                     ${queue}"
    echo "Reservation:	             ${reservation}"
    echo "Num Nodes:                 ${num_nodes}"
    echo "Num Switches:              ${num_switches}"
    echo "Job dependency:            ${dependencyJob}"
    echo "Exec-Time:                 ${wc_limit}"
    echo "Network:                   ${network}"
    echo "Node memory:	             ${node_memory}"
    echo "Tasks per Node:            ${tasks_per_node}"
    echo "Worker in Master Tasks:    ${worker_in_master_tasks}"
    echo "Worker in Master Memory:   ${worker_in_master_memory}"
    echo "Master Port:               ${master_port}"
    echo "Master WD:                 ${master_working_dir}"
    echo "Worker WD:                 ${worker_working_dir}"
    echo "Master JVM Opts:           ${jvm_master_opts}"
    echo "Workers JVM Opts:          ${jvm_workers_opts}"
    echo "Worker in Master JVM Opts: ${jvm_worker_in_master_opts}"
    echo "Library Path:              ${library_path}"
    echo "Classpath:                 ${cp}"  
    echo "Pythonpath:                ${pythonpath}"
    echo "Lang:                      ${lang}"
    echo "COMM:                      ${comm}"
    echo "Storage name:	             ${storageName}"
    echo "Storage conf:	             ${storageConf}"
    echo "Task execution:	     ${taskExecution}"
    echo "To COMPSs:                 ${toCOMPSs}"
    echo " " 

    # Set script variables
    scriptDir=$(dirname $0)
    IT_HOME=${scriptDir}/../../../..
  }

  ###############################################
  # Function to check the parameter constraints
  ###############################################
  check_parameters() {
    #Check arguments
    if [ ${num_nodes} -lt ${MINIMUM_NUM_NODES} ]; then
       display_error "${ERROR_NUM_NODES}" 1
    fi
    if [ ${tasks_per_node} -lt ${MINIMUM_TASKS_PER_NODE} ]; then
       display_error "${ERROR_TASKS_PER_NODE}" 1
    fi
  }

  ###############################################
  # Function to create a TMP submit script
  ###############################################
  create_tmp_submit() {
    # Create TMP DIR for submit script
    TMP_SUBMIT_SCRIPT=$(mktemp)
    echo "Temp submit script is: $TMP_SUBMIT_SCRIPT"
    if [ $? -ne 0 ]; then
      display_error "${ERROR_TMP_FILE}" 1
    fi

    # Add queue selection
    if [ "${queue}" != "default" ]; then
      cat >> $TMP_SUBMIT_SCRIPT << EOT
#!/bin/bash
#
#${QUEUE_CMD} ${QARG_QUEUE_SELECTION} ${queue}
EOT
    else 
      cat >> $TMP_SUBMIT_SCRIPT << EOT
#!/bin/bash
#
EOT
    fi

    # Switches selection
    if [ "${num_switches}" != "0" ]; then
      cat >> $TMP_SUBMIT_SCRIPT << EOT
#${QUEUE_CMD} ${QARG_NUM_SWITCHES}${QUEUE_SEPARATOR}"cu[maxcus=${num_switches}]"
EOT
    fi

    # Add Job name and job dependency
    if [ "${dependencyJob}" != "None" ]; then
      if [ "${QARG_JOB_DEP_INLINE}" == "true" ]; then
        cat >> $TMP_SUBMIT_SCRIPT << EOT
#${QUEUE_CMD} ${QARG_JOB_NAME}${QUEUE_SEPARATOR}COMPSs ${QARG_JOB_DEPENDENCY_OPEN}${dependencyJob}${QARG_JOB_DEPENDENCY_CLOSE}
EOT
      else
        cat >> $TMP_SUBMIT_SCRIPT << EOT
#${QUEUE_CMD} ${QARG_JOB_NAME}${QUEUE_SEPARATOR}COMPSs 
#${QUEUE_CMD} ${QARG_JOB_DEPENDENCY_OPEN}${dependencyJob}${QARG_JOB_DEPENDENCY_CLOSE}
EOT
      fi
    else 
      cat >> $TMP_SUBMIT_SCRIPT << EOT
#${QUEUE_CMD} ${QARG_JOB_NAME}${QUEUE_SEPARATOR}COMPSs
EOT
    fi

    # Reservation
    if [ "${reservation}" != "disabled" ]; then
      cat >> $TMP_SUBMIT_SCRIPT << EOT
#${QUEUE_CMD} ${QARG_RESERVATION}${QUEUE_SEPARATOR}${reservation}
EOT
    fi

    # Node memory
    if [ "${node_memory}" != "disabled" ]; then
      cat >> $TMP_SUBMIT_SCRIPT << EOT
#${QUEUE_CMD} ${QARG_MEMORY}${QUEUE_SEPARATOR}${node_memory}
EOT
    fi

    # Generic arguments
    cat >> $TMP_SUBMIT_SCRIPT << EOT
#${QUEUE_CMD} ${QARG_WD}${QUEUE_SEPARATOR}${master_working_dir} 
#${QUEUE_CMD} ${QARG_JOB_OUT} compss-%J.out
#${QUEUE_CMD} ${QARG_JOB_ERROR} compss-%J.err
#${QUEUE_CMD} ${QARG_NUM_NODES}${QUEUE_SEPARATOR}${num_nodes}
#${QUEUE_CMD} ${QARG_EXCLUSIVE_NODES}
#${QUEUE_CMD} ${QARG_WALLCLOCK} $wc_limit 
EOT

    # Sandbox and execute command
    cat >> $TMP_SUBMIT_SCRIPT << EOT
specific_log_dir=$HOME/.COMPSs/\${${ENV_VAR_JOB_ID}}/
mkdir -p \${specific_log_dir}

${scriptDir}/launch.sh ${IT_HOME} ${sc_cfg} \$${ENV_VAR_NODE_LIST} ${tasks_per_node} ${worker_in_master_tasks} ${worker_in_master_memory} ${worker_working_dir} "\${specific_log_dir}" "${jvm_master_opts}" "${jvm_workers_opts}" "${jvm_worker_in_master_opts}" ${network} ${node_memory} ${master_port} ${library_path} ${cp} ${pythonpath} ${lang} ${log_level} ${tracing} ${comm} ${storageName} ${storageConf} ${taskExecution} ${toCOMPSs}
EOT
  }

  ###############################################
  # Function to submit the script
  ###############################################
  submit() {
    if [ "${taskExecution}" != "compss" ]; then
      echo "Running in COMPSs and Storage mode."
      # Run directly the script
      chmod +x ${TMP_SUBMIT_SCRIPT}
      ${TMP_SUBMIT_SCRIPT}
      result=$?
    else
      echo "Running in COMPSs mode."
      # Submit the job to the queue
      ${SUBMISSION_CMD} < ${TMP_SUBMIT_SCRIPT} 1>${TMP_SUBMIT_SCRIPT}.out 2>${TMP_SUBMIT_SCRIPT}.err
      result=$?
    fi

    # Cleanup
    submit_err=$(cat ${TMP_SUBMIT_SCRIPT}.err)
    rm -rf ${TMP_SUBMIT_SCRIPT}.*

    # Check if submission failed
    if [ $result -ne 0 ]; then
      display_error "${ERROR_SUBMIT}${submit_err}" 1
    fi
  }

  
  #---------------------------------------------------------------------------------------
  # HELPER FUNCTIONS
  #---------------------------------------------------------------------------------------

  ###############################################
  # Function that converts a cost in minutes to an expression of wall clock limit
  ###############################################
  convert_to_wc() {
    local cost=$1
    wc_limit=${EMPTY_WC_LIMIT}

    local min=$(expr $cost % 60)
    if [ $min -lt 10 ]; then
      wc_limit=":0${min}${wc_limit}"
    else
      wc_limit=":${min}${wc_limit}"
    fi

    local hrs=$(expr $cost / 60)
    if [ $hrs -gt 0 ]; then
      if [ $hrs -lt 10 ]; then
        wc_limit="0${hrs}${wc_limit}"
      else
        wc_limit="${hrs}${wc_limit}"
      fi
    else
        wc_limit="00${wc_limit}"
    fi
  }

  ###############################################
  # Function to display errors and exit
  ###############################################
  display_error() {
    local errorMsg=$1
    local exitValue=$2

    echo " "
    echo "ERROR: $errorMsg"
    echo " "

    echo "Exiting..."
    exit $exitValue
  }
  
  
  #---------------------------------------------------------------------------------------
  # MAIN EXECUTION
  #---------------------------------------------------------------------------------------

  # Get parameters
  get_and_log_parameters "$@"

  # Load specific queue system variables
  source ${scriptDir}/../cfgs/${sc_cfg}
  source ${scriptDir}/../${QUEUE_SYSTEM}/${QUEUE_SYSTEM}.cfg

  # Check parameters
  check_parameters

  # Create TMP submit script
  create_tmp_submit

  # Submit
  submit

