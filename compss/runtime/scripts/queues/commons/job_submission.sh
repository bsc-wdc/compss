#!/bin/bash

if [ -n "${LOADED_QUEUES_COMMONS_JOB_SUBMISSION}" ]; then
  return 0
fi

# Checking COMPSs_HOME
if [ -z "${COMPSS_HOME}" ]; then
  echo "COMPSS_HOME not defined"
  exit 1
fi


# shellcheck source=./logger.sh"
# shellcheck disable=SC1091
source "${COMPSS_HOME}Runtime/scripts/system/commons/logger.sh"



#---------------------------------------------------
# SCRIPT CONSTANTS DECLARATION
#---------------------------------------------------
DEFAULT_SC_CFG="default"
DEFAULT_JOB_NAME="NO_NAME"  # Should be updated by other scripts after sourcing this
DEFAULT_NVRAM_OPTIONS="none"

#---------------------------------------------------
# ERROR CONSTANTS DECLARATION
#---------------------------------------------------
ERROR_CFG_SC="SuperComputer CFG file doesn't exist"
ERROR_CFG_Q="Queue system CFG file doesn't exist"

ERROR_CLUSTER_NA="Cluster not defined (use --cluster flag)"
ERROR_PROJECT_NAME_NA="Project name not defined (use --project_name flag)"

ERROR_NUM_NODES="Invalid number of nodes"
ERROR_SWITCHES="Too little switches for the specified number of nodes"
ERROR_NO_ASK_SWITCHES="Cannot ask switches for less than ${MIN_NODES_REQ_SWITCH} nodes"

ERROR_NUM_CPUS="Invalid number of CPUS per node"
ERROR_NODE_MEMORY="Incorrect node_memory parameter. Only disabled or <int> allowed. I.e. 33000, 66000"

ERROR_SUBMIT="Error submiting script to queue system"

###############################################
# Function to load the SC configuration
# $1: sc config
###############################################
load_SC_config(){
  local config_file="${1}"
  if [ -f "${config_file}" ]; then
     # shellcheck source=../supercomputers/default.cfg
     # shellcheck disable=SC1091
     # shellcheck disable=SC2154
     source "${config_file}"
  else
     # Load specific queue system variables
     # shellcheck source=../supercomputers/default.cfg
     # shellcheck disable=SC1091
     # shellcheck disable=SC2154
    if  [ -f "${COMPSS_HOME}Runtime/scripts/queues/supercomputers/${config_file}" ]; then
      source "${COMPSS_HOME}Runtime/scripts/queues/supercomputers/${config_file}"
    elif [ -f "${COMPSS_HOME}Runtime/scripts/queues/supercomputers/${config_file}.cfg" ]; then
      source "${COMPSS_HOME}Runtime/scripts/queues/supercomputers/${config_file}.cfg"
    else
     fatal_error "${ERROR_CFG_SC}" 1
    fi

  fi

  # Load SC Queue System options
  if [ -f "${COMPSS_HOME}Runtime/scripts/queues/queue_systems/${QUEUE_SYSTEM}.cfg" ]; then
    # Load specific queue system flags
    # shellcheck source=../queue_systems/slurm.cfg
    # shellcheck disable=SC1091
    source "${COMPSS_HOME}Runtime/scripts/queues/queue_systems/${QUEUE_SYSTEM}.cfg"
  else
    fatal_error "${ERROR_CFG_Q}"
  fi
}

###############################################
# Function that parses the options
# $* list of options
###############################################
parse_job_submission_options() {
  local OPTIND
  while getopts :-: flag; do
    case "$flag" in
    -)
      case "$OPTARG" in
        # Queue System Configuration options
        sc_cfg=*)
          sc_cfg=${OPTARG//sc_cfg=/}
          ;;
        # Submission options
        job_name=*)
          job_name=${OPTARG//job_name=/}
          ;;
        project_name=*)
          project_name=${OPTARG//project_name=/}
          ;;
        queue=*)
          queue=${OPTARG//queue=/}
          ;;
        reservation=*)
          reservation=${OPTARG//reservation=/}
          ;;
        qos=*)
          qos=${OPTARG//qos=/}
          ;;
        exec_time=*)
         exec_time=${OPTARG//exec_time=/}
         ;;
        job_dependency=*)
          dependencyJob=${OPTARG//job_dependency=/}
          ;;
        # Infrastructure options
        constraints=*)
          constraints=${OPTARG//constraints=/}
          ;;
        licenses=*)
          licenses=${OPTARG//licenses=/}
          ;;
        cluster=*)
          cluster=${OPTARG//cluster=/}
          ;;
        num_nodes=*)
          num_nodes=${OPTARG//num_nodes=/}
          ;;
        num_switches=*)
          num_switches=${OPTARG//num_switches=/}
          ;;
        # node options
        cpus_per_node=*)
          cpus_per_node=${OPTARG//cpus_per_node=/}
          ;;
        gpus_per_node=*)
          gpus_per_node=${OPTARG//gpus_per_node=/}
          ;;
        node_memory=*)
          node_memory=${OPTARG//node_memory=/}
          ;;
        nvram_options=*)
          nvram_options=${OPTARG//nvram_options=/}
          ;;
        file_systems=*)
          file_systems=${OPTARG//file_systems=/}
          ;;
        working_dir=*)
          submission_working_dir=${OPTARG//working_dir=/}
          ;;
        # extra flags
        extra_submit_flag=*)
          extra_submit_flag=(${extra_submit_flag[@]} ${OPTARG//extra_submit_flag=/})
         ;;
        *)
          fatal_error "Unknown option --$OPTARG"  1
          ;;
      esac
      ;;
    *)
      fatal_error "Unknown flag: $flag" 1
    esac
  done
}


###############################################
# Function to display the options
###############################################
log_submission_opts() {
  echo "Submission options"
  # Display generic arguments
  echo "    SC Configuration:          ${sc_cfg}"
  echo "    JobName:                   ${job_name}"
  if [ -n "${ENABLE_PROJECT_NAME}" ] && [ "${ENABLE_PROJECT_NAME}" == "true" ]; then
    echo "    Project name:              ${project_name}"
  fi
  echo "    Queue:                     ${queue}"
  echo "    Reservation:               ${reservation}"
  if [ -z "${DISABLE_QARG_QOS}" ] || [ "${DISABLE_QARG_QOS}" == "false" ]; then
    echo "    QoS:                       ${qos}"
  fi
  echo "    Exec-Time:                 ${wc_limit}"
  echo "    Job dependency:            ${dependencyJob}"
  if [ -z "${DISABLE_QARG_CONSTRAINTS}" ] || [ "${DISABLE_QARG_CONSTRAINTS}" == "false" ]; then
    echo "    Constraints:               ${constraints}"
  fi
  if [ -z "${DISABLE_QARG_LICENSES}" ] || [ "${DISABLE_QARG_LICENSES}" == "false" ]; then
    echo "    Licenses:                  ${licenses}"
  fi
  if [ "${ENABLE_QARG_CLUSTER}" == "true" ]; then
    echo "    Cluster:                   ${cluster}"
  fi
  echo "    Num Nodes:                 ${num_nodes}"
  echo "    Num Switches:              ${num_switches}"
  echo "    CPUs per node:             ${cpus_per_node}"
  echo "    GPUs per node:             ${gpus_per_node}"
  echo "    Memory per node:           ${node_memory}"
  if [ -z "${DISABLE_QARG_NVRAM}" ] || [ "${DISABLE_QARG_NVRAM}" == "false" ]; then
    echo "    NVRAM options:             ${nvram_options}"
  fi
  if [ -n "${ENABLE_FILE_SYSTEMS}" ] && [ "${ENABLE_FILE_SYSTEMS}" == "true" ]; then
    echo "    File Systems:              ${file_systems}"
  fi
  echo "    Submission Working dir:    ${submission_working_dir}"
  echo "    Extra submission flags:"
  if [ -n "${extra_submit_flag}" ]; then
    for flag in "${extra_submit_flag[@]}"; do
       flag=${flag//#/ }
       echo "        - ${flag}"
    done
  fi
}

###############################################
# Function to check the arguments
###############################################
check_job_submission_options() {
  # Check sc configuration argument
  if [ -z "${sc_cfg}" ]; then
    sc_cfg=${DEFAULT_SC_CFG}
  fi
  load_SC_config "${sc_cfg}"

  ###############################################################
  # Queue system checks
  ###############################################################
  if [ -z "${job_name}" ]; then
    job_name=${DEFAULT_JOB_NAME}
  fi

  if [ "${ENABLE_PROJECT_NAME}" == "true" ] && [ -z "${project_name}" ]; then
    fatal_error "${ERROR_PROJECT_NAME_NA}" 1
  fi

  if [ -z "${queue}" ]; then
    queue=${DEFAULT_QUEUE}
  fi

  if [ -z "${reservation}" ]; then
    reservation=${DEFAULT_RESERVATION}
  fi

  if [ -z "${qos}" ]; then
    qos=${DEFAULT_QOS}
  fi

  if [ -z "${exec_time}" ]; then
    exec_time=${DEFAULT_EXEC_TIME}
  fi
  if [ -z "${WC_CONVERSION_FACTOR}" ]; then
    convert_to_wc "$exec_time"
    submission_wc_limit=${wc_limit}
  else
    submission_wc_limit=$((exec_time * WC_CONVERSION_FACTOR))
  fi


  if [ -z "${dependencyJob}" ]; then
    dependencyJob=${DEFAULT_DEPENDENCY_JOB}
  fi

  ###############################################################
  # Infrastructure checks
  ###############################################################
  if [ -z "${constraints}" ]; then
    constraints=${DEFAULT_CONSTRAINTS}
  fi

  if [ -z "${licenses}" ]; then
    licenses=${DEFAULT_LICENSES}
  fi

  if [ "${ENABLE_QARG_CLUSTER}" == "true" ] && [ -z "${cluster}" ]; then
    fatal_error "${ERROR_CLUSTER_NA}}" 1
  fi

  if [ -z "${num_nodes}" ]; then
    num_nodes=${DEFAULT_NUM_NODES}
  fi

  if [ "${num_nodes}" -lt "${MINIMUM_NUM_NODES}" ]; then
    fatal_error "${ERROR_NUM_NODES}"  1
  fi

  if [ -z "${num_switches}" ]; then
    num_switches=${DEFAULT_NUM_SWITCHES}
  fi

  local maxnodes=$((num_switches * MAX_NODES_SWITCH))

  if [ "${num_switches}" != "0" ] && [ "${maxnodes}" -lt "${num_nodes}" ]; then
    fatal_error "${ERROR_SWITCHES}" 1
  fi

  if [ "${num_nodes}" -lt "${MIN_NODES_REQ_SWITCH}" ] && [ "${num_switches}" != "0" ]; then
    fatal_error "${ERROR_NO_ASK_SWITCHES}" 1
  fi

  ###############################################################
  # Node checks
  ###############################################################
  if [ -z "${cpus_per_node}" ]; then
    cpus_per_node=${DEFAULT_CPUS_PER_NODE}
  fi

  if [ "${cpus_per_node}" -lt "${MINIMUM_CPUS_PER_NODE}" ]; then
    fatal_error "${ERROR_NUM_CPUS}" 1
  fi

  if [ -z "${gpus_per_node}" ]; then
    gpus_per_node=${DEFAULT_GPUS_PER_NODE}
  fi

  if [ -z "${node_memory}" ]; then
    node_memory=${DEFAULT_NODE_MEMORY}
  elif [ "${node_memory}" != "disabled" ] && ! [[ "${node_memory}" =~ ^[0-9]+$ ]]; then
    fatal_error "${ERROR_NODE_MEMORY}" 1
  fi

  if [ -z "${nvram_options}" ]; then
    nvram_options=${DEFAULT_NVRAM_OPTIONS}
  fi

 # file systes can be empty

  if [ -z "${submission_working_dir}" ]; then
    submission_working_dir=${DEFAULT_JOB_EXECUTION_DIR}
  fi

 # extra_submission_flags can be empty

}

###############################################
# Function that converts a cost in minutes
# to an expression of wall clock limit
# $1: time
# Result left in wc_limit global variable
###############################################
convert_to_wc() {
  local cost=$1
  wc_limit=${EMPTY_WC_LIMIT}

  local min=$((cost % 60))
  if [ $min -lt 10 ]; then
    wc_limit=":0${min}${wc_limit}"
  else
    wc_limit=":${min}${wc_limit}"
  fi

  local hrs=$((cost / 60))
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
# Appends into a script all the headers for a submission
# $1: script to update
###############################################
append_submission_headers_to_script() {
  local submit_script="${1}"
  # Add queue selection
  if [ "${queue}" != "default" ]; then
    cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_QUEUE_SELECTION}${QUEUE_SEPARATOR}${queue}
EOT
  fi

  # Switches selection
  if [ -n "${QARG_NUM_SWITCHES}" ]; then
    if [ "${num_switches}" != "0" ]; then
      cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_NUM_SWITCHES}${QUEUE_SEPARATOR}"cu[maxcus=${num_switches}]"
EOT
    fi
  fi

  # GPU selection
  if [ -n "${QARG_GPUS_PER_NODE}" ]; then
    if [ "${gpus_per_node}" != "0" ]; then
      cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_GPUS_PER_NODE}${QUEUE_SEPARATOR}${gpus_per_node}
EOT
    fi
  fi

  # Add Job name and job dependency
  if [ "${dependencyJob}" != "None" ]; then
    if [ "${QARG_JOB_DEP_INLINE}" == "true" ]; then
      cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_JOB_NAME}${QUEUE_SEPARATOR}${job_name} ${QARG_JOB_DEPENDENCY_OPEN}${dependencyJob}${QARG_JOB_DEPENDENCY_CLOSE}
EOT
    else
      cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_JOB_NAME}${QUEUE_SEPARATOR}${job_name}
#${QUEUE_CMD} ${QARG_JOB_DEPENDENCY_OPEN}${dependencyJob}${QARG_JOB_DEPENDENCY_CLOSE}
EOT
    fi
  else
    cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_JOB_NAME}${QUEUE_SEPARATOR}${job_name}
EOT
  fi

  # Reservation
  if [ -n "${QARG_RESERVATION}" ]; then
    if [ "${reservation}" != "disabled" ]; then
      cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_RESERVATION}${QUEUE_SEPARATOR}${reservation}
EOT
    fi
  fi

  # QoS
  if [ -n "${QARG_QOS}" ]; then
    if [ "${qos}" != "default" ]; then
      if [ -z "${DISABLE_QARG_QOS}" ] || [ "${DISABLE_QARG_QOS}" == "false" ]; then
      	cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_QOS}${QUEUE_SEPARATOR}${qos}
EOT
      fi
    fi
  fi

  # QoS
  if [ -n "${QARG_OVERCOMMIT}" ]; then
      if [ -z "${DISABLE_QARG_OVERCOMMIT}" ] || [ "${DISABLE_QARG_OVERCOMMIT}" == "false" ]; then
        cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_OVERCOMMIT}
EOT
      fi
  fi

  # Constraints
  if [ -n "${QARG_CONSTRAINTS}" ]; then
    if [ "${constraints}" != "disabled" ]; then
      if [ -z "${DISABLE_QARG_CONSTRAINTS}" ] || [ "${DISABLE_QARG_CONSTRAINTS}" == "false" ]; then
        cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_CONSTRAINTS}${QUEUE_SEPARATOR}${constraints}
EOT
      fi
    fi
  fi

  # Licenses
  if [ -n "${QARG_LICENSES}" ]; then
    if [ "${licenses}" != "disabled" ]; then
      if [ -z "${DISABLE_QARG_LICENSES}" ] || [ "${DISABLE_QARG_LICENSES}" == "false" ]; then
        cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_LICENSES}${QUEUE_SEPARATOR}${licenses}
EOT
      fi
    fi
  fi

  # Node memory
  if [ -n "${QARG_MEMORY}" ]; then
    if [ "${node_memory}" != "disabled" ]; then
      if [ -z "${DISABLE_QARG_MEMORY}" ] || [ "${DISABLE_QARG_MEMORY}" == "false" ]; then
          cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_MEMORY}${QUEUE_SEPARATOR}${node_memory}
EOT
      fi
    fi
  fi

  # Add argument when exclusive mode is available
  if [ -n "${QARG_EXCLUSIVE_NODES}" ]; then
    if [ "${EXCLUSIVE_MODE}" != "disabled" ]; then
      cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_EXCLUSIVE_NODES}
EOT
    fi
  fi

  # Add argument when copy_env is defined
  if [ -n "${QARG_COPY_ENV}" ]; then
    cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_COPY_ENV}
EOT
  fi

  # Wall Clock
  if [ -n "${QARG_WALLCLOCK}" ]; then
    cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_WALLCLOCK}${QUEUE_SEPARATOR}${submission_wc_limit}
EOT
  fi
  #Working dir
  if [ -n "${QARG_WD}" ]; then
    cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_WD}${QUEUE_SEPARATOR}${submission_working_dir}
EOT
  fi
  # Add JOBID customizable stderr and stdout redirection when defined in queue system
  if [ -n "${QARG_JOB_OUT}" ]; then
    cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_JOB_OUT}${QUEUE_SEPARATOR}compss-${QJOB_ID}.out
EOT
  fi
  if [ -n "${QARG_JOB_ERROR}" ]; then
    cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_JOB_ERROR}${QUEUE_SEPARATOR}compss-${QJOB_ID}.err
EOT
  fi
  # Add num nodes when defined in queue system
  if [ -n "${QARG_NUM_NODES}" ]; then
    cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_NUM_NODES}${QUEUE_SEPARATOR}${num_nodes}
EOT
  fi

  # Add num processes when defined in queue system
  req_cpus_per_node=${cpus_per_node}
  if [ "${req_cpus_per_node}" -gt "${DEFAULT_CPUS_PER_NODE}" ]; then
    req_cpus_per_node=${DEFAULT_CPUS_PER_NODE}
  fi

  if [ -n "${QARG_NUM_PROCESSES}" ]; then
    if [ -n "${QNUM_PROCESSES_VALUE}" ]; then
      eval processes="${QNUM_PROCESSES_VALUE}"
    else
      processes=${req_cpus_per_node}
    fi
    echo "Requesting $processes processes"
    cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_NUM_PROCESSES}${QUEUE_SEPARATOR}${processes}
EOT
  fi

  # Span argument if defined on queue system
  if [ -n "${QARG_SPAN}" ]; then
    cat >> "${submit_script}" << EOT
#${QUEUE_CMD} $(eval "echo ${QARG_SPAN}")
EOT
  fi

  # Add project name defined in queue system
  if [ -n "${ENABLE_PROJECT_NAME}" ] && [ "${ENABLE_PROJECT_NAME}" == "true" ]; then
    if [ -n "${QARG_PROJECT_NAME}" ] && [ -n "${project_name}" ]; then
      cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_PROJECT_NAME}${QUEUE_SEPARATOR}${project_name}
EOT
    fi
  fi

  # Add file systems in queue system
  if [ -n "${ENABLE_FILE_SYSTEMS}" ] && [ "${ENABLE_FILE_SYSTEMS}" == "true" ]; then
    if [ -n "${QARG_FILE_SYSTEMS}" ] && [ -n "${file_systems}" ]; then
      cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_FILE_SYSTEMS}${QUEUE_SEPARATOR}${file_systems}
EOT
    fi
  fi

  # Add cluster name defined in queue system
  if [ -n "${QARG_CLUSTER}" ]; then
    if [ "${ENABLE_QARG_CLUSTER}" == "true" ]; then
       cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_CLUSTER}${QUEUE_SEPARATOR}${cluster}
EOT
    fi
  fi

  # Add NVRAM options if provided
  if [ "${nvram_options}" != "none" ]; then
    if [ -z "${DISABLE_QARG_NVRAM}" ] || [ "${DISABLE_QARG_NVRAM}" == "false" ]; then
    cat >> "${submit_script}" << EOT
#${QUEUE_CMD} --nvram-options=${nvram_options}
EOT
    fi
  fi

  if [ -n "${extra_submit_flag}" ]; then
    for flag in "${extra_submit_flag[@]}"; do
       flag=${flag//#/ }
       cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${flag}
EOT
    done
  fi
}

###############################################
# Appends into a script a source to a script defining the environment
# $1: script to update
###############################################
append_source_script_to_script() {
  local src_script="${1}"
  local submit_script="${2}"
     cat >> "${submit_script}" << EOT
source ${src_script}
EOT
}

###############################################
# Appends into a script a packjob separator
# $1: script to update
###############################################
append_packjob_separator_to_script(){
    local submit_script="${1}"
      cat >> "${submit_script}" << EOT
#${QUEUE_CMD} ${QARG_PACKJOB}
EOT
}


###############################################
# Appends into a script a directory change to the master working directory
# $1: script to update
###############################################
append_cd_master_wd_to_script(){
    local submit_script="${1}"
  #Add change to master working dir if not working dir option in job definition
  if [ -z "${QARG_WD}" ]; then
    cat >> "${submit_script}" << EOT
  cd ${submission_working_dir}
EOT
  fi
}

###############################################
# Appends into a script the list of master and worker nodes
# $1: script to update
###############################################
append_master_and_worker_nodes(){
  local submit_script="${1}"
  append_cd_master_wd_to_script "${submit_script}"
  # Host list parsing
  cat >> "${submit_script}" << EOT
  if [ "${HOSTLIST_CMD}" == "nodes.sh" ]; then
    source "${COMPSS_HOME}Runtime/scripts/queues/${HOSTLIST_CMD}"
  else
    host_list=\$(${HOSTLIST_CMD} \$${ENV_VAR_NODE_LIST} ${HOSTLIST_TREATMENT})
    master_node=\$(${MASTER_NAME_CMD})
    worker_nodes=\$(echo \${host_list} | sed -e "s/\${master_node}//g")
  fi

EOT
}

###############################################
# Appends into a script the master node
# $1: script to update
###############################################
append_only_master_node(){
  local submit_script="${1}"
  append_cd_master_wd_to_script "${submit_script}"
  # Host list parsing
  cat >> "${submit_script}" << EOT
  if [ "${HOSTLIST_CMD}" == "nodes.sh" ]; then
    source "${COMPSS_HOME}Runtime/scripts/queues/${HOSTLIST_CMD}"
  else
    host_list=\$(${HOSTLIST_CMD} \$${ENV_VAR_NODE_LIST} ${HOSTLIST_TREATMENT})
    master_node=\$(${MASTER_NAME_CMD})
    worker_nodes=""
  fi

EOT
}

###############################################
# Appends into a script the worker nodes
# $1: script to update
###############################################
append_only_worker_nodes(){
  local submit_script="${1}"
  local env_var_suffix=${2}
  # Host list parsing

  cat >> "${submit_script}" << EOT
  if [ "${HOSTLIST_CMD}" == "nodes.sh" ]; then
    source "${COMPSS_HOME}Runtime/scripts/queues/${HOSTLIST_CMD}"
  else
    host_list=\$(${HOSTLIST_CMD} \$${ENV_VAR_NODE_LIST}${env_var_suffix} ${HOSTLIST_TREATMENT})
    worker_nodes=\$(echo \${host_list})
  fi

EOT
}

###############################################
# Submits the script
# $1: script to submit
###############################################
submit(){
  local submit_script="${1}"
  # shellcheck disable=SC2086
  eval ${SUBMISSION_CMD} ${SUBMISSION_PIPE}${submit_script}
  result=$?

  # Check if submission failed
  if [ $result -ne 0 ]; then
    submit_err=$(cat "${submit_script}.err")
    echo "${ERROR_SUBMIT}${submit_err}"
    exit 1
  fi
}

LOADED_QUEUES_COMMONS_JOB_SUBMISSION=1
