#!/bin/bash

if [ -z "${COMPSS_HOME}" ]; then
  echo "\${COMPSS_HOME} not yet defined."
  exit 1
fi

#---------------------------------------------------
# SCRIPT CONSTANTS DECLARATION
#---------------------------------------------------
DEFAULT_SC_CFG="default"
DEFAULT_JOB_NAME="COMPSs"
DEFAULT_AGENTS_ENABLED="disabled"
DEFAULT_AGENTS_HIERARCHY="tree"
DEFAULT_NVRAM_OPTIONS="none"
DEFAULT_FORWARD_TIME_LIMIT="true"

#---------------------------------------------------
# ERROR CONSTANTS DECLARATION
#---------------------------------------------------
ERROR_NUM_NODES="Invalid number of nodes"
ERROR_NUM_CPUS="Invalid number of CPUS per node"
ERROR_NUM_IO_EXECUTORS="Invalid number of IO executors. Only integers >= 0 allowed."
ERROR_SWITCHES="Too little switches for the specified number of nodes"
ERROR_NO_ASK_SWITCHES="Cannot ask switches for less than ${MIN_NODES_REQ_SWITCH} nodes"
ERROR_NODE_MEMORY="Incorrect node_memory parameter. Only disabled or <int> allowed. I.e. 33000, 66000"
ERROR_NODE_STORAGE_BANDWIDTH="Incorrect node_storage_bandwidth parameter. Only <int> allowed. I.e. 120, 450"
ERROR_TMP_FILE="Cannot create TMP Submit file"
ERROR_STORAGE_PROPS="storage_props flag not defined"
ERROR_STORAGE_PROPS_FILE="storage_props file doesn't exist"
ERROR_PROJECT_NAME_NA="Project name not defined (use --project_name flag)"
ERROR_CLUSTER_NA="Cluster not defined (use --cluster flag)"

#---------------------------------------------------
# GLOBAL VARIABLE NAME DECLARATION
#---------------------------------------------------
STORAGE_HOME_ENV_VAR="STORAGE_HOME"

#---------------------------------------------------------------------------------------
# HELPER FUNCTIONS
#---------------------------------------------------------------------------------------

###############################################
# Displays usage
###############################################
usage() {
  local exitValue=$1

  cat <<EOT
Usage: $0 [options] application_name application_arguments

* Options:
  General:
    --help, -h                              Print this help message

    --opts                                  Show available options

    --version, -v                           Print COMPSs version

    --sc_cfg=<name>                         SuperComputer configuration file to use. Must exist inside queues/cfgs/
                                            Mandatory
                                            Default: ${DEFAULT_SC_CFG}

  Submission configuration:
EOT

  show_opts "$exitValue"
}

###############################################
# Show Options
###############################################
show_opts() {
  local exitValue=$1

  # Load default CFG for default values
  local defaultSC_cfg="${COMPSS_HOME}Runtime/scripts/queues/supercomputers/${DEFAULT_SC_CFG}.cfg"
  # shellcheck source=../supercomputers/default.cfg
  # shellcheck disable=SC1091
  source "${defaultSC_cfg}"
  local defaultQS_cfg="${COMPSS_HOME}Runtime/scripts/queues/queue_systems/${QUEUE_SYSTEM}.cfg"
  # shellcheck source=../queue_systems/slurm.cfg
  # shellcheck disable=SC1091
  source "${defaultQS_cfg}"

  # Show usage
  cat <<EOT
  General submision arguments:
    --exec_time=<minutes>                   Expected execution time of the application (in minutes)
                                            Default: ${DEFAULT_EXEC_TIME}
    --job_name=<name>                       Job name
                                            Default: ${DEFAULT_JOB_NAME}
    --queue=<name>                          Queue/partition name to submit the job. Depends on the queue system.
                                            Default: ${DEFAULT_QUEUE}
    --reservation=<name>                    Reservation to use when submitting the job.
                                            Default: ${DEFAULT_RESERVATION}
    --env_script=<path/to/script>           Script to source the required environment for the application.
                                            Default: Empty
    --extra_submit_flag=<flag>              Flag to pass queue system flags not supported by default command flags.
                                            Spaces must be added as '#'
                                            Default: Empty
EOT
   if [ -z "${DISABLE_QARG_CONSTRAINTS}" ] || [ "${DISABLE_QARG_CONSTRAINTS}" == "false" ]; then
    cat <<EOT
    --constraints=<constraints>		    Constraints to pass to queue system.
					    Default: ${DEFAULT_CONSTRAINTS}
EOT
   fi
   if [ -z "${DISABLE_QARG_LICENSES}" ] || [ "${DISABLE_QARG_LICENSES}" == "false" ]; then
    cat <<EOT
    --licenses=<licenses>	            Licenses to pass to queue system.
					    Default: ${DEFAULT_LICENSES}
EOT
   fi
   if [ "${ENABLE_QARG_CLUSTER}" == "true" ]; then
    cat <<EOT
    --cluster=<cluster>                     Cluster to pass to queue system.
                              		    Default: Empty.
EOT
   fi
   if [ -n "${ENABLE_PROJECT_NAME}" ] && [ "${ENABLE_PROJECT_NAME}" == "true" ]; then
    cat <<EOT

    --project_name=<name>                   Project name to pass to queue system.
                                            Default: Empty.
EOT
  fi
  if [ -n "${ENABLE_FILE_SYSTEMS}" ] && [ "${ENABLE_FILE_SYSTEMS}" == "true" ]; then
    cat <<EOT

    --file_systems=<name>                   File systems name to pass to queue system.
                                            Default: Empty
EOT
  fi
  if [ -z "${DISABLE_QARG_QOS}" ] || [ "${DISABLE_QARG_QOS}" == "false" ]; then
    cat <<EOT
    --qos=<qos>                             Quality of Service to pass to the queue system.
                                            Default: ${DEFAULT_QOS}
EOT
  fi

  if [ -z "${DISABLE_QARG_CPUS_PER_TASK}" ] || [ "${DISABLE_QARG_CPUS_PER_TASK}" == "false" ]; then
    cat <<EOT
    --forward_cpus_per_node=<true|false>    Flag to indicate if number to cpus per node must be forwarded to the worker process.
					    The number of forwarded cpus will be equal to the cpus_per_node in a worker node and
                                            equal to the worker_in_master_cpus in a master node.
                                            Default: ${DEFAULT_FORWARD_CPUS_PER_NODE}
EOT
  fi
  if [ -z "${DISABLE_QARG_NVRAM}" ] || [ "${DISABLE_QARG_NVRAM}" == "false" ]; then
    cat <<EOT
    --nvram_options="<string>"              NVRAM options (e.g. "1LM:2000" | "2LM:1000")
                                            Default: ${DEFAULT_NVRAM_OPTIONS}
EOT
  fi
    cat <<EOT
    --job_dependency=<jobID>                Postpone job execution until the job dependency has ended.
                                            Default: ${DEFAULT_DEPENDENCY_JOB}
    --forward_time_limit=<true|false>	    Forward the queue system time limit to the runtime.
					    It will stop the application in a controlled way.
					    Default: ${DEFAULT_FORWARD_TIME_LIMIT}
    --storage_home=<string>                 Root installation dir of the storage implementation.
                                            Can be defined with the ${STORAGE_HOME_ENV_VAR} environment variable.
                                            Default: ${DEFAULT_STORAGE_HOME}
    --storage_props=<string>                Absolute path of the storage properties file
                                            Mandatory if storage_home is defined
  Agents deployment arguments:
    --agents=<string>                       Hierarchy of agents for the deployment. Accepted values: plain|tree
                                            Default: ${DEFAULT_AGENTS_HIERARCHY}
    --agents                                Deploys the runtime as agents instead of the classic Master-Worker deployment.
                                            Default: ${DEFAULT_AGENTS_ENABLED}

  Homogeneous submission arguments:
    --num_nodes=<int>                       Number of nodes to use
                                            Default: ${DEFAULT_NUM_NODES}
    --num_switches=<int>                    Maximum number of different switches. Select 0 for no restrictions.
                                            Maximum nodes per switch: ${MAX_NODES_SWITCH}
                                            Only available for at least ${MIN_NODES_REQ_SWITCH} nodes.
                                            Default: ${DEFAULT_NUM_SWITCHES}
  Heterogeneous submission arguments:
    --type_cfg=<file_location>              Location of the file with the descriptions of node type requests
                                            File should follow the following format:
                                            type_X(){
                                              cpus_per_node=24
                                              node_memory=96
                                              ...
                                            }
                                            type_Y(){
                                              ...
                                            }
    --master=<master_node_type>             Node type for the master
                                            (Node type descriptions are provided in the --type_cfg flag)
    --workers=type_X:nodes,type_Y:nodes     Node type and number of nodes per type for the workers
                                            (Node type descriptions are provided in the --type_cfg flag)
  Launch configuration:
EOT
  if [ "${agents_enabled}" = "enabled" ]; then
    "${COMPSS_HOME}/Runtime/scripts/user/launch_compss_agents" --opts
  else
    "${COMPSS_HOME}/Runtime/scripts/user/launch_compss" --opts
  fi

  "${GOS_SCRIPTS_DIR}worker.sh" -help

  exit "$exitValue"
}

###############################################
# Displays version
###############################################
display_version() {
  local exitValue=$1

  "${COMPSS_HOME}/Runtime/scripts/queues/user/runcompss" --version

  exit "$exitValue"
}

###############################################
# Displays errors when treating arguments
###############################################
display_error() {
  local error_msg=$1

  echo "$error_msg"
  echo " "

  usage 1
}

###############################################
# Displays errors when executing
###############################################
display_execution_error() {
  local error_msg=$1

  echo "$error_msg"
  echo " "

  exit 1
}

###############################################
# Function to log the arguments
###############################################
log_args() {
  # Display generic arguments
  echo "SC Configuration:          ${sc_cfg}"
  echo "JobName:                   ${job_name}"
  echo "Queue:                     ${queue}"
  if [ "${agents_enabled}" = "enabled" ]; then
    echo "Deployment:                Agents"
    echo "Agents hierarchy:          ${agents_hierarchy}"
  else
    echo "Deployment:                Master-Worker"
  fi
  echo "Reservation:               ${reservation}"
  echo "Num Nodes:                 ${num_nodes}"
  echo "Num Switches:              ${num_switches}"
  echo "GPUs per node:             ${gpus_per_node}"
  echo "Job dependency:            ${dependencyJob}"
  echo "Exec-Time:                 ${wc_limit}"

  # Display optional arguments
  if [ -z "${DISABLE_QARG_QOS}" ] || [ "${DISABLE_QARG_QOS}" == "false" ]; then
    echo "QoS:                       ${qos}"
  fi
  if [ "${ENABLE_QARG_CLUSTER}" == "true" ]; then
    echo "Cluster:                   ${cluster}"
  fi

  if [ -n "${ENABLE_PROJECT_NAME}" ] && [ "${ENABLE_PROJECT_NAME}" == "true" ]; then
    echo "Project name:              ${project_name}"
  fi

  if [ -n "${ENABLE_FILE_SYSTEMS}" ] && [ "${ENABLE_FILE_SYSTEMS}" == "true" ]; then
    echo "File Systems:              ${file_systems}"
  fi

  if [ -z "${DISABLE_QARG_CONSTRAINTS}" ] || [ "${DISABLE_QARG_CONSTRAINTS}" == "false" ]; then
    echo "Constraints:               ${constraints}"
  fi

  if [ -z "${DISABLE_QARG_LICENSES}" ] || [ "${DISABLE_QARG_LICENSES}" == "false" ]; then
    echo "Licenses:                  ${licenses}"
  fi

  if [ -z "${DISABLE_QARG_NVRAM}" ] || [ "${DISABLE_QARG_NVRAM}" == "false" ]; then
    echo "NVRAM options:             ${nvram_options}"
  fi

  # Display storage arguments
  echo "Storage Home:              ${storage_home}"
  echo "Storage Properties:        ${storage_props}"

  # Display arguments to runcompss
  local other
  other=$(echo "${args_pass}" | sed 's/\ --/\n\t\t\t--/g')
  echo "Other:                     $other"
  echo " "
}

###############################################
# Function that converts a cost in minutes
# to an expression of wall clock limit
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

#---------------------------------------------------
# MAIN FUNCTIONS DECLARATION
#---------------------------------------------------

###############################################
# Function to get the arguments
###############################################
get_args() {
  # Avoid enqueue if there is no application
  if [ $# -eq 0 ]; then
    usage 1
  fi

  #Parse COMPSs Options
  while getopts hvgtmdp-: flag; do
    # Treat the argument
    case "$flag" in
      h)
        # Display help
        usage 0
        ;;
      v)
        # Display version
        display_version 0
        ;;
      -)
        # Check more complex arguments
        case "$OPTARG" in
          help)
            # Display help
            usage 0
            ;;
          version)
            # Display compss version
            display_version 0
            ;;
          opts)
            # Display options
            show_opts 0
            ;;
          sc_cfg=*)
            sc_cfg=${OPTARG//sc_cfg=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          job_name=*)
            job_name=${OPTARG//job_name=/}
            ;;
          master_working_dir=*)
            master_working_dir=${OPTARG//master_working_dir=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          exec_time=*)
            exec_time=${OPTARG//exec_time=/}
            ;;
          num_nodes=*)
            num_nodes=${OPTARG//num_nodes=/}
            ;;
          num_switches=*)
            num_switches=${OPTARG//num_switches=/}
            ;;
          agents=*)
            agents_hierarchy=${OPTARG//agents=/}
            agents_enabled="enabled"
            ;;
          agents)
            agents_hierarchy=${DEFAULT_AGENTS_HIERARCHY}
            agents_enabled="enabled"
            ;;
          cpus_per_node=*)
            cpus_per_node=${OPTARG//cpus_per_node=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          io_executors=*)
            io_executors=${OPTARG//io_executors=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          gpus_per_node=*)
            gpus_per_node=${OPTARG//gpus_per_node=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          queue=*)
            queue=${OPTARG//queue=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          reservation=*)
            reservation=${OPTARG//reservation=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          qos=*)
            qos=${OPTARG//qos=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          cpus_per_task)
            cpus_per_task="true"
            args_pass="$args_pass --$OPTARG"
            ;;
          constraints=*)
            constraints=${OPTARG//constraints=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          licenses=*)
            licenses=${OPTARG//licenses=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          cluster=*)
            cluster=${OPTARG//cluster=/}
            ;;
          project_name=*)
            project_name=${OPTARG//project_name=/}
            ;;
	  file_systems=*)
            file_systems=${OPTARG//file_systems=/}
            ;;
          job_dependency=*)
            dependencyJob=${OPTARG//job_dependency=/}
            ;;
          node_memory=*)
            node_memory=${OPTARG//node_memory=/}
            ;;
          node_storage_bandwidth=*)
            node_storage_bandwidth=${OPTARG//node_storage_bandwidth=/}
            ;;
          network=*)
            network=${OPTARG//network=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          storage_home=*)
            storage_home=${OPTARG//storage_home=/}
            ;;
          storage_props=*)
            storage_props=${OPTARG//storage_props=/}
            ;;
          storage_conf=*)
            # Storage conf is automatically generated. Remove it from COMPSs flags
            echo "WARNING: storage_conf is automatically generated. Omitting parameter"
            ;;
          # Heterogeneous submission arguments
          types_cfg=*)
            # Used in heterogeneous_submit.sh
            # shellcheck disable=SC2034
            types_cfg_file=${OPTARG//types_cfg=/}
            ;;
          master=*)
            # Used in heterogeneous_submit.sh
            # shellcheck disable=SC2034
            master_type=${OPTARG//master=/}
            ;;
          workers=*)
            # Used in heterogeneous_submit.sh
            # shellcheck disable=SC2034
            worker_types=${OPTARG//workers=/}
            ;;
          nvram_options=*)
            # Added for NEXTGENIO prototype (could be used in any other NVRAM
            # equipped supercomputer if slurm supports the flag --nvram_options)
            nvram_options=${OPTARG//nvram_options=/}
            ;;
          uuid=*)
            echo "WARNING: uuid is automatically generated. Omitting parameter"
            ;;
          forward_time_limit=)
            forward_wcl=${OPTARG//forward_time_limit=/}
	    ;;
	  wall_clock_limit=*)
	    wcl=${OPTARG//wall_clock_limit=/}
	    args_pass="$args_pass --$OPTARG"
	    ;;
	  env_script=*)
	    env_script=${OPTARG//env_script=/}
	    args_pass="$args_pass --$OPTARG"
	    ;;
	  extra_submit_flag=*)
            extra_submit_flag=(${extra_submit_flag[@]} ${OPTARG//extra_submit_flag=/})
	    ;;
          *)
            # Flag didn't match any patern. Add to COMPSs
            args_pass="$args_pass --$OPTARG"
            ;;
        esac
        ;;
      *)
        # Flag didn't match any patern. End of COMPSs flags
        args_pass="$args_pass -$flag"
        ;;
    esac
  done
  # Shift COMPSs arguments
  shift $((OPTIND-1))

  # Pass application name and args
  args_pass="$args_pass $*"
}

###############################################
# Function to set the wall clock time
###############################################
set_time() {
  if [ -z "${exec_time}" ]; then
    exec_time=${DEFAULT_EXEC_TIME}
  fi
  if [ -z "${forward_wcl}" ]; then
    forward_wcl=${DEFAULT_FORWARD_TIME_LIMIT}
  fi
  if [ -z "$wcl" ]; then
    if [ "${forward_wcl}" == "true" ]; then
       wcl=$(((exec_time - 1) * 60))
       args_pass="--wall_clock_limit=${wcl} ${args_pass}"
    fi
  fi
  if [ -z "${WC_CONVERSION_FACTOR}" ]; then
    convert_to_wc "$exec_time"
  else
    wc_limit=$((exec_time * WC_CONVERSION_FACTOR))
  fi
}

###############################################
# Function to check the arguments
###############################################
check_args() {
  ###############################################################
  # Queue system checks
  ###############################################################
  if [ -z "${job_name}" ]; then
    job_name=${DEFAULT_JOB_NAME}
  fi

  if [ -z "${queue}" ]; then
    queue=${DEFAULT_QUEUE}
  fi

  if [ -z "${reservation}" ]; then
    reservation=${DEFAULT_RESERVATION}
  fi

  if [ -z "${constraints}" ]; then
    constraints=${DEFAULT_CONSTRAINTS}
  fi

  if [ -z "${licenses}" ]; then
    licenses=${DEFAULT_LICENSES}
  fi

  if [ -z "${qos}" ]; then
    qos=${DEFAULT_QOS}
  fi

  if [ -z "${dependencyJob}" ]; then
    dependencyJob=${DEFAULT_DEPENDENCY_JOB}
  fi

  ###############################################################
  # Deployment checks
  ###############################################################
  if [ -z "${agents_enabled}" ]; then
    agents_enabled=${DEFAULT_AGENTS_ENABLED}
  fi

  if [ "${agents_enabled}" = "enabled" ]; then
    if [ -z "${agents_hierarchy}" ]; then
      agents_hierarchy=${DEFAULT_AGENTS_HIERARCHY}
    fi
  fi

  ###############################################################
  # Infrastructure checks
  ###############################################################
  if [ -z "${num_nodes}" ]; then
    num_nodes=${DEFAULT_NUM_NODES}
  fi

  if [ "${num_nodes}" -lt "${MINIMUM_NUM_NODES}" ]; then
      display_error "${ERROR_NUM_NODES}" 1
  fi

  if [ -z "${num_switches}" ]; then
    num_switches=${DEFAULT_NUM_SWITCHES}
  fi

  if [ -z "${cpus_per_node}" ]; then
    cpus_per_node=${DEFAULT_CPUS_PER_NODE}
  fi

  if [ "${cpus_per_node}" -lt "${MINIMUM_CPUS_PER_NODE}" ]; then
    display_error "${ERROR_NUM_CPUS}"
  fi

  if [ -z "${io_executors}" ]; then
    io_executors=${DEFAULT_IO_EXECUTORS}
  fi

  if [ "${io_executors}" -lt 0 ]; then
    display_error "${ERROR_NUM_IO_EXECUTORS}"
  fi

  if [ -z "${gpus_per_node}" ]; then
    gpus_per_node=${DEFAULT_GPUS_PER_NODE}
  fi

  maxnodes=$((num_switches * MAX_NODES_SWITCH))

  if [ "${num_switches}" != "0" ] && [ "${maxnodes}" -lt "${num_nodes}" ]; then
    display_error "${ERROR_SWITCHES}"
  fi

  if [ "${num_nodes}" -lt "${MIN_NODES_REQ_SWITCH}" ] && [ "${num_switches}" != "0" ]; then
    display_error "${ERROR_NO_ASK_SWITCHES}"
  fi

  # Network variable and modification
  if [ -z "${network}" ]; then
    network=${DEFAULT_NETWORK}
  elif [ "${network}" == "default" ]; then
    network=${DEFAULT_NETWORK}
  elif [ "${network}" != "ethernet" ] && [ "${network}" != "infiniband" ] && [ "${network}" != "data" ]; then
    display_error "${ERROR_NETWORK}"
  fi

  # NVRAM support
  if [ -z "${nvram_options}" ]; then
    nvram_options=${DEFAULT_NVRAM_OPTIONS}
  fi

  ###############################################################
  # Node checks
  ###############################################################
  if [ -z "${node_memory}" ]; then
    node_memory=${DEFAULT_NODE_MEMORY}
  elif [ "${node_memory}" != "disabled" ] && ! [[ "${node_memory}" =~ ^[0-9]+$ ]]; then
    display_error "${ERROR_NODE_MEMORY}"
  fi

  if [ -z "${node_storage_bandwidth}" ]; then
     node_storage_bandwidth=${DEFAULT_NODE_STORAGE_BANDWIDTH}
  elif ! [[ "${node_storage_bandwidth}" =~ ^[0-9]+$ ]]; then
     display_error "${ERROR_NODE_STORAGE_BANDWIDTH}"
  fi

  if [ -z "${master_working_dir}" ]; then
    master_working_dir=${DEFAULT_MASTER_WORKING_DIR}
  fi

  ###############################################################
  # Storage checks
  ###############################################################
  if [ -z "${storage_home}" ]; then
    # Check if STORAGE_HOME_ENV_VAR is defined in the environment
    storage_home=${!STORAGE_HOME_ENV_VAR:-$DEFAULT_STORAGE_HOME}
  fi

  if [ "${storage_home}" != "${DISABLED_STORAGE_HOME}" ]; then
    # Check storage props is defined
    if [ -z "${storage_props}" ]; then
      display_error "${ERROR_STORAGE_PROPS}"
    fi

    # Check storage props file exists
    if [ ! -f "${storage_props}" ]; then
      # PropsFile doesn't exist
      display_execution_error "${ERROR_STORAGE_PROPS_FILE}"
    fi
  fi

  ###############################################################
  # Project name check when required
  ###############################################################
  if [ "${ENABLE_PROJECT_NAME}" == "true" ] && [ -z "${project_name}" ]; then
    display_error "${ERROR_PROJECT_NAME_NA}"
  fi

  ###############################################################
  # Cluster name check when required
  ###############################################################
  if [ "${ENABLE_QARG_CLUSTER}" == "true" ] && [ -z "${cluster}" ]; then
    display_error "${ERROR_PROJECT_NAME_NA}"
  fi

}

###############################################
# Function to create a TMP submit script
###############################################
create_tmp_submit() {
  # Create TMP DIR for submit script
  TMP_SUBMIT_SCRIPT=$(mktemp)
  ev=$?
  if [ $ev -ne 0 ]; then
    display_error "${ERROR_TMP_FILE}" $ev
  fi
  echo "Temp submit script is: ${TMP_SUBMIT_SCRIPT}"

  cat > "${TMP_SUBMIT_SCRIPT}" << EOT
#!/bin/bash -e
#
EOT
}


###############################################
# MAIN ENTRY POINTS FROM SUBMIT/HETER_SUBMIT
###############################################

create_normal_tmp_submit(){
  create_tmp_submit
  add_submission_headers
  add_env_source
  add_master_and_worker_nodes
  add_launch
}



add_env_source(){
  if [ -n "${env_script}" ]; then
     cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
source ${env_script}
EOT
  fi
}

add_submission_headers(){
  # Add queue selection
  if [ "${queue}" != "default" ]; then
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_QUEUE_SELECTION}${QUEUE_SEPARATOR}${queue}
EOT
  fi

  # Switches selection
  if [ -n "${QARG_NUM_SWITCHES}" ]; then
    if [ "${num_switches}" != "0" ]; then
      cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_NUM_SWITCHES}${QUEUE_SEPARATOR}"cu[maxcus=${num_switches}]"
EOT
    fi
  fi

  # GPU selection
  if [ -n "${QARG_GPUS_PER_NODE}" ]; then
    if [ "${gpus_per_node}" != "0" ]; then
      cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_GPUS_PER_NODE}${QUEUE_SEPARATOR}${gpus_per_node}
EOT
    fi
  fi

  # Add Job name and job dependency
  if [ "${dependencyJob}" != "None" ]; then
    if [ "${QARG_JOB_DEP_INLINE}" == "true" ]; then
      cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_JOB_NAME}${QUEUE_SEPARATOR}${job_name} ${QARG_JOB_DEPENDENCY_OPEN}${dependencyJob}${QARG_JOB_DEPENDENCY_CLOSE}
EOT
    else
      cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_JOB_NAME}${QUEUE_SEPARATOR}${job_name}
#${QUEUE_CMD} ${QARG_JOB_DEPENDENCY_OPEN}${dependencyJob}${QARG_JOB_DEPENDENCY_CLOSE}
EOT
    fi
  else
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_JOB_NAME}${QUEUE_SEPARATOR}${job_name}
EOT
  fi


  # Reservation
  if [ -n "${QARG_RESERVATION}" ]; then
    if [ "${reservation}" != "disabled" ]; then
      cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_RESERVATION}${QUEUE_SEPARATOR}${reservation}
EOT
    fi
  fi

  # QoS
  if [ -n "${QARG_QOS}" ]; then
    if [ "${qos}" != "default" ]; then
      if [ -z "${DISABLE_QARG_QOS}" ] || [ "${DISABLE_QARG_QOS}" == "false" ]; then
      	cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_QOS}${QUEUE_SEPARATOR}${qos}
EOT
      fi
    fi
  fi

  # QoS
  if [ -n "${QARG_OVERCOMMIT}" ]; then
      if [ -z "${DISABLE_QARG_OVERCOMMIT}" ] || [ "${DISABLE_QARG_OVERCOMMIT}" == "false" ]; then
        cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_OVERCOMMIT}
EOT
      fi
  fi

  # Constraints
  if [ -n "${QARG_CONSTRAINTS}" ]; then
    if [ "${constraints}" != "disabled" ]; then
      if [ -z "${DISABLE_QARG_CONSTRAINTS}" ] || [ "${DISABLE_QARG_CONSTRAINTS}" == "false" ]; then
        cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_CONSTRAINTS}${QUEUE_SEPARATOR}${constraints}
EOT
      fi
    fi
  fi

  # Licenses
  if [ -n "${QARG_LICENSES}" ]; then
    if [ "${licenses}" != "disabled" ]; then
      if [ -z "${DISABLE_QARG_LICENSES}" ] || [ "${DISABLE_QARG_LICENSES}" == "false" ]; then
        cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_LICENSES}${QUEUE_SEPARATOR}${licenses}
EOT
      fi
    fi
  fi

  # Node memory
  if [ -n "${QARG_MEMORY}" ]; then
    if [ "${node_memory}" != "disabled" ]; then
      if [ -z "${DISABLE_QARG_MEMORY}" ] || [ "${DISABLE_QARG_MEMORY}" == "false" ]; then
          cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_MEMORY}${QUEUE_SEPARATOR}${node_memory}
EOT
      fi
    fi
  fi

  # Add argument when exclusive mode is available
  if [ -n "${QARG_EXCLUSIVE_NODES}" ]; then
    if [ "${EXCLUSIVE_MODE}" != "disabled" ]; then
      cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_EXCLUSIVE_NODES}
EOT
    fi
  fi

  # Add argument when copy_env is defined
  if [ -n "${QARG_COPY_ENV}" ]; then
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_COPY_ENV}
EOT
  fi

  # Wall Clock
  if [ -n "${QARG_WALLCLOCK}" ]; then
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_WALLCLOCK}${QUEUE_SEPARATOR}$wc_limit
EOT
  fi
  #Working dir
  if [ -n "${QARG_WD}" ]; then
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_WD}${QUEUE_SEPARATOR}${master_working_dir}
EOT
  fi
  # Add JOBID customizable stderr and stdout redirection when defined in queue system
  if [ -n "${QARG_JOB_OUT}" ]; then
    if [ -n "${REDIRECT_OUTPUT}" ]; then
        cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_JOB_OUT}${QUEUE_SEPARATOR}${NEW_OUTPUT}.out
EOT
    else
        cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_JOB_OUT}${QUEUE_SEPARATOR}compss-${QJOB_ID}.out
EOT
    fi
  fi
  if [ -n "${QARG_JOB_ERROR}" ]; then
    if [ -n "${REDIRECT_OUTPUT}" ]; then
      cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_JOB_ERROR}${QUEUE_SEPARATOR}${NEW_OUTPUT}.err
EOT
    else
      cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_JOB_ERROR}${QUEUE_SEPARATOR}compss-${QJOB_ID}.err
EOT
    fi
  fi

    # Add num nodes when defined in queue system
    if [ -n "${QARG_NUM_NODES}" ]; then
      cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
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
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_NUM_PROCESSES}${QUEUE_SEPARATOR}${processes}
EOT
  fi

  # Span argument if defined on queue system
  if [ -n "${QARG_SPAN}" ]; then
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} $(eval "echo ${QARG_SPAN}")
EOT
  fi

  # Add project name defined in queue system
  if [ -n "${ENABLE_PROJECT_NAME}" ] && [ "${ENABLE_PROJECT_NAME}" == "true" ]; then
    if [ -n "${QARG_PROJECT_NAME}" ] && [ -n "${project_name}" ]; then
      cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_PROJECT_NAME}${QUEUE_SEPARATOR}${project_name}
EOT
    fi
  fi

  # Add file systems in queue system
  if [ -n "${ENABLE_FILE_SYSTEMS}" ] && [ "${ENABLE_FILE_SYSTEMS}" == "true" ]; then
    if [ -n "${QARG_FILE_SYSTEMS}" ] && [ -n "${file_systems}" ]; then
      cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_FILE_SYSTEMS}${QUEUE_SEPARATOR}${file_systems}
EOT
    fi
  fi

  # Add cluster name defined in queue system
  if [ -n "${QARG_CLUSTER}" ]; then
    if [ "${ENABLE_QARG_CLUSTER}" == "true" ]; then
       cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_CLUSTER}${QUEUE_SEPARATOR}${cluster}
EOT
    fi
  fi

  # Add NVRAM options if provided
  if [ "${nvram_options}" != "none" ]; then
    if [ -z "${DISABLE_QARG_NVRAM}" ] || [ "${DISABLE_QARG_NVRAM}" == "false" ]; then
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} --nvram-options=${nvram_options}
EOT
    fi
  fi

  if [ -n "${extra_submit_flag}" ]; then
    for flag in "${extra_submit_flag[@]}"; do
       flag=${flag//#/ }
       cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${flag}
EOT
    done
  fi
}

add_packjob_separator(){
      cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_PACKJOB}
EOT
}

add_cd_master_wd(){
  #Add change to master working dir if not working dir option in job definition
  if [ -z "${QARG_WD}" ]; then
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
  cd ${master_working_dir}
EOT
  fi
}

add_master_and_worker_nodes(){
  add_cd_master_wd
  # Host list parsing
  cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
  if [ "${HOSTLIST_CMD}" == "nodes.sh" ]; then
    source "${COMPSS_HOME}/Runtime/scripts/system/${HOSTLIST_CMD}"
  else
    host_list=\$(${HOSTLIST_CMD} \$${ENV_VAR_NODE_LIST} ${HOSTLIST_TREATMENT})
    master_node=\$(${MASTER_NAME_CMD})
    worker_nodes=\$(echo \${host_list} | sed -e "s/\${master_node}//g")
  fi

EOT
}



add_only_master_node(){
  add_cd_master_wd
  # Host list parsing
  cat >> "${TMP_SUBMIT_SCRIPT}" << EOT

  if [ "${HOSTLIST_CMD}" == "nodes.sh" ]; then
    source "${COMPSS_HOME}/Runtime/scripts/system/${HOSTLIST_CMD}"
  else
    host_list=\$(${HOSTLIST_CMD} \$${ENV_VAR_NODE_LIST} ${HOSTLIST_TREATMENT})
    master_node=\$(${MASTER_NAME_CMD})
    worker_nodes=""
  fi

EOT
}

add_only_worker_nodes(){
 # Host list parsing
  local env_var_suffix=$1
  cat >> "$TMP_SUBMIT_SCRIPT" << EOT
  if [ "${HOSTLIST_CMD}" == "nodes.sh" ]; then
    source "${COMPSS_HOME}/Runtime/scripts/system/${HOSTLIST_CMD}"
  else
    host_list=\$(${HOSTLIST_CMD} \$${ENV_VAR_NODE_LIST}${env_var_suffix} ${HOSTLIST_TREATMENT})
    worker_nodes=\$(echo \${host_list})
  fi

EOT
}

add_launch(){
  if [ "${agents_enabled}" = "enabled" ]; then
    AGENTS_SUFFIX="_agents"
    AGENTS_HIERARCHY="--hierarchy=${agents_hierarchy} "
  fi

  # Storage init
  if [ "${storage_home}" != "${DISABLED_STORAGE_HOME}" ]; then
    # ADD STORAGE_INIT, STORAGE_FINISH AND NODES PARSING
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
storage_conf=$HOME/.COMPSs/\$${ENV_VAR_JOB_ID}/storage/cfgfiles/storage.properties
storage_master_node="\${master_node}"

# The storage_init.sh can put environment variables in the temporary file which will be sourced afterwards
variables_to_be_sourced=$(mktemp)
${storage_home}/scripts/storage_init.sh \$${ENV_VAR_JOB_ID} "\${master_node}" "\${storage_master_node}" "\${worker_nodes}" ${network} ${storage_props} "\${variables_to_be_sourced}"

if [ -f "\${variables_to_be_sourced}" ]; then
    source "\${variables_to_be_sourced}"
    rm "\${variables_to_be_sourced}"
fi

${COMPSS_HOME}/Runtime/scripts/user/launch_compss${AGENTS_SUFFIX} ${AGENTS_HIERARCHY} --master_node="\${master_node}" --worker_nodes="\${worker_nodes}" --node_memory=${node_memory} --node_storage_bandwidth=${node_storage_bandwidth} --storage_conf=\${storage_conf} ${args_pass}

${storage_home}/scripts/storage_stop.sh \$${ENV_VAR_JOB_ID} "\${master_node}" "\${storage_master_node}" "\${worker_nodes}" ${network} ${storage_props}

EOT
  else
    # ONLY ADD EXECUTE COMMAND
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT

${COMPSS_HOME}/Runtime/scripts/user/launch_compss${AGENTS_SUFFIX} ${AGENTS_HIERARCHY} --master_node="\${master_node}" --worker_nodes="\${worker_nodes}" --node_memory=${node_memory} --node_storage_bandwidth=${node_storage_bandwidth} ${args_pass}
EOT
  fi
}
