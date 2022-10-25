#!/bin/bash

if [ -n "${LOADED_QUEUES_COMMONS_TRACE_GENERATION}" ]; then
  return 0
fi

# Checking up COMPSs_HOME
if [ -z "${COMPSS_HOME}" ]; then
  echo "COMPSS_HOME not defined"
  exit 1
fi

# Load auxiliar scripts
# shellcheck source=../system/commons/version.sh"
# shellcheck disable=SC1091
source "${COMPSS_HOME}Runtime/scripts/system/commons/version.sh"
# shellcheck source=../system/commons/logger.sh"
# shellcheck disable=SC1091
source "${COMPSS_HOME}Runtime/scripts/system/commons/logger.sh"
# shellcheck source=../system/commons/utils.sh"
# shellcheck disable=SC1091
source "${COMPSS_HOME}Runtime/scripts/system/commons/utils.sh"
# shellcheck source=./job_submission.sh"
# shellcheck disable=SC1091
source "${COMPSS_HOME}Runtime/scripts/queues/commons/job_submission.sh"
# shellcheck source=../system/runtime/compss_setup.sh"
# shellcheck disable=SC1091
source "${COMPSS_HOME}Runtime/scripts/system/runtime/compss_setup.sh"

#---------------------------------------------------
# SCRIPT CONSTANTS DECLARATION
#---------------------------------------------------
DEFAULT_JOB_NAME="COMPSS_GENTRACE"
DEFAULT_NVRAM_OPTIONS="none"
DEFAULT_CUSTOM_THREADS="true"
DEFAULT_TRACE_NAME="trace"
DEFAULT_LOG_LEVEL_ARGUMENT="${LOG_LEVEL_DEBUG}"
DEFAULT_KEEP_PACKAGES="false"

#---------------------------------------------------
# ERROR CONSTANTS DECLARATION
#---------------------------------------------------

#---------------------------------------------------
# GLOBAL VARIABLE NAME DECLARATION
#---------------------------------------------------

#---------------------------------------------------------------------------------------
# HELPER FUNCTIONS
#---------------------------------------------------------------------------------------
###############################################
# Clear files
###############################################
cleanup() {
  rm -rf "${submit_script}"
  rm -rf "${submit_script}"*
}


###############################################
# Show Options
###############################################
show_trace_submission_opts() {

  load_SC_config "${DEFAULT_SC_CFG}"

  # Show usage
  cat <<EOT
  Queue system configuration:
    --sc_cfg=<name>                         SuperComputer configuration file to use. Must exist inside queues/cfgs/
                                            Default: ${DEFAULT_SC_CFG}

  General submision arguments:

    --job_name=<name>                       Job name
                                            Default: ${DEFAULT_JOB_NAME}

EOT
  if [ -n "${ENABLE_PROJECT_NAME}" ] && [ "${ENABLE_PROJECT_NAME}" == "true" ]; then
    cat <<EOT
    --project_name=<name>                   Project name to pass to queue system.
                                            Default: Empty.

EOT
  fi
  cat <<EOT
    --queue=<name>                          Queue/partition name to submit the job. Depends on the queue system.
                                            Default: ${DEFAULT_QUEUE}

    --reservation=<name>                    Reservation to use when submitting the job.
                                            Default: ${DEFAULT_RESERVATION}
EOT
  if [ -z "${DISABLE_QARG_QOS}" ] || [ "${DISABLE_QARG_QOS}" == "false" ]; then
    cat <<EOT
    --qos=<qos>                             Quality of Service to pass to the queue system.
                                            Default: ${DEFAULT_QOS}

EOT
  fi
  cat <<EOT
    --exec_time=<minutes>                   Expected execution time of the application (in minutes)
                                            Default: ${DEFAULT_EXEC_TIME}

    --job_dependency=<jobID>                Postpone job execution until the job dependency has ended.
                                            Default: ${DEFAULT_DEPENDENCY_JOB}

EOT

  if [ -z "${DISABLE_QARG_CONSTRAINTS}" ] || [ "${DISABLE_QARG_CONSTRAINTS}" == "false" ]; then
    cat <<EOT
    --constraints=<constraints>		    Constraints to pass to queue system.
					    Default: ${DEFAULT_CONSTRAINTS}

EOT
  fi
  if [ -z "${DISABLE_QARG_LICENSES}" ] || [ "${DISABLE_QARG_LICENSES}" == "false" ]; then
    cat <<EOT
    --licenses=<licenses>		          Licenses to pass to queue system.
					    Default: ${DEFAULT_LICENSES}

EOT
  fi
  if [ "${ENABLE_QARG_CLUSTER}" == "true" ]; then
    cat <<EOT
    --cluster=<cluster>                     Cluster to pass to queue system.
                              		    Default: Empty.

EOT
   fi
  cat <<EOT
    --num_nodes=<int>                       Number of nodes to use
                                            Default: ${DEFAULT_NUM_NODES}

    --num_switches=<int>                    Maximum number of different switches. Select 0 for no restrictions.
                                            Maximum nodes per switch: ${MAX_NODES_SWITCH}
                                            Only available for at least ${MIN_NODES_REQ_SWITCH} nodes.
                                            Default: ${DEFAULT_NUM_SWITCHES}

    --cpus_per_node                         Number of cpus per node the queue system must allocate per job.
                                            Default: ${DEFAULT_CPUS_PER_NODE}

    --gpus_per_node                         Number of gpus per node the queue system must allocate per job.
                                            Default: ${DEFAULT_GPUS_PER_NODE}

EOT
  if [ -z "${DISABLE_QARG_NVRAM}" ] || [ "${DISABLE_QARG_NVRAM}" == "false" ]; then
    cat <<EOT
    --nvram_options="<string>"              NVRAM options (e.g. "1LM:2000" | "2LM:1000")
                                            Default: ${DEFAULT_NVRAM_OPTIONS}

EOT
  fi
  if [ -n "${ENABLE_FILE_SYSTEMS}" ] && [ "${ENABLE_FILE_SYSTEMS}" == "true" ]; then
    cat <<EOT

    --file_systems=<name>                   File systems name to pass to queue system.
                                            Default: Empty

EOT
  fi
  cat <<EOT
    --storage_home=<string>                 Root installation dir of the storage implementation.
                                            Can be defined with the ${STORAGE_HOME_ENV_VAR} environment variable.
                                            Default: ${DEFAULT_STORAGE_HOME}

    --storage_props=<string>                Absolute path of the storage properties file
                                            Mandatory if storage_home is defined

    --env_script=<path/to/script>           Script to source the required environment for the application.
                                            Default: Empty

    --extra_submit_flag=<flag>              Flag to pass queue system flags not supported by default command flags.
                                            Spaces must be added as '#'
                                            Default: Empty

  Debug options:
    --debug, -d                             Set the debug level to ${LOG_LEVEL_DEBUG}
                                            Default: ${DEFAULT_LOG_LEVEL}

    --log_dir=<path>                        Path where to leave the log information
                                            Default: same as output directory

    --log_level=<level>                     Set the debug level: ${LOG_LEVEL_OFF} | ${LOG_LEVEL_INFO} | ${LOG_LEVEL_API} | ${LOG_LEVEL_DEBUG} | ${LOG_LEVEL_TRACE}
                                            Default: ${DEFAULT_LOG_LEVEL}

  Trace Merging options:
    --keep_packages=<bool>                  If true, the script mantains the packages after merging; otherwise, input packages are deleted.
                                            Default: ${DEFAULT_KEEP_PACKAGES}

    --custom_threads=<bool>                 Disables the thread re-organization and labeling indicating what each thread is
                                            Default: ${DEFAULT_CUSTOM_THREADS}

    --out_dir=<path>                        Directory where to save the output trace
                                            Default: same as input directory

    --trace_name=<string>                   Name of the trace gathering all the events.
                                            Default: ${DEFAULT_TRACE_NAME}
EOT
}

###############################################
# Function to get the arguments
###############################################
parse_trace_submission_options() {
  local submission_opts=""
  gen_trace_opts=""
  local OPTIND
   while getopts d-: flag; do
    # Treat the argument
    case "$flag" in
      -)
        # Check more complex arguments
        case "${OPTARG}" in
          # Trae Generation options
          keep_packages=*)
            keep_packages=${OPTARG//keep_packages=/}
            gen_trace_opts="${gen_trace_opts} --${OPTARG}"
            ;;
          custom_threads=*)
            custom_threads=${OPTARG//custom_threads=/}
            gen_trace_opts="${gen_trace_opts} --${OPTARG}"
            ;;
          debug*)
            # Enable debug in log level
            log_level=${DEFAULT_LOG_LEVEL_ARGUMENT}
            gen_trace_opts="${gen_trace_opts} --${OPTARG}"
            ;;
          log_dir=*)
            # Tracing system
            specific_log_dir=${OPTARG//log_dir=/}
            gen_trace_opts="${gen_trace_opts} --${OPTARG}"
            ;;
          log_level=*)
            # Tracing system
            log_level=${OPTARG//log_level=/}
            gen_trace_opts="${gen_trace_opts} --${OPTARG}"
            ;;
          out_dir=*)
            # Register desired output directory
            out_dir=${OPTARG//out_dir=/}
            gen_trace_opts="${gen_trace_opts} --${OPTARG}"
            ;;
          trace_name=*)
            # Register desired trace name
            trace_name=${OPTARG//trace_name=/}
            gen_trace_opts="${gen_trace_opts} --${OPTARG}"
            ;;
          *)
            # Flag didn't match any pattern. Add to submission
            submission_opts="${submission_opts} --$OPTARG"
            ;;
        esac
        ;;
      d)
        log_level=${DEFAULT_LOG_LEVEL_ARGUMENT}
        gen_trace_opts="${gen_trace_opts} -${flag}"
        ;;
      *)
        # Flag didn't match any pattern. Add to submission.
        submission_opts="${submission_opts} -${flag}"
        ;;
    esac
  done
  # Shift COMPSs arguments
  shift $((OPTIND-1))
  parse_job_submission_options ${submission_opts}

  # Parse input dir
  in_dir=${1}
}


###############################################
# Function to check the arguments
###############################################
check_trace_submission_args() {
  check_job_submission_options

  if [ -z "${in_dir}" ]; then
    in_dir=$(pwd)
  fi
  in_dir=$(eval "readlink -f ${in_dir}")
}


###############################################
# Function to log the arguments
###############################################
log_trace_submission_opts() {
  # Display generic arguments
  log_submission_opts
  cat <<EOT
COMPSs Paraver trace generation
  Traces:
    Input folder: ${in_dir}
    Output folder: ${out_dir}
    Trace name: ${trace_name}

  Merging Options:
    Custom threads: ${custom_threads}
    Keep packages: ${keep_packages}

  Logging:
    Level: ${log_level}
    Folder: ${specific_log_dir}
EOT
}


create_trace_submit_script(){
  create_tmp_submit
  submit_script="${TMP_SUBMIT_SCRIPT}"
  echo "Temp submit script is: ${submit_script}"
  # Trap cleanup
  trap cleanup EXIT

  append_submission_headers_to_script "${submit_script}"
  cat >>${submit_script} << EOT
  ${COMPSS_HOME}Runtime/scripts/user/compss_gentrace${gen_trace_opts} ${in_dir}
EOT
}

#---------------------------------------------------------------------------------------
# MAIN FUNCTIONS
#---------------------------------------------------------------------------------------
submit_gen_trace(){
  create_trace_submit_script
  submit "${submit_script}"
}


LOADED_QUEUES_COMMONS_TRACE_GENERATION=1
