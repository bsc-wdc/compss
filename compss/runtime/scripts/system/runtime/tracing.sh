# shellcheck source=../commons/logger.sh
# shellcheck disable=SC1091
source "${COMPSS_HOME}/Runtime/scripts/system/commons/logger.sh"


###############################################
###############################################
#            CONSTANTS DEFINITION
###############################################
###############################################

#----------------------------------------------
# DEFAULT VALUES
#----------------------------------------------
#Tracing
TRACING_DEACTIVATED="false"
TRACING_ENABLED="true"


DEFAULT_TRACING="${TRACING_DEACTIVATED}"
DEFAULT_TRACE_LABEL="None"
DEFAULT_EXTRAE_CONFIG_FILE="null"
DEFAULT_EXTRAE_CONFIG_FILE_PYTHON="null"
DEFAULT_TRACING_TASK_DEPENDENCIES="false"
#----------------------------------------------
# ERROR MESSAGES
#----------------------------------------------


###############################################
###############################################
#        STREAM HANDLING FUNCTIONS
###############################################
###############################################

#----------------------------------------------
# CHECK TRACING-RELATED ENV VARIABLES
#----------------------------------------------
check_tracing_env() {
  # Extrae Environment
  if [ -z "${EXTRAE_HOME}" ]; then
    EXTRAE_HOME="${COMPSS_HOME}/Dependencies/extrae"
  fi

  if [ -z "${EXTRAE_LIB}" ]; then
    EXTRAE_LIB="${EXTRAE_HOME}/lib"
  fi
}

#----------------------------------------------
# CHECK TRACING-RELATED SETUP values
#----------------------------------------------
check_tracing_setup () {
  if [ -z "${tracing}" ]; then
    tracing="${DEFAULT_TRACING}"
  fi

  if [ -z "${tracing_task_dependencies}" ]; then
    tracing_task_dependencies="${DEFAULT_TRACING_TASK_DEPENDENCIES}"
  fi

  # TRACING file option
  if [ -z "${custom_extrae_config_file}" ]; then
    custom_extrae_config_file="${DEFAULT_EXTRAE_CONFIG_FILE}"
  fi

  if [ -z "${custom_extrae_config_file_python}" ]; then
    custom_extrae_config_file_python="${DEFAULT_EXTRAE_CONFIG_FILE_PYTHON}"
  fi

  if [ -z "${trace_label}" ]; then
    trace_label="${DEFAULT_TRACE_LABEL}"
  fi

  if [ "${tracing}" == "${TRACING_ENABLED}" ]; then
    extraeFile=${COMPSS_HOME}/Runtime/configuration/xml/tracing/extrae_basic.xml
  fi

  # Overwrite extraeFile if already defined
  if [ "${custom_extrae_config_file}" != "${DEFAULT_EXTRAE_CONFIG_FILE}" ]; then
    extraeFile="${custom_extrae_config_file}"
  fi

  extraeWDir="${specific_log_dir}/trace"

  # Set tracing env
  if [ "${tracing}" == "${TRACING_ENABLED}" ]; then
    export LD_LIBRARY_PATH=${EXTRAE_LIB}:${LD_LIBRARY_PATH}
    export EXTRAE_HOME=${EXTRAE_HOME}
    export EXTRAE_CONFIG_FILE=${extraeFile}
    export EXTRAE_USE_POSIX_CLOCK=0

    # Where the intermediate trace files will be stored during the execution of the application
    export EXTRAE_DIR=${extraeWDir}
    # Where the intermediate trace files will be stored once the execution has been finished.
    export EXTRAE_FINAL_DIR=${extraeWDir}
  fi
}

#----------------------------------------------
# GENERATE CONFIGURATION FILES
#----------------------------------------------
generate_tracing_config_files() {
  # No need to do anything. Config files available on the COMPSs distribution
  :
}

#----------------------------------------------
# APPEND PROPERTIES TO FILE
#----------------------------------------------
append_tracing_jvm_options_to_file() {
  local jvm_options_file=${1}
  cat >> "${jvm_options_file}" << EOT
-Dcompss.tracing=${tracing}
-Dcompss.extrae.working_dir=${extraeWDir}
-Dcompss.tracing.task.dependencies=${tracing_task_dependencies}
-Dcompss.trace.label=${trace_label}
-Dcompss.extrae.file=${custom_extrae_config_file}
-Dcompss.extrae.file.python=${custom_extrae_config_file_python}
EOT
}

#----------------------------------------------
# STARTS TRACING ENGINE
#----------------------------------------------
start_tracing() {
  if [ "${tracing}" == "${TRACING_ENABLED}" ]; then   
    export LD_PRELOAD=${EXTRAE_LIB}/libpttrace.so
    if [ "${lang}" == "python" ]; then
      export PYTHONPATH=${EXTRAE_HOME}/libexec/:${EXTRAE_HOME}/lib/:${PYTHONPATH}
    fi
  fi
}

#----------------------------------------------
# STOP TRACING ENGINE
#----------------------------------------------
stop_tracing() {
  if [ "${tracing}" == "${TRACING_ENABLED}" ]; then
    unset LD_PRELOAD
  fi
}
#----------------------------------------------
# CLEAN ENV
#----------------------------------------------
clean_tracing_env () {
  # No need to do anything
  :
}
