#!/bin/bash

if [ -n "${LOADED_SYSTEM_RUNTIME_STORAGE}" ]; then
  return 0
fi

# Checking up COMPSs_HOME
if [ -z "${COMPSS_HOME}" ]; then
  echo "COMPSS_HOME not defined"
  exit 1
fi

# Load auxiliar scripts

# shellcheck source=../system/commons/logger.sh
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
# Configuration file
DEFAULT_STORAGE_CONF="null"
# Task execution mode
DEFAULT_TASK_EXECUTION=compss

#----------------------------------------------
# ERROR MESSAGES
#----------------------------------------------
ERR_STORAGE_IMPL_NOT_FOUND="Storage implementation not found"

###############################################
###############################################
#        ADAPTORS HANDLING FUNCTIONS
###############################################
###############################################
#----------------------------------------------
# CHECK ADAPTORS-RELATED ENV VARIABLES
#----------------------------------------------
check_storage_env() {
  :
}


#----------------------------------------------
# CHECK ADAPTORS-RELATED SETUP values
#----------------------------------------------
check_storage_setup () {

  if [ -z "$storageConf" ]; then
    storageConf=${DEFAULT_STORAGE_CONF}
  fi

  if [ -z "${taskExecution}" ]; then
    display_info "Using default execution type: ${DEFAULT_TASK_EXECUTION}"
    taskExecution=${DEFAULT_TASK_EXECUTION}
  fi

  if [ -n "${storageImpl}" ]; then
    storage_jars=""
    if [ -d "${COMPSS_HOME}/Tools/storage/$storageImpl" ]; then
      # Deal with storage implementation
      for jarname in ${COMPSS_HOME}/Tools/storage/$storageImpl/*.jar; do
        if [ -z "${storage_jars}" ]; then
          storage_jars=${jarname}
        else
          storage_jars=${storage_jars}:${jarname}
        fi
      done
      export CLASSPATH=${storage_jars}:${CLASSPATH}
      export PYTHONPATH=${COMPSS_HOME}/Tools/storage/${storageImpl}/python:${PYTHONPATH}
    else
      fatal_error "${ERR_STORAGE_IMPL_NOT_FOUND}" 1 
    fi
  fi
}



#----------------------------------------------
# APPEND PROPERTIES TO FILE
#----------------------------------------------
append_storage_jvm_options_to_file() {
  local jvm_options_file=${1}
  cat >> "${jvm_options_file}" << EOT
-Dcompss.task.execution=${taskExecution}
-Dcompss.storage.conf=${storageConf}
EOT
}


#----------------------------------------------
# CLEAN ENV
#----------------------------------------------
clean_storage_env () {
  : 
}

LOADED_SYSTEM_RUNTIME_STORAGE=1