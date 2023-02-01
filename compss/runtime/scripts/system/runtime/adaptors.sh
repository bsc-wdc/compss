#!/bin/bash

if [ -n "${LOADED_SYSTEM_RUNTIME_ADAPTORS}" ]; then
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

# Master contact details
DEFAULT_MASTER_PORT="[43000,44000]"
DEFAULT_MASTER_NAME=""

# Available Adaptors
NIO_ADAPTOR=es.bsc.compss.nio.master.NIOAdaptor
GAT_ADAPTOR=es.bsc.compss.gat.master.GATAdaptor
REST_AGENT_ADAPTOR=es.bsc.compss.agent.rest.master.Adaptor
COMM_AGENT_ADAPTOR=es.bsc.compss.agent.comm.CommAgentAdaptor

DEFAULT_COMMUNICATION_ADAPTOR=${NIO_ADAPTOR}
#DEFAULT_COMMUNICATION_ADAPTOR=${GAT_ADAPTOR}

DEFAULT_REUSE_RESOURCES_ON_BLOCK="true"
DEFAULT_ENABLED_NESTED_TASKS_DETECTION="false"

#----------------------------------------------
# ERROR MESSAGES
#----------------------------------------------


###############################################
###############################################
#        ADAPTORS HANDLING FUNCTIONS
###############################################
###############################################
#----------------------------------------------
# CHECK ADAPTORS-RELATED ENV VARIABLES
#----------------------------------------------
check_adaptors_env() {
  # GAT Environment
  if [ -z "${GAT_LOCATION}" ]; then
    GAT_LOCATION=${COMPSS_HOME}/Dependencies/JAVA_GAT
  fi
}


#----------------------------------------------
# CHECK ADAPTORS-RELATED SETUP values
#----------------------------------------------
check_adaptors_setup () {
  # MASTER
  if [ -z "${master_name}" ]; then
    master_name=${DEFAULT_MASTER_NAME}
  fi

  # master_port might be null. In that case, the master tries different ports host the master

  # Adaptor
  if [ -z "$comm" ]; then
    comm=${DEFAULT_COMMUNICATION_ADAPTOR}
  fi
  
  # Should resources be released while a task execution stalls
  if [ -z "${reuse_resources_on_block}" ]; then
    reuse_resources_on_block=${DEFAULT_REUSE_RESOURCES_ON_BLOCK}
  fi

  # Should nested task detection be enabled while executing on local resources
  if [ -z "${enabled_nested_local}" ]; then
    enabled_nested_local=${DEFAULT_ENABLED_NESTED_TASKS_DETECTION}
  fi
}



#----------------------------------------------
# APPEND PROPERTIES TO FILE
#----------------------------------------------
append_adaptors_jvm_options_to_file() {
  local jvm_options_file=${1}
  cat >> "${jvm_options_file}" << EOT
-Dcompss.comm=${comm}
-Dcompss.masterName=${master_name}
-Dcompss.masterPort=${master_port}
-Dgat.adaptor.path=${GAT_LOCATION}/lib/adaptors
-Dgat.debug=false
-Dgat.broker.adaptor=sshtrilead
-Dgat.file.adaptor=sshtrilead
-Dcompss.execution.reuseOnBlock=${reuse_resources_on_block}
-Dcompss.execution.nested.enabled=${enabled_nested_local}
EOT
}


#----------------------------------------------
# CLEAN ENV
#----------------------------------------------
clean_adaptors_env () {
  : 
}

LOADED_SYSTEM_RUNTIME_ADAPTORS=1