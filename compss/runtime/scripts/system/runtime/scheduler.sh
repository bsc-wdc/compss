#!/bin/bash

if [ -n "${LOADED_SYSTEM_RUNTIME_SCHEDULERS}" ]; then
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
# Available Schedulers

BASE_SCHEDULER="es.bsc.compss.components.impl.TaskScheduler"
SCHEDULERS_PACKAGE="es.bsc.compss.scheduler"

OS_SCHEDULERS="${SCHEDULERS_PACKAGE}.orderstrict"
OS_FIFO_SCHEDULER="${OS_SCHEDULERS}.fifo.FifoTS"

LA_SCHEDULERS="${SCHEDULERS_PACKAGE}.lookahead"
LA_MT_SCHEDULERS="${LA_SCHEDULERS}.mt"
LA_FIFO_SCHEDULER="${LA_SCHEDULERS}.fifo.FifoTS"
LA_LIFO_SCHEDULER="${LA_SCHEDULERS}.lifo.LifoTS"
LA_LOCALITY_SCHEDULER="${LA_SCHEDULERS}.locality.LocalityTS"

LA_SUCC_SCHEDULERS="${LA_SCHEDULERS}.successors"
LA_MT_SUCC_SCHEDULERS="${LA_MT_SCHEDULERS}.successors"
LA_SUCC_CONSTRAINTS_FIFO_SCHEDULER="${LA_SUCC_SCHEDULERS}.constraintsfifo.ConstraintsFifoTS"
LA_MT_SUCC_CONSTRAINTS_FIFO_SCHEDULER="${LA_MT_SUCC_SCHEDULERS}.constraintsfifo.ConstraintsFifoTS"
LA_SUCC_FIFO_SCHEDULER="${LA_SUCC_SCHEDULERS}.fifo.FifoTS"
LA_MT_SUCC_FIFO_SCHEDULER="${LA_MT_SUCC_SCHEDULERS}.fifo.FifoTS"
LA_SUCC_LIFO_SCHEDULER="${LA_SUCC_SCHEDULERS}.lifo.LifoTS"
LA_MT_SUCC_LIFO_SCHEDULER="${LA_MT_SUCC_SCHEDULERS}.lifo.LifoTS"
LA_SUCC_LOCALITY_SCHEDULER="${LA_SUCC_SCHEDULERS}.locality.LocalityTS"
LA_MT_SUCC_LOCALITY_SCHEDULER="${LA_MT_SUCC_SCHEDULERS}.locality.LocalityTS"
DEFAULT_SCHEDULER="${LA_LOCALITY_SCHEDULER}"

# Available Cloud Connector
DEFAULT_SSH_CONNECTOR="es.bsc.compss.connectors.DefaultSSHConnector"
DEFAULT_NO_SSH_CONNECTOR="es.bsc.compss.connectors.DefaultNoSSHConnector"

DEFAULT_CONNECTOR=${DEFAULT_SSH_CONNECTOR}

DEFAULT_EXTERNAL_ADAPTATION=false

#----------------------------------------------
# ERROR MESSAGES
#----------------------------------------------


###############################################
###############################################
#        SCHEDULER HANDLING FUNCTIONS
###############################################
###############################################
#----------------------------------------------
# CHECK SCHEDULER-RELATED ENV VARIABLES
#----------------------------------------------
check_scheduler_env() {
  # Configuration files
  if [ -z "$DEFAULT_PROJECT" ]; then
    DEFAULT_PROJECT=${COMPSS_HOME}/Runtime/configuration/xml/projects/default_project.xml
  fi

  if [ -z "$DEFAULT_RESOURCES" ]; then
    DEFAULT_RESOURCES=${COMPSS_HOME}/Runtime/configuration/xml/resources/default_resources.xml
  fi

}


#----------------------------------------------
# CHECK SCHEDULER-RELATED SETUP values
#----------------------------------------------
check_scheduler_setup () {
  if [ -z "$projFile" ]; then
    display_info "Using default location for project file: ${DEFAULT_PROJECT}"
    projFile=${DEFAULT_PROJECT}
  fi

  if [ -z "$resFile" ]; then
    display_info "Using default location for resources file: ${DEFAULT_RESOURCES}"
    resFile=${DEFAULT_RESOURCES}
  fi

  # Scheduler
  if [ -z "$scheduler" ]; then
    scheduler=${DEFAULT_SCHEDULER}
  fi

  # input_profile, output_profile and scheduler_config are variables potentially empty

  if [ -z "$conn" ]; then
    conn=${DEFAULT_CONNECTOR}
  fi
  
  if [ -z "$external_adaptation" ]; then
	  external_adaptation=$DEFAULT_EXTERNAL_ADAPTATION
  fi 
}



#----------------------------------------------
# APPEND PROPERTIES TO FILE
#----------------------------------------------
append_scheduler_jvm_options_to_file() {
  local jvm_options_file=${1}
  cat >> "${jvm_options_file}" << EOT
-Dcompss.scheduler=${scheduler}
-Dcompss.scheduler.config=${scheduler_config}
-Dcompss.profile.input=${input_profile}
-Dcompss.profile.output=${output_profile}
-Dcompss.project.file=${projFile}
-Dcompss.resources.file=${resFile}
-Dcompss.project.schema=${COMPSS_HOME}/Runtime/configuration/xml/projects/project_schema.xsd
-Dcompss.resources.schema=${COMPSS_HOME}/Runtime/configuration/xml/resources/resources_schema.xsd
-Dcompss.conn=${conn}
-Dcompss.external.adaptation=${external_adaptation}
EOT
}


#----------------------------------------------
# CLEAN ENV
#----------------------------------------------
clean_scheduler_env () {
  :
}

LOADED_SYSTEM_RUNTIME_SCHEDULERS=1
