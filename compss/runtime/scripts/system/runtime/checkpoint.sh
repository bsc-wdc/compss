#!/bin/bash

if [ -n "${LOADED_SYSTEM_RUNTIME_CHECKPOINT}" ]; then
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

# Available Checkpointers
CHECKPOINT_FINISHED_TASKS=es.bsc.compss.checkpoint.policies.CheckpointPolicyFinishedTasks
CHECKPOINT_PERIODIC_TIME=es.bsc.compss.checkpoint.policies.CheckpointPolicyPeriodicTime
CHECKPOINT_INSTANTIATED_GROUP=es.bsc.compss.checkpoint.policies.CheckpointPolicyInstantiatedGroup
NO_CHECKPOINT=es.bsc.compss.checkpoint.policies.NoCheckpoint

DEFAULT_CHECKPOINT=${NO_CHECKPOINT}

#----------------------------------------------
# ERROR MESSAGES
#----------------------------------------------
CHECKPOINT_FOLDER_ERROR="ERROR: Checkpoint folder path not provided"

###############################################
###############################################
#        CHECKPOINT HANDLING FUNCTIONS
###############################################
###############################################
#----------------------------------------------
# CHECK CHECKPOINT-RELATED ENV VARIABLES
#----------------------------------------------
check_checkpoint_env() {
  # Configuration files
  :
}


#----------------------------------------------
# CHECK CHECKPOINT-RELATED SETUP values
#----------------------------------------------
check_checkpoint_setup () {
  # Checkpointer
  if [ -z "${checkpoint}" ]; then
    checkpoint=${DEFAULT_CHECKPOINT}
  fi

  if [ "${checkpoint}" != "${NO_CHECKPOINT}" ]; then
    if [ -z "${checkpoint_folder}" ]; then
      fatal_error "${CHECKPOINT_FOLDER_ERROR}" 1
    fi
  fi

}




#----------------------------------------------
# APPEND PROPERTIES TO FILE
#----------------------------------------------
append_checkpoint_jvm_options_to_file() {
  local jvm_options_file=${1}
  cat >> "${jvm_options_file}" << EOT
-Dcompss.checkpoint.policy=${checkpoint}
-Dcompss.checkpoint.params=${checkpoint_params}
-Dcompss.checkpoint.folder=${checkpoint_folder}
EOT
}



#----------------------------------------------
# CLEAN ENV
#----------------------------------------------
clean_checkpoint_env () {
  :
}


LOADED_SYSTEM_RUNTIME_CHECKPOINT=1
