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
MPI_CHECKPOINT_DIR_ERROR="ERROR: --ckptMPI_dir must be provided when --checkpointMPI is enabled"

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

  # MPI task checkpointing (MANA or SCR)
  if [ "${mpi_checkpoint_enabled}" = "true" ]; then
    if [ -z "${mpi_checkpoint_dir}" ]; then
      fatal_error "${MPI_CHECKPOINT_DIR_ERROR}" 1
    fi
    # JVM flags read by MPIInvoker in the workers
    _mpi_ckpt_flags="-Dcompss.mpi.checkpoint.enabled=true,-Dcompss.mpi.checkpoint.dir=${mpi_checkpoint_dir}"
    if [ -n "${mpi_checkpoint_interval}" ]; then
      _mpi_ckpt_flags="${_mpi_ckpt_flags},-Dcompss.mpi.checkpoint.interval=${mpi_checkpoint_interval}"
    fi
    if [ -z "${jvm_workers_opts}" ] || [ "${jvm_workers_opts}" = '""' ]; then
      jvm_workers_opts="${_mpi_ckpt_flags}"
    else
      jvm_workers_opts="${jvm_workers_opts},${_mpi_ckpt_flags}"
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
# REMOVE MPI CHECKPOINTS AFTER A SUCCESSFUL RUN
#----------------------------------------------
# Called with the application's exit code. On success, and without --ckptMPI_keep, removes the MANA
# checkpoints (mana_ckpt_*) of --ckptMPI_dir. SCR checkpoints are kept.
clean_mpi_checkpoints () {
  local end_code=${1}
  if [ "${end_code}" != "0" ] || [ "${mpi_checkpoint_enabled}" != "true" ] || [ "${mpi_checkpoint_keep}" = "true" ]; then
    return 0
  fi
  if [ -z "${mpi_checkpoint_dir}" ] || [ ! -d "${mpi_checkpoint_dir}" ] || [ "${mpi_checkpoint_dir}" = "/" ]; then
    return 0
  fi
  local ckpts
  ckpts=$(find "${mpi_checkpoint_dir}" -mindepth 1 -maxdepth 1 -type d -name 'mana_ckpt_*' 2>/dev/null)
  if [ -z "${ckpts}" ]; then
    return 0
  fi
  display_info "[MPI CHECKPOINT] Application finished successfully: removing MANA checkpoints in ${mpi_checkpoint_dir} (use --ckptMPI_keep to keep them)"
  find "${mpi_checkpoint_dir}" -mindepth 1 -maxdepth 1 -type d -name 'mana_ckpt_*' -exec rm -rf {} + 2>/dev/null \
    || display_warning "[MPI CHECKPOINT] Could not remove some checkpoints in ${mpi_checkpoint_dir}"
}

#----------------------------------------------
# CLEAN ENV
#----------------------------------------------
clean_checkpoint_env () {
  :
}


LOADED_SYSTEM_RUNTIME_CHECKPOINT=1
