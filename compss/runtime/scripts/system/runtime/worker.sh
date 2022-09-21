#!/bin/bash

if [ -n "${LOADED_SYSTEM_RUNTIME_WORKER}" ]; then
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
# JVM options on the worker
DEFAULT_JVM_WORKERS="-Xms256m,-Xmx1024m,-Xmn100m"

# Default (CPU, GPU and FPGA) affinity mappings for the Executors
AFFINITY_DISABLED="disabled"
AFFINITY_AUTOMATIC="automatic"
DEFAULT_CPU_AFFINITY=${AFFINITY_AUTOMATIC} # disabled, automatic, user string
DEFAULT_GPU_AFFINITY=${AFFINITY_AUTOMATIC} # disabled, automatic, user string
DEFAULT_FPGA_AFFINITY=${AFFINITY_AUTOMATIC} # disabled, automatic, user string

DEFAULT_FPGA_REPROGRAM=""
DEFAULT_IO_EXECUTORS=0

#----------------------------------------------
# ERROR MESSAGES
#----------------------------------------------


###############################################
###############################################
#        WORKER HANDLING FUNCTIONS
###############################################
###############################################
#----------------------------------------------
# CHECK WORKER-RELATED ENV VARIABLES
#----------------------------------------------
check_worker_env() {
  : # No worker environments set on the worker
}


#----------------------------------------------
# CHECK WORKER-RELATED SETUP values
#----------------------------------------------
check_worker_setup () {

  if [ -z "${jvm_workers_opts}" ] || [ "${jvm_workers_opts}" = \"\" ]; then
    jvm_workers_opts=${DEFAULT_JVM_WORKERS}
  fi

  # WORKER THREAD AFFINITY
  if [ -z "${worker_cpu_affinity}" ]; then
    worker_cpu_affinity=${DEFAULT_CPU_AFFINITY}
  fi
  if [ -z "${worker_gpu_affinity}" ]; then
    worker_gpu_affinity=${DEFAULT_GPU_AFFINITY}
  fi
  if [ -z "${worker_fpga_affinity}" ]; then
    worker_fpga_affinity=${DEFAULT_FPGA_AFFINITY}
  fi

  if [ -z "${worker_io_executors}" ]; then
    worker_io_executors=${DEFAULT_IO_EXECUTORS}
  fi

  # Accelerators
  if [ -z "${fpga_prog}" ]; then
    fpga_prog=${DEFAULT_FPGA_REPROGRAM}
  fi

}



#----------------------------------------------
# APPEND PROPERTIES TO FILE
#----------------------------------------------
append_worker_jvm_options_to_file() {
  local jvm_options_file=${1}
  cat >> "${jvm_options_file}" << EOT
-Dcompss.worker.cp=${CLASSPATH}
-Dcompss.worker.appdir=${appdir}
-Dcompss.worker.jvm_opts=${jvm_workers_opts}
-Dcompss.worker.cpu_affinity=${worker_cpu_affinity}
-Dcompss.worker.gpu_affinity=${worker_gpu_affinity}
-Dcompss.worker.fpga_affinity=${worker_fpga_affinity}
-Dcompss.worker.fpga_reprogram=${fpga_prog}
-Dcompss.worker.io_executors=${worker_io_executors}
EOT
  if [ -n "${env_script_path}" ];then
    cat >> "${jvm_options_file}" << EOT
-Dcompss.worker.env_script=${env_script_path}
EOT
  fi
}


#----------------------------------------------
# CLEAN ENV
#----------------------------------------------
clean_worker_env () {
  :
}

LOADED_SYSTEM_RUNTIME_WORKER=1