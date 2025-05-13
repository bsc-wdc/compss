#!/bin/bash

if [ -n "${LOADED_SYSTEM_RUNTIME_TRACING}" ]; then
  return 0
fi

# Checking up COMPSs_HOME
if [ -z "${COMPSS_HOME}" ]; then
  echo "COMPSS_HOME not defined"
  exit 1
fi

# Load auxiliar scripts

# shellcheck source=../commons/logger.sh
# shellcheck disable=SC1091
source "${COMPSS_HOME}/Runtime/scripts/system/commons/logger.sh"

# shellcheck source=../trace/generatePRVs.sh
# shellcheck disable=SC1091
source "${COMPSS_HOME}/Runtime/scripts/system/trace/generatePRVs.sh"


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
DEFAULT_EXTRAE_CONFIG_FILE="${COMPSS_HOME}/Runtime/configuration/xml/tracing/extrae_basic.xml"
DEFAULT_EXTRAE_CONFIG_FILE_PYTHON="null"
DEFAULT_TRACING_TASK_DEPENDENCIES="false"
if [ -z "${DEFAULT_GENERATE_TRACE}" ]; then
  DEFAULT_GENERATE_TRACE="true"
fi
if [ -z "${DEFAULT_TRACING_DELETE_PACKAGES}" ]; then
  DEFAULT_TRACING_DELETE_PACKAGES="true"
fi
DEFAULT_CUSTOM_THREAD_ORDER="true"
#----------------------------------------------
# ERROR MESSAGES
#----------------------------------------------


###############################################
###############################################
#        TRACING HANDLING FUNCTIONS
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

  if [ -z "${tracing_custom_threads}" ]; then
    tracing_custom_threads="${DEFAULT_CUSTOM_THREAD_ORDER}"
  fi

  if [ -z "${tracing_generate_trace}" ]; then
    tracing_generate_trace="${DEFAULT_GENERATE_TRACE}"
  fi

  if [ -z "${tracing_delete_packages}" ]; then
    tracing_delete_packages="${DEFAULT_TRACING_DELETE_PACKAGES}"
  fi

  # Determine extrae directories
  extraeFile="${DEFAULT_EXTRAE_CONFIG_FILE}"
  if [ "${tracing}" == "${TRACING_ENABLED}" ]; then
    if [ -z "${custom_extrae_config_file}" ]; then
      extraeFile="${custom_extrae_config_file}"
    fi
    extrae_xml_final_path="${exec_dir}/cfgfiles/extrae.xml"
    mkdir -p "${exec_dir}/cfgfiles"
    cp "${extraeFile}" "${extrae_xml_final_path}"
    sed -i "s+{{EXTRAE_HOME}}+${EXTRAE_HOME}+g" "${extrae_xml_final_path}"
    sed -i "s+{{TRACE_OUTPUT_DIR}}+${exec_dir}/trace+g" "${extrae_xml_final_path}"
    extraeFile="${extrae_xml_final_path}"
    extraeWDir=$(grep "final-directory" "${extraeFile}" | cut -d'>' -f2 | rev| cut -c18- |rev)
  else
    extraeFile="null"
    extraeWDir="null"
  fi

  # Set tracing env
  if [ "${tracing}" == "${TRACING_ENABLED}" ]; then
    export LD_LIBRARY_PATH=${EXTRAE_LIB}:${LD_LIBRARY_PATH}
    export EXTRAE_HOME=${EXTRAE_HOME}
    export EXTRAE_CONFIG_FILE=${extraeFile}
    export EXTRAE_USE_POSIX_CLOCK=0
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
-Dcompss.tracing.task.dependencies=${tracing_task_dependencies}
-Dcompss.extrae.working_dir=${extraeWDir}
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
    export PYTHONPATH=${EXTRAE_HOME}/libexec/:${EXTRAE_HOME}/lib/:${PYTHONPATH}
  else
    if [ "${lang}" == "r" ]; then
      export LD_LIBRARY_PATH=${COMPSS_HOME}/Bindings/RCOMPSs/dummy_extrae/:${LD_LIBRARY_PATH}
    fi
  fi
}

#----------------------------------------------
# STOP TRACING ENGINE
#----------------------------------------------
stop_tracing() {
  if [ "${tracing}" == "${TRACING_ENABLED}" ]; then
    unset LD_PRELOAD

    if [ "${tracing_generate_trace}" == "true" ]; then
      # Generate all traces from packages
      trace_name="${appName}"
      if [ ! "${trace_label}" == "None" ]; then
        trace_name="${trace_name}_${trace_label}"
      fi
      trace_name="${trace_name}_compss"
      echo "Creating trace..."

      log_level="${LOG_LEVEL_DEBUG}"
      if [ ! "${log_level}" == "${LOG_LEVEL_OFF}" ]; then
        out_redirect="${specific_log_dir}/traceMerger.log"
        err_redirect="${specific_log_dir}/traceMerger.log"
      else
        out_redirect="/dev/null"
        err_redirect="/dev/null"
      fi
      out_redirect="${specific_log_dir}/traceMerger.log"
      err_redirect="${specific_log_dir}/traceMerger.log"
      generate_trace 1>>${out_redirect} 2>>${err_redirect}
      echo "Trace generation completed"
    fi

    if [ "${tracing_delete_packages}" == "true" ]; then
        if [ "${tracing_generate_trace}" == "false" ]; then
          echo "Dismissing tracing package removal. Traces were requested but not generated."
        else
          rm -rf ${packages}
        fi
    fi
  fi
}

generate_trace() {
  if [ "${log_level}" == "${LOG_LEVEL_OFF}" ]; then
    gen_tracing_log_level="${GEN_TRACING_LOG_LEVEL_OFF}"
  else
    gen_tracing_log_level="${GEN_TRACING_LOG_LEVEL_DEBUG}"
  fi
  gen_tracing_log_dir="${specific_log_dir}"

  echo "Creating prvs "
  packages=$(find "${extraeWDir}" -name "*.tar.gz")
  echo "found trace packages to merge: ${packages}"
  gen_traces "${extraeWDir}" "${trace_name}" "1" ${packages}
  if [ ! "${endCode}" -eq "0" ]; then
    exit "${endCode}"
  fi

  echo "Joining python traces"
  python_traces=$(find "${extraeWDir}/python" -name "*.prv")
  merge_python_traces "${extraeWDir}" "${trace_name}" ${python_traces}
  if [ ! "${endCode}" -eq "0" ]; then
    exit "${endCode}"
  fi
  rm -rf "${extraeWDir}/python"

  echo "Joining R traces"
  R_traces=$(find "${extraeWDir}/R" -name "*.prv")
  merge_R_traces "${extraeWDir}" "${trace_name}" ${R_traces}
  if [ ! "${endCode}" -eq "0" ]; then
    exit "${endCode}"
  fi
  rm -rf "${extraeWDir}/R"

  if [ "${tracing_custom_threads}" == "true" ]; then
    echo "Customizing threads"
    rearrange_trace_threads "${extraeWDir}" "${trace_name}"
    if [ ! "${endCode}" -eq "0" ]; then
      exit "${endCode}"
    fi
  fi
}

#----------------------------------------------
# CLEAN ENV
#----------------------------------------------
clean_tracing_env () {
  # No need to do anything
  :
}

LOADED_SYSTEM_RUNTIME_TRACING=1
