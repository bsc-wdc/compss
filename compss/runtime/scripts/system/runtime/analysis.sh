#!/bin/bash

if [ -n "${LOADED_SYSTEM_RUNTIME_ANALYSIS}" ]; then
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
# Available Logger levels
LOG_LEVEL_TRACE="trace"
LOG_LEVEL_DEBUG="debug"
LOG_LEVEL_INFO="info"
LOG_LEVEL_API="api"
LOG_LEVEL_OFF="off"

DEFAULT_LOG_LEVEL="${LOG_LEVEL_OFF}"

# Enable COMPSs execution summary at the end of the execution
DEFAULT_SUMMARY="false"

# Graph Generation
DEFAULT_GRAPH="false"

# Monitoring
DEFAULT_MONITORING_INTERVAL="0"

# External debugger options
DEFAULT_DEBUGGER="false"
DEFAULT_DEBUGGER_PORT="9999"

#----------------------------------------------
# ERROR MESSAGES
#----------------------------------------------


###############################################
###############################################
#      APP ANALYSIS HANDLING FUNCTIONS
###############################################
###############################################
#----------------------------------------------
# CHECK APP ANALISYS-RELATED ENV VARIABLES
#----------------------------------------------
check_analysis_env() {
  :
}

#----------------------------------------------
# Creating Log directory
#----------------------------------------------
create_log_folder() {
  # specific_log_dir can be empty. If so, specified uses ${base_log_dir}/<application_name>_<overload offset>
  # base_log_dir can be empty. If so, placing it in user's home folder .COMPSs
  if [ -n  "${specific_log_dir}" ]; then
    mkdir -p "${specific_log_dir}"
  else
    local final_log_dir
    if [ -n  "${base_log_dir}" ]; then
      final_log_dir="${base_log_dir}"
    else
      final_log_dir="${HOME}/.COMPSs"
    fi
    base_log_dir="${final_log_dir}"
    mkdir -p "${final_log_dir}"

    if [ ! "${final_log_dir: -1}" == "/" ]; then
      final_log_dir="${final_log_dir}/"
    fi

    local folder_creation_exit_code
    folder_creation_exit_code="-1"
    while [ ! "${folder_creation_exit_code}" == "0" ]; do
      local app_folder_name
      local oldest_date=""
      local override="true"

      for overload_id in  $(seq 1 99); do
        local overload_tag
        if [ "${overload_id}" -gt "9" ]; then
          overload_tag="_${overload_id}"
        else
          overload_tag="_0${overload_id}"
        fi
        overload_tag="${appName}${overload_tag}"
        overload_log_dir="${final_log_dir}${overload_tag}"


        if [ -d "${overload_log_dir}" ]; then
          if [[ "$OSTYPE" == "darwin"* ]]; then
            overload_date=$(stat -f %Fm "${overload_log_dir}" )
          else  
            overload_date=$(stat -c %.9Y "${overload_log_dir}" )
          fi
          overload_date="${overload_date//./}"
          overload_date="${overload_date//,/}"
          if [ -z "${oldest_date}" ]; then
            app_folder_name=${overload_tag}
            oldest_date=${overload_date}
          else
            if [ "${oldest_date}" -gt "${overload_date}" ]; then
              app_folder_name=${overload_tag}
              oldest_date=${overload_date}
            fi
          fi
        else
          app_folder_name=${overload_tag}
          override="false"
          break
        fi
      done

      if [ "${override}" == "true" ]; then
        display_warning "${WARN_LOG_OVERRIDE} ${final_log_dir}"
        rm -rf "${final_log_dir}${app_folder_name}"
      fi

      specific_log_dir="${final_log_dir}${app_folder_name}"
      mkdir "${specific_log_dir}"
      folder_creation_exit_code="${?}"
    done
  fi
}

#----------------------------------------------
# CHECK ANALYSIS-RELATED SETUP values
#----------------------------------------------
check_analysis_setup () {
 
  if [ -z "${log_level}" ]; then
    log_level="${DEFAULT_LOG_LEVEL}"
  fi

  if [ -z "${summary}" ]; then
    summary="${DEFAULT_SUMMARY}"
  fi

  if [ -z "${graph}" ]; then
    graph="${DEFAULT_GRAPH}"
  fi

  create_log_folder

  if [ -z "$monitoring" ]; then
    monitoring="${DEFAULT_MONITORING_INTERVAL}"
  else
    # If monitor has been activated trigger final graph generation and log_level = at least info
    graph="${DEFAULT_GRAPH_ARGUMENT}"
    if [ "${log_level}" == "${DEFAULT_LOG_LEVEL}" ] || [ "${log_level}" == "${LOG_LEVEL_OFF}" ]; then
       log_level="${LOG_LEVEL_INFO}"
    fi
  fi

    # Master log level
  if [ "${log_level}" == "${DEFAULT_LOG_LEVEL}" ]; then
    itlog4j_file="COMPSsMaster-log4j"
  else
    itlog4j_file="COMPSsMaster-log4j.${log_level}"
  fi

  if [ "${log_level}" == "${LOG_LEVEL_DEBUG}" ] || [ "${log_level}" == "${LOG_LEVEL_TRACE}" ]; then
    export COMPSS_BINDINGS_DEBUG=1
  fi

  # External debugger
  if [ -z "$external_debugger" ]; then
    external_debugger="${DEFAULT_DEBUGGER}"
  fi

  if [ "${external_debugger}" == "true" ]; then
    if [ -z "${external_debugger_port}" ]; then
      external_debugger_port="${DEFAULT_DEBUGGER_PORT}"
    fi
  fi

  # jmx_port might be empty
}



#----------------------------------------------
# APPEND PROPERTIES TO FILE
#----------------------------------------------
append_analysis_jvm_options_to_file() {
  local jvm_options_file=${1}
    cat >> "${jvm_options_file}" << EOT
-Dcompss.baseLogDir=${base_log_dir}
-Dcompss.specificLogDir=${specific_log_dir}
-Dcompss.appLogDir=/tmp/${uuid}/
-Dlog4j.configurationFile=${COMPSS_HOME}/Runtime/configuration/log/${itlog4j_file}
-Dcompss.graph=${graph}
-Dcompss.monitor=${monitoring}
-Dcompss.summary=${summary}
EOT

  if [ "${external_debugger}" == "true" ]; then
    cat >> "${jvm_options_file}" << EOT
-Xdebug
-agentlib:jdwp=transport=dt_socket,address=${external_debugger_port},server=y,suspend=y
EOT
  fi

  if [ -n "${jmx_port}" ]; then
    cat >> "${jvm_options_file}" << EOT
-Dcom.sun.management.jmxremote
-Dcom.sun.management.jmxremote.port=${jmx_port}
-Dcom.sun.management.jmxremote.authenticate=false
-Dcom.sun.management.jmxremote.ssl=false
EOT
    fi

# Uncomment block in case that you want to debug errors in JNI
##  if [ "${log_level}" == "debug" ]; then
##        cat >> "${jvm_options_file}" << EOT
##-Xcheck:jni
##-verbose:jni
##EOT
##  fi
}


#----------------------------------------------
# CLEAN ENV
#----------------------------------------------
clean_analysis_env () {
  : 
}

LOADED_SYSTEM_RUNTIME_ANALYSIS=1
