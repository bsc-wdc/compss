source "${COMPSS_HOME}Runtime/scripts/system/commons/logger.sh"

###############################################
###############################################
#            CONSTANTS DEFINITION
###############################################
###############################################
#----------------------------------------------
# DEFAULT VALUES
#----------------------------------------------
#Tracing
TRACING_ARM_DDT=-3
TRACING_ARM_MAP=-2
TRACING_SCOREP=-1
TRACING_DEACTIVATED=0
TRACING_BASIC=1
TRACING_ADVANCED=2

DEFAULT_TRACING=${TRACING_DEACTIVATED}
DEFAULT_TRACE_LABEL="None"
DEFAULT_EXTRAE_CONFIG_FILE="null"


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
    EXTRAE_HOME=${COMPSS_HOME}/Dependencies/extrae
  fi

  if [ -z "${EXTRAE_LIB}" ]; then
    EXTRAE_LIB=${EXTRAE_HOME}/lib
  fi
}


#----------------------------------------------
# CHECK TRACING-RELATED SETUP values
#----------------------------------------------
check_tracing_setup () {
  if [ -z "${tracing}" ]; then
    tracing=${DEFAULT_TRACING}
  fi

  # TRACING file option
  if [ -z "${custom_extrae_config_file}" ]; then
    custom_extrae_config_file=${DEFAULT_EXTRAE_CONFIG_FILE}
  fi
  
  if [ -z "${trace_label}" ]; then
    trace_label=${DEFAULT_TRACE_LABEL}
  fi

  if [ ${tracing} -eq ${TRACING_BASIC} ]; then
    extraeFile=${COMPSS_HOME}/Runtime/configuration/xml/tracing/extrae_basic.xml
  elif [ ${tracing} -eq ${TRACING_ADVANCED} ]; then
    extraeFile=${COMPSS_HOME}/Runtime/configuration/xml/tracing/extrae_advanced.xml
  fi

  # Overwrite extraeFile if already defined
  if [ "${custom_extrae_config_file}" != "${DEFAULT_EXTRAE_CONFIG_FILE}" ]; then
    extraeFile=${custom_extrae_config_file}
  fi

  # Set tracing env
  if [ ${tracing} -gt ${TRACING_DEACTIVATED} ]; then
    export EXTRAE_HOME=${EXTRAE_HOME}
    export LD_LIBRARY_PATH=${EXTRAE_LIB}:${LD_LIBRARY_PATH}
    export EXTRAE_CONFIG_FILE=${extraeFile}
  fi
}

#----------------------------------------------
# GENERATE CONFIGURATION FILES
#----------------------------------------------
generate_tracing_config_files() {
  : # no need to do anything. Config files available on the COMPSs distribution.
}

#----------------------------------------------
# APPEND PROPERTIES TO FILE
#----------------------------------------------
append_tracing_jvm_options_to_file() {
  local jvm_options_file=${1}
  cat >> "${jvm_options_file}" << EOT
-Dcompss.tracing=${tracing}
-Dcompss.trace.label=${trace_label}
-Dcompss.extrae.file=${custom_extrae_config_file}
EOT
}

#----------------------------------------------
# STARTS TRACING ENGINE
#----------------------------------------------
start_tracing() {
  if [ ${tracing} -gt 0 ]; then
    export LD_PRELOAD=${EXTRAE_LIB}/libpttrace.so
  fi
}

#----------------------------------------------
# STOP TRACING ENGINE
#----------------------------------------------
stop_tracing() {
  if [ $tracing -gt 0 ]; then
    unset LD_PRELOAD
  fi
}
#----------------------------------------------
# CLEAN ENV
#----------------------------------------------
clean_tracing_env () {
  :
}