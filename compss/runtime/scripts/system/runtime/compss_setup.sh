source "${COMPSS_HOME}Runtime/scripts/system/commons/logger.sh"

source "${COMPSS_HOME}Runtime/scripts/system/runtime/analysis.sh"
source "${COMPSS_HOME}Runtime/scripts/system/runtime/worker.sh"
source "${COMPSS_HOME}Runtime/scripts/system/runtime/adaptors.sh"
source "${COMPSS_HOME}Runtime/scripts/system/runtime/scheduler.sh"
source "${COMPSS_HOME}Runtime/scripts/system/runtime/bindings.sh"

source "${COMPSS_HOME}Runtime/scripts/system/runtime/streams.sh"
source "${COMPSS_HOME}Runtime/scripts/system/runtime/tracing.sh"
source "${COMPSS_HOME}Runtime/scripts/system/runtime/storage.sh"

###############################################
###############################################
#            CONSTANTS DEFINITION
###############################################
###############################################
# Paths where to find the application and the libraries it depends on
DEFAULT_CLASSPATH=$(pwd)
DEFAULT_PYTHONPATH=$(pwd)
DEFAULT_LIBRARY_PATH=$(pwd)
DEFAULT_APPDIR=$(pwd)
if [ -n "${PBS_O_WORKDIR}" ]; then
  DEFAULT_CLASSPATH=${PBS_O_WORKDIR}
  DEFAULT_PYTHONPATH=${PBS_O_WORKDIR}
  DEFAULT_LIBRARY_PATH=${PBS_O_WORKDIR}
  DEFAULT_APPDIR=${PBS_O_WORKDIR}
fi

#----------------------------------------------
# RUNTIME INTERNAL COMPONENTS
#----------------------------------------------
# Agent implementation
AGENT_IMPLEMENTATION=es.bsc.compss.agent.Agent

# Default path containing the configuration to start an agent
DEFAULT_AGENT_CONFIG="${COMPSS_HOME}Runtime/configuration/agents/all.json"

# Master's JVM options
DEFAULT_JVM_MASTER=""

# Loader implementation
RUNTIME_LOADER=es.bsc.compss.loader.ITAppLoader

#----------------------------------------------
# ERROR MESSAGES
#----------------------------------------------
AGENT_ERROR="Error running the agent"
JAVA_HOME_ERROR="ERROR: Cannot find Java JRE installation. Please set JAVA_HOME."
RUNTIME_ERROR="Error running application"
TMP_FILE_JVM_ERROR="ERROR: Can't create temporary file for JVM options."

###############################################
###############################################
#        COMPSs HANDLING FUNCTIONS
###############################################
###############################################
#----------------------------------------------
# CHECK COMPSs-RELATED ENV VARIABLES
#----------------------------------------------
check_compss_env() {
  # JAVA HOME
  if [[ -z "$JAVA_HOME" ]]; then
    JAVA=java
  elif [ -f "$JAVA_HOME"/jre/bin/java ]; then
    JAVA=$JAVA_HOME/jre/bin/java
  elif [ -f "$JAVA_HOME"/bin/java ]; then
    JAVA=$JAVA_HOME/bin/java
  else
    fatal_error "${JAVA_HOME_ERROR}" 1
  fi

  # Added for SGE queue systems which do not allow to copy LD_LIBRARY_PATH
  if [ -z "$LD_LIBRARY_PATH" ]; then
     # shellcheck disable=SC2153
     if [ -n "$LIBRARY_PATH" ]; then
         export LD_LIBRARY_PATH=$LIBRARY_PATH
         display_info "LD_LIBRARY_PATH not defined set to LIBRARY_PATH"
     fi
  fi

  check_analysis_env

  check_scheduler_env
  check_adaptors_env
  check_worker_env

  check_bindings_env
  # AutoParallel environment
  if [ -z "${PLUTO_HOME}" ]; then
    PLUTO_HOME=${COMPSS_HOME}/Dependencies/pluto/
    export PLUTO_HOME=${PLUTO_HOME}
    export PATH=${PLUTO_HOME}/bin:${PATH}
  fi  
  check_stream_env
  check_storage_env

  check_tracing_env

  #gen_core and appName can be empty
}

#----------------------------------------------
# CHECK COMPSS-RELATED SETUP values
#----------------------------------------------
check_compss_setup () {
  if [ -z "${uuid}" ]; then
    get_uuid
  fi

  # JVM
  if [ -z "${jvm_master_opts}" ] || [ "${jvm_master_opts}" = \"\" ]; then
    jvm_master_opts=${DEFAULT_JVM_MASTER}
  fi
  # Change jvm master opts separation character "," by " "
  jvm_master_opts=$(echo $jvm_master_opts | tr "," "\\n")


  # Classpath
  if [ -z "$cp" ]; then
    cp=${DEFAULT_CLASSPATH}
    for jar in "${DEFAULT_CLASSPATH}"/*.jar; do
       cp=$cp:$jar
    done
  else
    fcp=""
    convert_pathlist_to_absolute_paths "${cp}"
    cp="${fcp}"
    display_info "Relative Classpath resolved: $cp"
  fi
  export CLASSPATH=${cp}:${CLASSPATH}

  # Library Path
  if [ -z "$library_path" ]; then
    library_path=${DEFAULT_LIBRARY_PATH}
  fi
  if [ -z "${LD_LIBRARY_PATH}" ]; then
    export LD_LIBRARY_PATH=${library_path}
  else
    export LD_LIBRARY_PATH=${library_path}:${LD_LIBRARY_PATH}
  fi
  
  # Python Path
  if [ -z "$pythonpath" ]; then
    pythonpath=${DEFAULT_PYTHONPATH}
  else
    # Adds execution dir by default to pythonpath
    pythonpath=$pythonpath":${DEFAULT_PYTHONPATH}"
  fi
  if [ -z "${PYTHONPATH}" ]; then
    export PYTHONPATH=${pythonpath}
  else
    export PYTHONPATH=${pythonpath}:${PYTHONPATH}
  fi

  if [ -z "${agent_config}" ]; then
    agent_config=${DEFAULT_AGENT_CONFIG}
  fi
  
  check_analysis_setup

  check_worker_setup
  check_adaptors_setup
  check_scheduler_setup

  check_bindings_setup

  check_stream_setup
  check_storage_setup

  check_tracing_setup

}

#----------------------------------------------
# Write down current configuration to file
#----------------------------------------------
generate_jvm_opts_file() {
  jvm_options_file=$(mktemp -p /tmp) || arguments_error "${TMP_FILE_JVM_ERROR}"
  export JVM_OPTIONS_FILE=${jvm_options_file}

  # PLEASE: Any new parameter added here may be also added into interactive.py config dict.

  cat >> "${jvm_options_file}" << EOT
${jvm_master_opts}
-XX:+PerfDisableSharedMem
-XX:-UsePerfData
-XX:+UseG1GC
-XX:+UseThreadPriorities
-XX:ThreadPriorityPolicy=42
-Dcompss.to.file=false
-Dcompss.appName=${appName}
-Dcompss.uuid=${uuid}
EOT
  append_analysis_jvm_options_to_file "${jvm_options_file}"
  append_worker_jvm_options_to_file "${jvm_options_file}"
  append_adaptors_jvm_options_to_file "${jvm_options_file}"
  append_scheduler_jvm_options_to_file "${jvm_options_file}"

  append_bindings_jvm_options_to_file "${jvm_options_file}"

  append_stream_jvm_options_to_file "${jvm_options_file}"
  append_storage_jvm_options_to_file "${jvm_options_file}"

  append_tracing_jvm_options_to_file "${jvm_options_file}"

}

#----------------------------------------------
# Prepares all the necessary configuration for the runtime
#----------------------------------------------
prepare_runtime_environment() {
  # Create tmp dir for initial loggers configuration
  mkdir -p /tmp/"$uuid"

  # Create JVM Options file
  generate_jvm_opts_file

  # Start streaming backend if required
  start_stream_backends

  if [ -n "${gen_core}" ]; then
    echo "[RUNCOMPSS] Setting coredump generation." 
    ulimit -c unlimited
  fi
}

#----------------------------------------------
# APPEND PROPERTIES TO FILE - Specific for application
#----------------------------------------------
append_app_jvm_options_to_file() {
  # Add Application-specific options
  cat >> "${jvm_options_file}" << EOT
-Dcompss.appName=${appName}
EOT
}

#----------------------------------------------
# APPEND PROPERTIES TO FILE - Specific for the agent
#----------------------------------------------
append_agent_jvm_options_to_file() {
  # Add Application-specific options
  cat >> "${jvm_options_file}" << EOT
-Dcompss.agent.configpath=${agent_config}
EOT
}

#----------------------------------------------
# Cleans up all the runtime environment
#----------------------------------------------
clean_runtime_environment() {
  # Stop streaming backend if necessary
  clean_stream_env

  # Delete JVM options file
  rm -f "${jvm_options_file}"

  # Remove folder with initial loggers
  rm -rf /tmp/"$uuid"
}



#----------------------------------------------
# MAIN FUNCTION TO START AN APPLICATION
#----------------------------------------------
start_compss_agent() {
  # Prepare COMPSs Runtime + Bindings environment
  prepare_runtime_environment
  
  append_agent_jvm_options_to_file "${jvm_options_file}"

  # Define command
  local java_opts
  local JAVACMD
  java_opts=$(tr "\\n" " " < "${jvm_options_file}")
  JAVACMD=$JAVA" -noverify -classpath ${CLASSPATH}:${COMPSS_HOME}/Runtime/compss-engine.jar:${COMPSS_HOME}/Runtime/compss-agent-impl.jar ${java_opts}"
  # Launch application
  start_tracing
  # shellcheck disable=SC2086
  $JAVACMD "${AGENT_IMPLEMENTATION}"
  endCode=$?
  stop_tracing
  if [ $endCode -ne 0 ]; then
    fatal_error "${AGENT_ERROR}" ${endCode}
  fi
}

start_compss_app() {
  appName=$(basename "${fullAppPath}")

  # Prepare COMPSs Runtime + Bindings environment
  prepare_runtime_environment
  
  append_app_jvm_options_to_file "${jvm_options_file}"

  # Init COMPSs
  echo -e "\\n----------------- Executing $appName --------------------------\\n"
 # Launch application execution
  if [ ${lang} = java ]; then
    exec_java
  elif [ ${lang} = c ]; then
    exec_c
  elif [ ${lang} = python ]; then
    exec_python
  fi

  # End
  echo
  echo ------------------------------------------------------------
}

exec_java() {
  # Define command
  local java_opts
  local JAVACMD
  java_opts=$(tr "\\n" " " < "${jvm_options_file}")
  JAVACMD=$JAVA" -noverify -classpath ${CLASSPATH}:${COMPSS_HOME}/Runtime/compss-engine.jar ${java_opts}"
  
  # Launch application
  start_tracing
  # shellcheck disable=SC2086
  $JAVACMD "${RUNTIME_LOADER}" "total" "$fullAppPath" ${application_args}
  endCode=$?
  stop_tracing
  if [ $endCode -ne 0 ]; then
    fatal_error "${RUNTIME_ERROR}" ${endCode}
  fi
}


exec_c() {
  # Export needed variables
  if [ -d "${COMPSS_HOME}/Bindings/c" ]; then
    local CPP_COMPSS_HOME=${COMPSS_HOME}/Bindings/c
    export CPP_PATH=${CPP_COMPSS_HOME}:$cp
  else
    export CPP_PATH=$cp
  fi

  cat >> "${jvm_options_file}" << EOT
-Dcompss.constraints.file=$fullAppPath.idl
EOT

  # Launch application
  echo "JVM_OPTIONS_FILE: $JVM_OPTIONS_FILE"
  echo "COMPSS_HOME: $COMPSS_HOME"
  echo "Args: $application_args"
  echo " "
 
  start_tracing
  # shellcheck disable=SC2086
  $fullAppPath ${application_args}
  endCode=$?
  stop_tracing

  if [ $endCode -ne 0 ]; then
    fatal_error "${RUNTIME_ERROR}" ${endCode}
  fi
}

exec_python() {
  PYCOMPSS_HOME=${COMPSS_HOME}/Bindings/python/${python_version}
  export PYTHONPATH=${PYCOMPSS_HOME}:$PYTHONPATH

  # Initialize python flags
  if [ "$log_level" != "debug" ] && [ "$log_level" != "trace" ] ; then
    py_flags="-u -O"
  else
    py_flags="-u"
  fi

  # Launch application
  start_tracing
  # shellcheck disable=SC2086
  $python_interpreter ${py_flags} "$PYCOMPSS_HOME"/pycompss/runtime/launch.py ${log_level} ${PyObject_serialize} ${storageConf} ${streaming} ${streaming_master_name} ${streaming_master_port} "${fullAppPath}" ${application_args}
  endCode=$?
  stop_tracing

  if [ $endCode -ne 0 ]; then
    fatal_error "${RUNTIME_ERROR}" ${endCode}
  fi
}

#----------------------------------------------
# Clean up an application environment 
#----------------------------------------------
clear_compss_app() {
  clean_runtime_environment
}
