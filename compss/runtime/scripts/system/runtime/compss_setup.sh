#!/bin/bash

if [ -n "${LOADED_SYSTEM_RUNTIME_COMPSS_SETUP}" ]; then
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

# shellcheck source=../system/commons/java.sh
# shellcheck disable=SC1091
source "${COMPSS_HOME}/Runtime/scripts/system/commons/java.sh"

# shellcheck source=./analysis.sh
# shellcheck disable=SC1091
source "${COMPSS_HOME}/Runtime/scripts/system/runtime/analysis.sh"
# shellcheck source=./worker.sh
# shellcheck disable=SC1091
source "${COMPSS_HOME}/Runtime/scripts/system/runtime/worker.sh"
# shellcheck source=./adaptors.sh
# shellcheck disable=SC1091
source "${COMPSS_HOME}/Runtime/scripts/system/runtime/adaptors.sh"
# shellcheck source=./scheduler.sh
# shellcheck disable=SC1091
source "${COMPSS_HOME}/Runtime/scripts/system/runtime/scheduler.sh"
# shellcheck source=./checkpoint.sh
# shellcheck disable=SC1091
source "${COMPSS_HOME}/Runtime/scripts/system/runtime/checkpoint.sh"
# shellcheck source=./bindings.sh
# shellcheck disable=SC1091
source "${COMPSS_HOME}/Runtime/scripts/system/runtime/bindings.sh"

# shellcheck source=./streams.sh
# shellcheck disable=SC1091
source "${COMPSS_HOME}/Runtime/scripts/system/runtime/streams.sh"
# shellcheck source=./tracing.sh
# shellcheck disable=SC1091
source "${COMPSS_HOME}/Runtime/scripts/system/runtime/tracing.sh"
# shellcheck source=./storage.sh
# shellcheck disable=SC1091
source "${COMPSS_HOME}/Runtime/scripts/system/runtime/storage.sh"


###############################################
###############################################
#     SIGNAL INTERCEPTION HANDLERS
###############################################
###############################################

bus_handler() {
  echo "ERROR: MASTER RUN OUT OF MEMORY"
}
trap bus_handler SIGBUS


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
DEFAULT_AGENT_CONFIG="${COMPSS_HOME}/Runtime/configuration/agents/all.json"

# Master's JVM options
DEFAULT_JVM_MASTER=""

# Loader implementation
RUNTIME_LOADER=es.bsc.compss.loader.ITAppLoader

# Default wall clock limit
DEFAULT_WALL_CLOCK_LIMIT=0

#----------------------------------------------
# ERROR MESSAGES
#----------------------------------------------
AGENT_ERROR="Error running the agent"
RUNTIME_ERROR="Error running application"
TMP_FILE_JVM_ERROR="ERROR: Can't create temporary file for JVM options."
LD_LIBRARY_PATH_NOT_SET_WARN="LD_LIBRARY_PATH not defined set to LIBRARY_PATH"
EXEC_DIR_CREATION_ERROR="Could not create execution folder"

###############################################
###############################################
#        COMPSs HANDLING FUNCTIONS
###############################################
###############################################
#----------------------------------------------
# CHECK COMPSs-RELATED ENV VARIABLES
#----------------------------------------------
check_compss_env() {
  # Added for SGE queue systems which do not allow to copy LD_LIBRARY_PATH
  if [ -z "${LD_LIBRARY_PATH}" ]; then
     # shellcheck disable=SC2153
     if [ -n "${LIBRARY_PATH}" ]; then
         export LD_LIBRARY_PATH=$LIBRARY_PATH
         display_warning "${LD_LIBRARY_PATH_NOT_SET_WARN}"
     fi
  fi

  check_analysis_env

  check_scheduler_env
  check_checkpoint_env
  check_adaptors_env
  check_worker_env

  check_bindings_env

  check_stream_env
  check_storage_env

  check_tracing_env

  #gen_core and appName can be empty
}


#----------------------------------------------
# Creating Execution directory
#----------------------------------------------
create_exec_folder() {
  # specific_log_dir can be empty. If so, specified uses ${log_dir}/<application_name>_<overload offset>
  # log_dir can be empty. If so, placing it in user's home folder .COMPSs
  if [ -n  "${specific_log_dir}" ]; then
    mkdir -p "${specific_log_dir}"
    exec_dir="${specific_log_dir}"

    if [ ! "${exec_dir: -1}" == "/" ]; then
      exec_dir="${exec_dir}/"
    fi
  else
    if [ -n  "${log_dir}" ]; then
      exec_dir="${log_dir}"
    else
      exec_dir="${HOME}/.COMPSs"
    fi

    if ! mkdir -p "${exec_dir}"; then
      fatal_error "${EXEC_DIR_CREATION_ERROR}" 1
    fi

    if [ ! "${exec_dir: -1}" == "/" ]; then
      exec_dir="${exec_dir}/"
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
        overload_log_dir="${exec_dir}${overload_tag}"


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
        display_warning "${WARN_LOG_OVERRIDE} ${exec_dir}"
        rm -rf "${exec_dir}${app_folder_name}"
      fi

      mkdir "${exec_dir}${app_folder_name}"
      folder_creation_exit_code="${?}"
    done
    exec_dir="${exec_dir}${app_folder_name}/"
  fi
}

#----------------------------------------------
# CHECK COMPSS-RELATED SETUP values
#----------------------------------------------
check_compss_setup () {
  appName=$(basename "${fullAppPath}")
  if [ -z "${uuid}" ]; then
    get_uuid
  fi

  create_exec_folder

  # JVM
  if [ -z "${jvm_master_opts}" ] || [ "${jvm_master_opts}" = \"\" ]; then
    jvm_master_opts=${DEFAULT_JVM_MASTER}
  fi
  # Change jvm master opts separation character "," by " "
  jvm_master_opts=$(echo "${jvm_master_opts}" | tr "," "\\n")

  # Application Dir
  if [ -z "$appdir" ]; then
    appdir="${DEFAULT_APPDIR}"
  fi

  # Classpath
  if [ -z "${cp}" ]; then
    cp="${DEFAULT_CLASSPATH}"
    for jar in "${DEFAULT_CLASSPATH}"/*.jar; do
       cp=$cp:$jar
    done
  else
    fcp=""
    convert_pathlist_to_absolute_paths "${cp}"
    cp="${fcp}"
    display_info "Relative Classpath resolved: $cp"
  fi
  export CLASSPATH="${cp}:${CLASSPATH}"

  # Library Path
  if [ -z "${library_path}" ]; then
    library_path=${DEFAULT_LIBRARY_PATH}
  fi
  if [ -z "${LD_LIBRARY_PATH}" ]; then
    export LD_LIBRARY_PATH=${library_path}
  else
    export LD_LIBRARY_PATH=${library_path}:${LD_LIBRARY_PATH}
  fi

  # Python Path
  if [ -z "${pythonpath}" ]; then
    pythonpath="${DEFAULT_PYTHONPATH}"
  else
    # Adds execution dir by default to pythonpath
    pythonpath="${pythonpath}:${DEFAULT_PYTHONPATH}"
  fi
  if [ -z "${PYTHONPATH}" ]; then
    export PYTHONPATH="${pythonpath}"
  else
    export PYTHONPATH="${pythonpath}:${PYTHONPATH}"
  fi

  if [ -z "${agent_config}" ]; then
    agent_config="${DEFAULT_AGENT_CONFIG}"
  fi

  if [ -n "${wdir_in_master}" ]; then
    wdir_in_master="${wdir_in_master}/.COMPSs/${uuid}/tmpFiles/"
  else
    wdir_in_master="${exec_dir}/tmpFiles/"
  fi
  mkdir -p ${wdir_in_master}

  if [ -z "${wall_clock_limit}" ]; then
    wall_clock_limit="${DEFAULT_WALL_CLOCK_LIMIT}"
  fi

  check_analysis_setup

  check_worker_setup

  check_adaptors_setup

  check_scheduler_setup

  check_checkpoint_setup

  check_bindings_setup

  check_stream_setup

  check_storage_setup

  check_tracing_setup
}

#----------------------------------------------
# Write down current configuration to file
#----------------------------------------------
generate_jvm_opts_file() {
  if [[ "$OSTYPE" == "darwin"* ]]; then
    jvm_options_file=$(mktemp -t /tmp) || arguments_error "${TMP_FILE_JVM_ERROR}"
  else
    jvm_options_file=$(mktemp -p /tmp) || arguments_error "${TMP_FILE_JVM_ERROR}"
  fi
  export JVM_OPTIONS_FILE=${jvm_options_file}

  # PLEASE: Any new parameter added here may be also added into interactive.py config dict.

  cat >> "${jvm_options_file}" << EOT
${jvm_master_opts}
EOT
  if [ "$(uname -m)" != "riscv64" ]; then
     cat >> "${jvm_options_file}" << EOT
-XX:+PerfDisableSharedMem
-XX:-UsePerfData
-XX:+UseG1GC
-XX:+UseThreadPriorities
-XX:ThreadPriorityPolicy=0
EOT
  fi
  cat >> "${jvm_options_file}" << EOT
-javaagent:${COMPSS_HOME}/Runtime/compss-engine.jar
-Dcompss.to.file=false
-Dcompss.appName=${appName}
-Dcompss.exec.label=${exec_label}
-Dcompss.data_provenance=${provenance}
-Dcompss.uuid=${uuid}
-Dcompss.shutdown_in_node_failure=${shutdown_in_node_failure}
-Dcompss.master.workingDir=${wdir_in_master}
EOT
  append_analysis_jvm_options_to_file "${jvm_options_file}"
  append_worker_jvm_options_to_file "${jvm_options_file}"
  append_adaptors_jvm_options_to_file "${jvm_options_file}"
  append_scheduler_jvm_options_to_file "${jvm_options_file}"
  append_checkpoint_jvm_options_to_file "${jvm_options_file}"

  append_bindings_jvm_options_to_file "${jvm_options_file}"

  append_stream_jvm_options_to_file "${jvm_options_file}"
  append_storage_jvm_options_to_file "${jvm_options_file}"

  append_tracing_jvm_options_to_file "${jvm_options_file}"

}


prepare_coverage() {
    jacoco_master_expression=$(echo "${jacoco_agent_expression}" | tr "#" "," | tr "@" ",")
    if [ -z "${jvm_master_opts}" ] || [ "${jvm_master_opts}" = \"\" ];then
      jvm_master_opts="-javaagent:${jacoco_master_expression}"
    else
      if [[ $jvm_master_opts == *"-agentpath"* ]] || [[ $jvm_master_opts == *"-javaagent"* ]]; then
        echo "WARN: Ignoring jacoco coverage at master because application already uses a java agent"
      else
       jvm_master_opts+=",-javaagent:"
       jvm_master_opts=$(echo $jvm_master_opts | tr "," "\\n")
       jvm_master_opts+="${jacoco_master_expression}"
      fi
    fi

    #Adding worker jacoco agent in jvm options
    IFS='#'
    aux=$jacoco_agent_expression
    read -ra ADDR <<< "${aux}"
    location=${ADDR[0]}
    options=${ADDR[1]}
    IFS='/'
    read -ra ADDR <<< "${ADDR[0]}"
    IFS=''
    p=0
    text="${ADDR[-1]}"
    ADDR[-1]="${ADDR[-1]:0:p}"workerffff"${ADDR[-1]:p}"
    for i in "${ADDR[@]}"; do
        final+="${i}/"
    done
    final=${final%?}
    IFS=','
    read -ra ADDR <<< "${aux}"
    IFS=' '
    ADDR[0]=${final}
    for i in "${ADDR[@]}"; do
        final2+="${i},"
    done
    final2=${final2%?}
    if [ -z "$options" ]; then
        jacoco_worker_expression="-javaagent:${final2}"
    else
        jacoco_worker_expression="-javaagent:${final2}#${options}"
    fi
    if [ -z "${jvm_workers_opts}" ] || [ "${jvm_workers_opts}" = \"\" ];then
        jvm_workers_opts="${jacoco_worker_expression}"
    else
      if [[ $jvm_workers_opts == *"-agentpath"* ]] || [[ $jvm_workers_opts == *"-javaagent"* ]]; then
        echo "WARN: Ignoring jacoco coverage at master because application already uses a java agent"
      else
       jvm_workers_opts+=",${jacoco_worker_expression}"
      fi
    fi

    #Adding coverage for python
    destfile="${location}"
    IFS='='
    read -ra ADDR <<< "${location}"
    IFS=' '
    IFS='.'
    read -ra ADDR <<< "${ADDR[2]}"
    IFS=' '
    final=""
    final=$(echo "${ADDR[0]}" | rev | cut -d"/" -f2- | rev)
    #echo "[run]" > /tmp/coverage_rc
    #echo "parallel=true" >> /tmp/coverage_rc
    #echo "data_file=${final}/coverage" >> /tmp/coverage_rc
    #python_interpreter="coverage${python_version}#run#--rcfile=/tmp/coverage_rc"
    python_interpreter="coverage${python_version}#run#--rcfile=${final}/coverage_rc"
}

#----------------------------------------------
# Prepares all the necessary configuration for the runtime
#----------------------------------------------
prepare_runtime_environment() {
  # Create tmp dir for initial loggers configuration
  mkdir -p "/tmp/$uuid"


  #Coverage Mode logic
  if [ -n "${coverage}" ]; then
    prepare_coverage
  fi

  if [ -n "${gen_core}" ]; then
    prepare_coredump_generation
  fi

  if [ -n "${keepWD}" ]; then
    prepare_keep_workingdir
  fi

  # Create JVM Options file
  generate_jvm_opts_file

  # Start streaming backend if required
  start_stream_backends

}

prepare_coredump_generation() {
    display_info "[COMPSS] Setting coredump generation."
    ulimit -c unlimited
    if [ -z "${jvm_workers_opts}" ] || [ "${jvm_workers_opts}" = \"\" ];then
        jvm_workers_opts="-Dcompss.worker.gen_coredump=true"
    else
        jvm_workers_opts+=",-Dcompss.worker.gen_coredump=true"
    fi
}

prepare_keep_workingdir() {
    if [ -z "${jvm_workers_opts}" ] || [ "${jvm_workers_opts}" = \"\" ];then
        jvm_workers_opts="-Dcompss.worker.removeWD=false"
    else
        jvm_workers_opts+=",-Dcompss.worker.removeWD=false"
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

append_wall_clock_jvm_options_to_file() {
  # Add Application-specific options
  if [ "${lang}" == "python" ]; then
    cat >> "${jvm_options_file}" << EOT
-Dcompss.wcl=0
EOT
  else
    cat >> "${jvm_options_file}" << EOT
-Dcompss.wcl=${wall_clock_limit}
EOT
  fi

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
  rm -rf "/tmp/$uuid"

  # Remove master working dir
  rm -rf "${wdir_in_master}"
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
  # Prepare COMPSs Runtime + Bindings environment
  prepare_runtime_environment

  append_app_jvm_options_to_file "${jvm_options_file}"

  append_wall_clock_jvm_options_to_file "${jvm_options_file}"
  #echo "Options file: ${jvm_options_file}"
  #cat ${jvm_options_file}


  # Init COMPSs
  echo -e "\\n----------------- Executing $appName --------------------------\\n"
 # Launch application execution
  if [ "${lang}" == "java" ]; then
    exec_java
  elif [ "${lang}" == "c" ]; then
    exec_c
  elif [ "${lang}" == "python" ]; then
    exec_python
  elif [ "${lang}" == "r" ]; then
    exec_r
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
    local CPP_COMPSS_HOME="${COMPSS_HOME}/Bindings/c"
    export CPP_PATH="${CPP_COMPSS_HOME}:${cp}"
  else
    export CPP_PATH=${cp}
  fi

  cat >> "${jvm_options_file}" << EOT
-Dcompss.constraints.file=$fullAppPath.idl
EOT

  # Launch application
  echo "JVM_OPTIONS_FILE: ${JVM_OPTIONS_FILE}"
  echo "COMPSS_HOME: ${COMPSS_HOME}"
  echo "Args: ${application_args}"
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
  PYCOMPSS_HOME="${COMPSS_HOME}/Bindings/python/${python_version}"
  export PYTHONPATH=${PYCOMPSS_HOME}:$PYTHONPATH
  #CHANGED TO SUPPORT coverage#run as command
  python_interpreter=$(echo "${python_interpreter}" | tr "#" " ")

  # Initialize python flags
  if [ "${coverage}" != "true" ]; then
    if [ "${log_level}" != "debug" ] && [ "${log_level}" != "trace" ] ; then
      py_flags="-u -O"
    else
      py_flags="-u"
    fi
    if [ "${python_memory_profile}" == "true" ]; then
      py_flags="${py_flags} -m mprof run --multiprocess --include-children"
    fi
  fi

  # Launch application
  start_tracing
  # shellcheck disable=SC2086
  ${python_interpreter} ${py_flags} "${PYCOMPSS_HOME}/pycompss/runtime/launch.py" ${wall_clock_limit} ${log_level} ${tracing} ${PyObject_serialize} ${storageConf} ${streaming} ${streaming_master_name} ${streaming_master_port} "${fullAppPath}" ${application_args}
  endCode=$?
  stop_tracing

  if [ $endCode -ne 0 ]; then
    fatal_error "${RUNTIME_ERROR}" ${endCode}
  fi
}

exec_r(){
  # Launch application
  start_tracing
  # Even though tracing is enabled, fails with Rscript due to R
  # interprets the Extrae's welcome message as input.
  # So, we unset the LD_PRELOAD here and rely in the start_runtime
  unset LD_PRELOAD
  Rscript "${fullAppPath}" ${application_args}
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

LOADED_SYSTEM_RUNTIME_COMPSS_SETUP=1
