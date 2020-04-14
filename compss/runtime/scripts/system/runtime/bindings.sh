source "${COMPSS_HOME}Runtime/scripts/system/commons/logger.sh"
source "${COMPSS_HOME}Runtime/scripts/system/commons/utils.sh"

###############################################
###############################################
#            CONSTANTS DEFINITION
###############################################
###############################################

#----------------------------------------------
# DEFAULT VALUES
#----------------------------------------------
# Number of CE
DEFAULT_TASK_COUNT=50

# Application language
DEFAULT_LANGUAGE=java

# Python
DEFAULT_PyOBJECT_SERIALIZE=false
DEFAULT_PYTHON_INTERPRETER=python
DEFAULT_PYTHON_VERSION=$( ${DEFAULT_PYTHON_INTERPRETER} -c "import sys; print(sys.version_info[:][0])" )
if [ -z "${VIRTUAL_ENV}" ]; then
  DEFAULT_PYTHON_VIRTUAL_ENVIRONMENT="null"
else
  DEFAULT_PYTHON_VIRTUAL_ENVIRONMENT="${VIRTUAL_ENV}"
fi
DEFAULT_PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT=true
DEFAULT_PYTHON_MPI_WORKER=false

# C
DEFAULT_PERSISTENT_WORKER_C=false

#----------------------------------------------
# ERROR MESSAGES
#----------------------------------------------
WARN_INCOMPATIBLE_ADAPTOR_AND_PERSITENT_C="Persistent C worker is not compatible with the specified adaptor"
JAVA_JRE_ERROR="ERROR: Can't find JVM libraries in JAVA_HOME. Please check your Java JRE Installation."

###############################################
###############################################
#        BINDINGS HANDLING FUNCTIONS
###############################################
###############################################
#----------------------------------------------
# CHECK BINDING-RELATED ENV VARIABLES
#----------------------------------------------
check_bindings_env() {
  : 
}


#----------------------------------------------
# CHECK BINDINGS-RELATED SETUP values
#----------------------------------------------
check_bindings_setup () {
  if [ -z "$task_count" ]; then
    task_count=${DEFAULT_TASK_COUNT}
  fi

  # Language handling
  enable_java=false
  enable_bindings=false
  enable_c=false
  enable_python=false
  if [ -z "$lang" ]; then
    enable_java=true
    enable_bindings=true
    enable_c=true
    enable_python=true
  elif [ "$lang" == "java" ]; then
    enable_java=true
  elif [ "$lang" == "c" ]; then
    enable_bindings=true
    enable_c=true
  elif [ "$lang" == "python" ]; then
    enable_bindings=true
    enable_python=true
  fi

  if [ ${enable_bindings} ]; then
    # Look for the JVM Library
    libjava=$(find "${JAVA_HOME}"/jre/lib/ -name libjvm.so | head -n 1)
    if [ -z "$libjava" ]; then
      libjava=$(find "${JAVA_HOME}"/jre/lib/ -name libjvm.dylib | head -n 1)
      if [ -z "$libjava" ]; then
        fatal_error "${JAVA_JRE_ERROR}"
      fi
    fi
    libjavafolder=$(dirname "$libjava")
    export LD_LIBRARY_PATH=$libjavafolder:${COMPSS_HOME}/Bindings/bindings-common/lib:$COMPSS_HOME/Bindings/c/lib:${LD_LIBRARY_PATH}
  fi

  # PYTHON BINDING VARIABLES
  # ------------------------
  # Add application folder to PYTHONPATH
  module_name=$(basename "${fullAppPath}")
  if [ -e "${DEFAULT_APPDIR}/${module_name}" ]; then
    pythonpath=${DEFAULT_APPDIR}:${pythonpath}
  else
    module_folder=$(readlink -f "${fullAppPath}" | xargs dirname)
    pythonpath=${module_folder}:${pythonpath}
  fi

  if [ -z "$PyObject_serialize" ]; then
    PyObject_serialize="${DEFAULT_PyOBJECT_SERIALIZE}"
  fi
  
  if [ -z "$python_interpreter" ]; then
    python_interpreter=$DEFAULT_PYTHON_INTERPRETER
    python_version=$DEFAULT_PYTHON_VERSION
  else
    python_version=$( ${python_interpreter} -c "import sys; print(sys.version_info[:][0])")
  fi

  if [ -z "$python_propagate_virtual_environment" ]; then
	  python_propagate_virtual_environment=$DEFAULT_PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT
  fi

  if [ -z "$python_mpi_worker" ]; then
	  python_mpi_worker=$DEFAULT_PYTHON_MPI_WORKER
  fi

  if [ ${enable_python} ]; then
    if ! command_exists "${python_interpreter}" ; then
      fatal_error "ERROR: Python interpreter $python_interpreter does not exist." 1
    fi

    # Check if installed
    if [ ! -d "${COMPSS_HOME}/Bindings/python/$python_version" ]; then
      fatal_error "PyCOMPSs for Python $python_version not installed." 1
    fi
  fi

  # Options ONLY for C
  # ------------------------
  if [ -z "$appdir" ]; then
    appdir=$DEFAULT_APPDIR
  fi

  if [ -z "$persistent_worker_c" ]; then
    persistent_worker_c=${DEFAULT_PERSISTENT_WORKER_C}
  fi

  if [ "${persistent_worker_c}" == "true" ]; then
    if [ "${comm}" == "${NIO_ADAPTOR}" ]; then
      export COMPSS_PERSISTENT_BINDING=0
    else
      display_warning "${WARN_INCOMPATIBLE_ADAPTOR_AND_PERSITENT_C}"
      persistent_worker_c="false" 
    fi
  fi
}



#----------------------------------------------
# APPEND PROPERTIES TO FILE - Language enabled
#----------------------------------------------
append_bindings_jvm_options_to_file() {
  local jvm_options_file=${1}
  cat >> "${jvm_options_file}" << EOT
-Dcompss.lang=${lang}
-Dcompss.core.count=${task_count}
EOT
  if [ ${enable_java} ]; then
    add_bindings_jvm_opts_java "${jvm_options_file}"
  fi
  if [ ${enable_bindings} ]; then
    add_bindings_jvm_opts_bindings "${jvm_options_file}"
  fi
  if [ ${enable_python} ]; then
    add_bindings_jvm_opts_python "${jvm_options_file}"
  fi
  if [ ${enable_c} ]; then
    add_bindings_jvm_opts_c "${jvm_options_file}"
  fi
}

add_bindings_jvm_opts_java() {
  :  # no need to to anything for java
}

add_bindings_jvm_opts_bindings() {  # PLEASE: Any new parameter added here may be also added into interactive.py config dict.
  local jvm_options_file=${1}
  cat >> "${jvm_options_file}" << EOT
-Djava.class.path=${CLASSPATH}:${COMPSS_HOME}/Runtime/compss-engine.jar
-Djava.library.path=${LD_LIBRARY_PATH}
EOT
}

add_bindings_jvm_opts_c() {
  local jvm_options_file=${1}
  cat >> "${jvm_options_file}" << EOT
-Dcompss.worker.persistent.c=${persistent_worker_c}
-Dcompss.worker.appdir=${appdir}
-Dcompss.constraints.file=$fullAppPath.idl
EOT
}

add_bindings_jvm_opts_python() {  # PLEASE: Any new parameter added here may be also added into interactive.py config dict.
  local jvm_options_file=${1}
  cat >> "${jvm_options_file}" << EOT
-Dcompss.worker.pythonpath=${PYTHONPATH}
-Dcompss.python.interpreter=${python_interpreter}
-Dcompss.python.version=${python_version}
-Dcompss.python.virtualenvironment=${DEFAULT_PYTHON_VIRTUAL_ENVIRONMENT}
-Dcompss.python.propagate_virtualenvironment=${python_propagate_virtual_environment}
-Dcompss.python.mpi_worker=${python_mpi_worker}
EOT
}


#----------------------------------------------
# CLEAN ENV
#----------------------------------------------
clean_bindings_env () {
  :
}
