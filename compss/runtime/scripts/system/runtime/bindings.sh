#!/bin/bash

if [ -n "${LOADED_SYSTEM_RUNTIME_BINDINGS}" ]; then
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

# shellcheck source=../system/commons/utils.sh
# shellcheck disable=SC1091
source "${COMPSS_HOME}/Runtime/scripts/system/commons/utils.sh"

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
DEFAULT_PYTHON_INTERPRETER=python3
if [ -z "${VIRTUAL_ENV}" ]; then
  DEFAULT_PYTHON_VIRTUAL_ENVIRONMENT="null"
else
  DEFAULT_PYTHON_VIRTUAL_ENVIRONMENT="${VIRTUAL_ENV}"
fi
DEFAULT_PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT=true
DEFAULT_PYTHON_MPI_WORKER=false
DEFAULT_PYTHON_MEMORY_PROFILE=false
DEFAULT_PYTHON_WORKER_CACHE=false
DEFAULT_CACHE_PROFILER=false

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
  enable_java="false"
  enable_bindings="false"
  enable_c="false"
  enable_python="false"
  if [ -z "$lang" ]; then
    lang="UNKNOWN"
    enable_java="true"
    enable_bindings="true"
    enable_c="true"
    if [ -d "${COMPSS_HOME}/Bindings/python/2" ] || [ -d "${COMPSS_HOME}/Bindings/python/3" ]; then
      enable_python="true"
    else
      enable_python="false"
    fi
  elif [ "$lang" = "java" ]; then
    enable_java="true"
  elif [ "$lang" = "c" ]; then
    enable_bindings="true"
    enable_c="true"
  elif [ "$lang" = "python" ]; then
    enable_bindings="true"
    enable_python="true"
  fi

  if [ "${enable_bindings}" = "true" ]; then
    # Look for the JVM Library
    if [ -d "${JAVA_HOME}/jre/lib/" ]; then #Java 8 case 
      libjava=$(find "${JAVA_HOME}"/jre/lib/ -name libjvm.so | head -n 1)
      if [ -z "$libjava" ]; then
        libjava=$(find "${JAVA_HOME}"/jre/lib/ -name libjvm.dylib | head -n 1)
        if [ -z "$libjava" ]; then
          fatal_error "${JAVA_JRE_ERROR}" 1
	      fi
      fi
    else # Java 9+
      libjava=$(find "${JAVA_HOME}"/lib/ -name libjvm.so | head -n 1)
      if [ -z "$libjava" ]; then
        libjava=$(find "${JAVA_HOME}"/lib/ -name libjvm.dylib | head -n 1)
        if [ -z "$libjava" ]; then
          libjava=$(find "${JAVA_HOME}"/lib/server -name libjvm.dylib | head -n 1)
          if [ -z "$libjava" ]; then
            fatal_error "${JAVA_JRE_ERROR}" 1
          fi
        fi
      fi
    fi
    libjavafolder=$(dirname "$libjava")
    export LD_LIBRARY_PATH=$libjavafolder:${COMPSS_HOME}/Bindings/bindings-common/lib:$COMPSS_HOME/Bindings/c/lib:${LD_LIBRARY_PATH}
  fi

  # PYTHON BINDING VARIABLES
  # ------------------------
  # Add application folder to PYTHONPATH

  if [ "${enable_python}" = "true" ]; then
    module_name=$(basename "${fullAppPath}")
    if [ -e "${DEFAULT_APPDIR}/${module_name}" ]; then
      export PYTHONPATH=${DEFAULT_APPDIR}:${pythonpath}:${PYTHONPATH}
    else
      module_folder=$(readlink -f "${fullAppPath}" | xargs dirname)
      export PYTHONPATH=${module_folder}:${pythonpath}:${PYTHONPATH}
    fi

    if [ -z "$PyObject_serialize" ]; then
       PyObject_serialize="${DEFAULT_PyOBJECT_SERIALIZE}"
    fi

    if [ -z "$python_interpreter" ]; then
      python_interpreter=$DEFAULT_PYTHON_INTERPRETER
      if ! command_exists "${python_interpreter}" ; then
        fatal_error "ERROR: Python interpreter $python_interpreter does not exist." 1
      fi
      python_version=$( ${python_interpreter} -c "import sys; print(sys.version_info[:][0])")
    else
      if [ "${coverage}" = "true" ]; then
        echo "import sys; print(sys.version_info[:][0])" > .tmp.py
        py_aux=$(echo ${python_interpreter} | tr "#" " ")
        python_version=$(${py_aux} .tmp.py)
        rm .tmp.py
        if ! command_exists ${py_aux} ; then
          fatal_error "ERROR: Python interpreter $py_aux does not exist." 1
        fi
      else
  	    python_version=$( ${python_interpreter} -c "import sys; print(sys.version_info[:][0])")
        if ! command_exists "${python_interpreter}" ; then
          fatal_error "ERROR: Python interpreter $python_interpreter does not exist." 1
        fi
	    fi
    fi

    if [ -z "$python_propagate_virtual_environment" ]; then
            python_propagate_virtual_environment=$DEFAULT_PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT
    fi

    if [ -z "$python_mpi_worker" ]; then
            python_mpi_worker=$DEFAULT_PYTHON_MPI_WORKER
    fi

    if [ -z "$python_memory_profile" ]; then
            python_memory_profile=$DEFAULT_PYTHON_MEMORY_PROFILE
    fi


    if [ -z "$python_worker_cache" ]; then
      python_worker_cache=$DEFAULT_PYTHON_WORKER_CACHE
    else
      # Check python version >= 3.8
      cache_supported=$($python_interpreter -c"import sys; print('true' if sys.version_info >= (3, 8) else 'false')")
      if [ "$python_worker_cache" = "false" ]; then
        python_worker_cache="false"
      elif [ "$python_worker_cache" = "true" ]; then
        if [ "$cache_supported" = "true" ]; then
          python_worker_cache="true"
        else
          echo "WARNING: Can not enable python worker cache."
          python_worker_cache="false"
        fi
      else
        # size
        if [ "$cache_supported" = "true" ]; then
          python_worker_cache="$python_worker_cache"
        else
          echo "WARNING: Can not enable python worker cache."
          python_worker_cache="false"
        fi
      fi
    fi

    if [ -z "$python_cache_profiler" ]; then
      python_cache_profiler=$DEFAULT_PYTHON_CACHE_PROFILER
    else
      # Check python version >= 3.8
      cache_supported=$($python_interpreter -c"import sys; print('true' if sys.version_info >= (3, 8) else 'false')")
      if [ "$python_cache_profiler" = "false" ]; then
        python_cache_profiler="false"
      elif [ "$python_cache_profiler" = "true" ]; then
        if [ "$cache_supported" = "true" ]; then
          python_cache_profiler="true"
        else
          echo "WARNING: Can not enable python worker cache."
          python_cache_profiler="false"
        fi
      fi
    fi

    # Check if installed
    if [ ! -d "${COMPSS_HOME}/Bindings/python/$python_version" ]; then
      fatal_error "PyCOMPSs for Python $python_version not installed." 1
    fi
  fi

  # Options ONLY for C
  # ------------------------

  if [ -z "$persistent_worker_c" ]; then
    persistent_worker_c=${DEFAULT_PERSISTENT_WORKER_C}
  fi

  if [ "${persistent_worker_c}" = "true" ]; then
    if [ "${comm}" = "${NIO_ADAPTOR}" ]; then
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
  if [ "${enable_java}" = "true" ]; then
    add_bindings_jvm_opts_java "${jvm_options_file}"
  fi
  if [ "${enable_bindings}" = "true" ]; then
    add_bindings_jvm_opts_bindings "${jvm_options_file}"
  fi
  if [ "${enable_python}" = "true" ]; then
    add_bindings_jvm_opts_python "${jvm_options_file}"
  fi
  if [ "${enable_c}" = "true" ]; then
    add_bindings_jvm_opts_c "${jvm_options_file}"
  fi
}

add_bindings_jvm_opts_java() {
  :  # no need to to anything for java
}

add_bindings_jvm_opts_bindings() {  # PLEASE: Any new parameter added here may be also added into interactive.py config dict.
  local jvm_options_file=${1}
  cat >> "${jvm_options_file}" << EOT
-Djava.class.path=${CLASSPATH}:${COMPSS_HOME}/Runtime/compss-engine.jar:${COMPSS_HOME}/Runtime/compss-agent-impl.jar
-Djava.library.path=${LD_LIBRARY_PATH}
EOT
}

add_bindings_jvm_opts_c() {
  local jvm_options_file=${1}
  cat >> "${jvm_options_file}" << EOT
-Dcompss.worker.persistent.c=${persistent_worker_c}
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
-Dcompss.python.worker_cache=${python_worker_cache}
-Dcompss.python.cache_profiler=${python_cache_profiler}
EOT
}


#----------------------------------------------
# CLEAN ENV
#----------------------------------------------
clean_bindings_env () {
  :
}

LOADED_SYSTEM_RUNTIME_BINDINGS=1