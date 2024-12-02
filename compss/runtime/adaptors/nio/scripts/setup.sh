#!/bin/bash
  JAVA_JRE_ERROR="ERROR: Can't find JVM libraries in JAVA_HOME. Please check your Java JRE Installation."

  NUM_PARAMS=41

  ######################
  # INTERNAL FUNCTIONS
  ######################
  add_to_classpath () {
    local baseDir=${1}
    for i in ${baseDir}/*.jar; do
      CLASSPATH=$CLASSPATH:"$i"
    done
    export CLASSPATH=$CLASSPATH
  }
  # Displays runtime/application errors
  error_msg() {
    local error_msg=$1

    # Display error
    echo
    echo "$error_msg"
    echo

    # Exit
    exit 1
  }

  ######################
  # COMMON HELPER FUNCTIONS
  ######################
  load_parameters() {
    # Script Variables
    if [ -z "${SCRIPT_DIR}" ]; then
        if [ -z "$COMPSS_HOME" ]; then
           SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
           COMPSS_HOME="${SCRIPT_DIR}/../../../../.."
        else
           SCRIPT_DIR="${COMPSS_HOME}/Runtime/scripts/system/adaptors/nio"
        fi
    fi

    #Unset PMI vars affecting MPI Tasks
    unset PMI_FD
    unset PMI_JOBID
    unset PMI_RANK
    unset PMI_SIZE

    # Get parameters
    envScriptPath=$1
    libPath=$2
    appDir=$3
    cp=$4
    streaming=$5
    numJvmFlags=$6

    jvmFlags=""
    for i in $(seq 1 "$numJvmFlags"); do
      pos=$((6 + i))
      jvmFlags="${jvmFlags} ${!pos}"
    done
    #Changed to support Coverage mode
    uuid=$(uuidgen)
    jvmFlags=$(echo "$jvmFlags" | tr "#" ",")
    jvmFlags=$(echo "$jvmFlags" | tr "@" ",")
    jvmFlags="${jvmFlags/ffff/$uuid}"

    # Shift parameters for script and leave only the NIOWorker parameters
    paramsToShift=$((6 + numJvmFlags))
    shift ${paramsToShift}

    FPGAargs=""
    numFPGAargs=$1
    if [ "$numFPGAargs" -gt 0 ]; then
      for i in $(seq 1 "$numFPGAargs"); do
        pos=$((1 + i))
        FPGAargs="${FPGAargs} ${!pos}"
      done
    fi

    # Shift parameters for script and leave only the NIOWorker parameters
    paramsToShift=$((1 + numFPGAargs))
    shift ${paramsToShift}

    # shellcheck disable=SC2034,2124
    paramsToCOMPSsWorker=$@

    # Check number of parameters
    if [ $# -ne ${NUM_PARAMS} ]; then
        echo "ERROR: Incorrect number of parameters. Expected: ${NUM_PARAMS}. Got: $#"
        exit 1
    fi

    # Catch some NIOWorker parameters
    debug=${1}
    hostName=${4}
    worker_port=${5}
    streaming_port=${8}
    cusCPU=${9}
    cusGPU=${10}
    cusFPGA=${11}
    cpuMap=${12}
    lot=${16}
    appUuid=${17}
    lang=${18}
    workingDir=${19}
    installDir=${20}
    appDirNW=${21}
    libPathNW=${22}
    cpNW=${23}
    pythonpath=${24}
    tracing=${25}
    extraeFile=${26}
    hostId=${27}
    traceTaskDependencies=${28}
    storageConf=${29}
    execType=${30}
    persistentBinding=${31}
    pythonInterpreter=${32}
    pythonVersion=${33}
    pythonVirtualEnvironment=${34}
    pythonPropagateVirtualEnvironment=${35}
    pythonExtraeFile=${36}
    pythonMpiWorker=${37}
    pythonWorkerCache=${38}
    pythonCacheProfiler=${39}
    ear=${40}
    provenance=${41}

    #This decides where the worker.* files are stored
    #NIOWorker.java getLogDir decides where the binding_worker.* files are stored
    #TODO: unify this with NIOWorker.java file getLogDir function to have it defined only in one place
    logDir=${workingDir}/log

    if [ "$debug" == "true" ]; then
      echo "setup.sh"
      echo "- HostName:            $hostName"
      echo "- WorkerPort:          ${worker_port}"
      echo "- WorkingDir:          $workingDir"
      echo "- LogDir:              $logDir"
      echo "- InstallDir:          $installDir"

      echo "- Streaming Type:      ${streaming}"
      echo "- Streaming Port:      ${streaming_port}"

      echo "- Computing Units CPU: ${cusCPU}"
      echo "- Computing Units GPU: ${cusGPU}"
      echo "- Computing Units GPU: ${cusFPGA}"
      echo "- Limit Of Tasks:      ${lot}"
      echo "- JVM Opts:            $jvmFlags"

      echo "- AppUUID:             ${appUuid}"
      echo "- Lang:                ${lang}"
      echo "- AppDir:              $appDirNW"
      echo "- libPath:             $libPathNW"
      echo "- Classpath:           $cpNW"
      echo "- Pythonpath:          $pythonpath"
      echo "- Python Interpreter   $pythonInterpreter"
      echo "- Python Version       $pythonVersion"
      echo "- Python Virtual Env.  $pythonVirtualEnvironment"
      echo "- Python Propagate Virtual Env.  $pythonPropagateVirtualEnvironment"
      echo "- Python MPI Worker.   $pythonMpiWorker"
      echo "- Python Worker Cache. $pythonWorkerCache"
      echo "- Python Cache Profiler. $pythonCacheProfiler"
      echo "- Python Extrae File   $pythonExtraeFile"

      echo "- Tracing:             $tracing"
      echo "- ExtraeFile:          ${extraeFile}"
      echo "- HostId:              ${hostId}"
      echo "- TracingTaskDep:      ${traceTaskDependencies}"
      echo "- StorageConf:         ${storageConf}"
      echo "- ExecType:            ${execType}"
      echo "- Persistent:          ${persistentBinding}"
      echo "- Ear:                 ${ear}"
      echo "- Provenance           ${provenance}"
    fi

    # Calculate Log4j file
    if [ "${debug}" == "true" ]; then
      itlog4j_file=COMPSsWorker-log4j.debug
    else
      itlog4j_file=COMPSsWorker-log4j.off
    fi

    # Calculate must erase working dir
    if [[ "$jvmFlags" == *"-Dcompss.worker.removeWD=false"* ]]; then
      eraseWD="false"
    else
      eraseWD="true"
    fi
    # Calculate generate coredump
    if [[ "$jvmFlags" == *"-Dcompss.worker.gen_coredump=true"* ]]; then
      genCoredump="true"
    else
      genCoredump="false"
    fi

    # DLB activation
    if [[ "$cpuMap" == "dlb" ]]; then
      if [ "${debug}" == "true" ]; then
        export COMPSS_WITH_DLB=2
      else
        export COMPSS_WITH_DLB=1
      fi
    else
      export COMPSS_WITH_DLB=0
    fi

  }

  setup_extrae() {
    # Trace initialization
    if [ "${tracing}" == "true" ]; then

      configPath="${SCRIPT_DIR}/../../../../configuration/xml/tracing"

      # Determine source extrae config file
      if [ -z "${extraeFile}" ] || [ "${extraeFile}" == "null" ] || [ "${extraeFile}" == "false" ]; then
        # Only define extraeFile if it is not a custom location
          baseConfigFile="${configPath}/extrae_basic.xml"
      else
          baseConfigFile="${extraeFile}"
      fi

      if [ -z "$EXTRAE_HOME" ]; then
        export EXTRAE_HOME=${COMPSS_HOME}/Dependencies/extrae/
      fi

      tracing_output_dir="${workingDir}"
      mkdir -p "${tracing_output_dir}"

      extraeFile="${workingDir}/extrae.xml"
      cp "${baseConfigFile}" "${extraeFile}"

      escaped_extrae_home=$(echo "${EXTRAE_HOME}" | sed 's_/_\\/_g')
      sed -i "s/{{EXTRAE_HOME}}/${escaped_extrae_home}/g" "${extraeFile}"

      escaped_tracing_output_dir=$(echo "${tracing_output_dir}" | sed 's_/_\\/_g')
      sed -i "s/{{TRACE_OUTPUT_DIR}}/${escaped_tracing_output_dir}/g" "${extraeFile}"

      export EXTRAE_LIB=${EXTRAE_HOME}/lib
      export LD_LIBRARY_PATH=${EXTRAE_LIB}:${LD_LIBRARY_PATH}
      export EXTRAE_CONFIG_FILE=${extraeFile}
      export EXTRAE_USE_POSIX_CLOCK=0
      export AFTER_EXTRAE_LD_PRELOAD=${EXTRAE_HOME}/lib/libpttrace.so
    fi
  }

  setup_environment(){
    # Added for SGE queue systems which do not allow to copy LD_LIBRARY_PATH
    if [ -z "$LD_LIBRARY_PATH" ]; then
        if [ -n "$LIBRARY_PATH" ]; then
            export LD_LIBRARY_PATH=$LIBRARY_PATH
            echo "[persistent_worker.sh] LD_LIBRARY_PATH not defined set to LIBRARY_PATH"
        fi
    fi

    # Set lib path
    if [ "${envScriptPath}" != "null" ]; then
        if [ "$debug" == "true" ]; then
            echo "[persistent_worker.sh] Loading environment scripts"
        fi
        scripts=$(echo "${envScriptPath}" | tr ":" " ")
        echo "${scripts}"
        for script in ${scripts}
        do
            if [ "$debug" == "true" ]; then
                echo "[persistent_worker.sh] Loading ${script}"
            fi
            source "$script"
        done
    fi

    # Create sandbox
    if [ ! -d "$workingDir" ]; then
        mkdir -p "$workingDir"
    fi
    export COMPSS_WORKING_DIR=$workingDir
    mkdir -p "$workingDir"/log
    mkdir -p "$workingDir"/jobs

    # Look for the JVM Library
    if [ -d "${JAVA_HOME}/jre/lib/" ]; then #Java 8 case
    	libjava=$(find "${JAVA_HOME}"/jre/lib/ -name libjvm.so | head -n 1)
    	if [ -z "$libjava" ]; then
            libjava=$(find "${JAVA_HOME}"/jre/lib/ -name libjvm.dylib | head -n 1)
            if [ -z "$libjava" ]; then
                error_msg "${JAVA_JRE_ERROR}"
            fi
        fi
    else # Java 9+
      	libjava=$(find "${JAVA_HOME}"/lib/ -name libjvm.so | head -n 1)
      	if [ -z "$libjava" ]; then
           libjava=$(find "${JAVA_HOME}"/lib/ -name libjvm.dylib | head -n 1)
	          if [ -z "$libjava" ]; then
               error_msg "${JAVA_JRE_ERROR}"
           fi
        fi
    fi
    if [ -n "$libjava" ]; then
        libjavafolder=$(dirname "$libjava")
        export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$libjavafolder
    fi

    # Set lib path
    if [ "$libPath" != "null" ]; then
        export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$libPath
    fi

    # Set appDir
    export COMPSS_APP_DIR=$appDir
    if [ "$appDir" != "null" ]; then
    	add_to_classpath "$appDir"
    	add_to_classpath "$appDir/lib"
    fi

    # Set the classpath
    if [ "$cp" == "null" ]; then
      cp=""
    fi

    # Coredump
    if [ "$genCoredump" == "true" ]; then
        ulimit -c unlimited
    fi

    # Export environment
    export CLASSPATH=$cpNW:$CLASSPATH
    export PYTHONPATH=$pythonpath:$PYTHONPATH:${COMPSS_HOME}/Dependencies/threadpoolctl/
    export LD_LIBRARY_PATH=$libPathNW:${SCRIPT_DIR}/../../../../../Bindings/bindings-common/lib:${SCRIPT_DIR}/../../../../../Bindings/c/lib:$LD_LIBRARY_PATH
  }

  setup_jvm() {
    # Prepare the worker command
    local JAVA=java
    worker_jar=${SCRIPT_DIR}/../../../../adaptors/nio/worker/compss-adaptors-nio-worker.jar
    local main_worker_class=es.bsc.compss.nio.worker.NIOWorker
    perf_jvm_flags="-XX:+PerfDisableSharedMem -XX:-UsePerfData -XX:+UseG1GC -XX:ParallelGCThreads=1"  # -XX:+UseSerialGC"
    compss_jvm_flags="-Dlog4j.configurationFile=${installDir}/Runtime/configuration/log/${itlog4j_file} \
    -Dcompss.streaming=${streaming} \
    -Dcompss.python.interpreter=${pythonInterpreter} \
    -Dcompss.python.version=${pythonVersion} \
    -Dcompss.python.virtualenvironment=${pythonVirtualEnvironment} \
    -Dcompss.python.propagate_virtualenvironment=${pythonPropagateVirtualEnvironment} \
    -Dcompss.extrae.file.python=${pythonExtraeFile} \
    -Dcompss.ear=${ear} \
    -Dcompss.data_provenance=${provenance} \
    -Djava.library.path=$LD_LIBRARY_PATH"
    if [ "$(uname -m)" == "riscv64" ]; then
      worker_jvm_flags="${jvmFlags} ${compss_jvm_flags}"
    else
      worker_jvm_flags="${jvmFlags} ${perf_jvm_flags} ${compss_jvm_flags}"
    fi

    if [ "$lang" = "c" ] && [ "${persistentBinding}" = "true" ]; then
      generate_jvm_opts_file
      # shellcheck disable=SC2034
      cmd="${appDir}/worker/nio_worker_c"
    else
      # shellcheck disable=SC2034
      cmd="$JAVA ${worker_jvm_flags} -classpath $CLASSPATH:${worker_jar} ${main_worker_class}"
    fi

  }

  generate_jvm_opts_file() {
    jvm_worker_opts=$(echo "${worker_jvm_flags}" | tr " " "\\n")
    jvm_options_file=$(mktemp) || error_msg "Error creating java_opts_tmp_file"
    cat >> "${jvm_options_file}" << EOT
${jvm_worker_opts}
-Djava.class.path=$CLASSPATH:${worker_jar}
EOT
  }

  reprogram_fpga() {
    if [ -n "${FPGAargs}" ]; then
        echo "Reprogramming FPGA with the command ${FPGAargs}"
        eval "$FPGAargs"
    fi
  }

  pre_launch() {
    cd "$workingDir" || exit 1

    if [ "${persistentBinding}" = "true" ]; then
    	export COMPSS_HOME=${SCRIPT_DIR}/../../../../../
    	export LD_LIBRARY_PATH=${COMPSS_HOME}/Bindings/bindings-common/lib:${COMPSS_HOME}/Bindings/c/lib:${LD_LIBRARY_PATH}
	    export JVM_OPTIONS_FILE=${jvm_options_file}
    fi
  }

  post_launch() {
    # Do nothing
    :
  }

  clean_env() {
    if [ "${tracing}" == "true" ]; then
      unset LD_PRELOAD
      unset EXTRAE_HOME
      unset EXTRAE_LIB
      unset EXTRAE_CONFIG_FILE
      unset EXTRAE_USE_POSIX_CLOCK
      unset AFTER_EXTRAE_LD_PRELOAD
    fi
    if [ "$eraseWD" = "true" ]; then
      if [ "$debug" == "true" ]; then
        echo "[persistent_worker.sh] Clean WD ${workingDir}"
      fi
      rm -rf "${workingDir}"
      # Check if parent of workingDir (uuid) is empty. If empty remove it
      local parentdir="$(dirname "${workingDir}")"
      if [ -d "${parentdir}" ]; then
        if [ "$(ls -A ${parentdir})" ]; then
          if [ "$debug" == "true" ]; then
            echo "[persistent_worker.sh] Not Cleaning parent WD because not empty"
          fi
        else
          if [ "$debug" == "true" ]; then
            echo "[persistent_worker.sh] Cleaning parent WD"
          fi
          rm -rf ${parentdir}
        fi
      else
        if [ "$debug" == "true" ]; then
          echo "[persistent_worker.sh] Not Cleaning parent WD because doesn't exists"
        fi
      fi
      # Check if tmp directory of worker workingdir is empty. If empty remove it
      local tmpdir="$(dirname "${parentdir}")"
      if [ -d "${tmpdir}" ]; then
        if [ "$(ls -A ${tmpdir})" ]; then
          if [ "$debug" == "true" ]; then
            echo "[persistent_worker.sh] Not Cleaning tmp WD because not empty"
          fi
        else
          if [ "$debug" == "true" ]; then
            echo "[persistent_worker.sh] Cleaning tmp WD"
          fi
          rm -rf ${tmpdir}
        fi
      else
        if [ "$debug" == "true" ]; then
          echo "[persistent_worker.sh] Not Cleaning tmp WD because doesn't exists"
        fi
      fi
    else
      if [ "$debug" == "true" ]; then
        echo "[persistent_worker.sh] Not cleaning WD ${workingDir}"
      fi
    fi
  }
