#!/bin/bash
  JAVA_JRE_ERROR="ERROR: Can't find JVM libraries in JAVA_HOME. Please check your Java JRE Installation."

  NUM_PARAMS=39

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

    if [ "$debug" == "true" ]; then
      echo "PERSISTENT_WORKER.sh"
      echo "- HostName:            $hostName"
      echo "- WorkerPort:          ${worker_port}"
      echo "- WorkingDir:          $workingDir"
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
          echo "[persistent_worker.sh] Not Cleaning parent WD because doesn't exists"
        fi
      fi
    else
      if [ "$debug" == "true" ]; then
        echo "[persistent_worker.sh] Not cleaning WD ${workingDir}"
      fi
    fi
  }
