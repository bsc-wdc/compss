#
# This script defines common methods that can be sourced and executed
#

# shellcheck disable=SC2034

##########################
# HELPER FUNCTIONS
##########################

get_taskID () {
  taskID=$1;
  shift 1
  remainingParams="$@"
}
get_batch_parameters () {
  isBatch=$1
  shift 1
  remainingParams="$@"

  if [ "${isBatch}" = "true" ]; then
          programID=$batchID_var
          batchSlaveNodesNum=$(echo "$worker_nodes" | wc -w)
          # shellcheck disable=SC2206
          batchSlaveNodesList=($worker_nodes)
          echo "[WORKER_COMMONS.SH] is batch, reads slave nodes: ${batchSlaveNodesList[*]} given by batchScript.sh"
  else
        create_kill_script_interactive "$killScriptDir" "$killScriptPath" "$programID"
        programID=$$
  fi
  if [ -z "${programID}" ]; then
        programID="NOT_GIVEN"
  fi

}

get_response_parameters () {
  responseDir=$1
  responsePath="${responseDir}/${taskID}"
  killScriptDir=$2
  killScriptPath="${killScriptDir}/${taskID}"
  shift 2

  #if [ isBatch = "true" ]; then
      #programID=$(cat "$responsePath" | awk '{print $1}')
  #fi

  mark_as_launch "$responseDir" "$responsePath" "$programID"

  responseFlags=("${responseDir}" "${responsePath}" "${killScriptDir}" "${killScriptPath}")
  remainingParams="$@"
}

get_host_parameters () {
    nodeName=$1
    if [ $isBatch = "true" ]; then
      nodeName=$master_node
    fi
    installDir=$2
    appDir=$3

    envScript=$4
    if [ "$envScript" != "null" ]; then
      source "$envScript"
    fi

    libPath=$5
    if [ "$libPath" != "null" ]; then
      export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$libPath
    fi

    workingDir=$6
    if [ ! -d "$workingDir" ]; then
      mkdir -p "$workingDir"
    fi
    export COMPSS_WORKING_DIR=$workingDir
    mkdir -p "$workingDir"/log
    mkdir -p "$workingDir"/jobs

    storageConf=$7
    streaming=$8
    streamingMasterName=$9
    streamingPort="${10}"
    debug=${11}

    if [ "${debug}" == "true" ]; then
        itlog4j_file=COMPSsWorker-log4j.debug
    else
        itlog4j_file=COMPSsWorker-log4j.off
    fi

    hostFlags=( "${debug}" "${nodeName}" "${workingDir}" "${installDir}" "${appDir}" "${envScript}"
                    "${storageConf}" "${streaming}" "${streamingMasterName}" "${streamingPort}")

    rmfilesNum=${12}
    obsoletes=${*:13:${rmfilesNum}}
    shift $((12 + rmfilesNum))

    tracing=$1
    export EXTRAE_BUFFER_SIZE=100
    runtimeEventType=$2
    sandBoxCreationId=$3
    sandBoxRemovalId=$4
    taskEventType=$5
    tracingTaskId=$6
    slot=$7
    tracingFlags=( "${tracing}" "${runtimeEventType}" "${sandBoxCreationId}" "${sandBoxRemovalId}"
                      "${taskEventType}" "${tracingTaskId}" "${slot}" )
    shift 7




    # shellcheck disable=SC2206
    remainingParams="$@"
    invocation=($@)
}

get_implementation_parameters() {
    # Get parameters

    persistentBinding=$1
    shift 1

    lang=$1
    taskSandboxWorkingDir=$2
    cp=$3
    pythonpath=$4
    pythonInterpreter=$5
    pythonVersion=$6
    pythonVirtualEnvironment=$7
    pythonPropagateVirtualEnvironment=$8
    pythonExtraeFile=$9
    provenance=${10}
    # Added to support coverage
    if [[ "${pythonInterpreter}" = coverage* ]]; then
         pythonInterpreter=$(echo ${pythonInterpreter} | tr "#" " " )
    fi
    langFlags=("${persistentBinding}" "${lang}" "${taskSandboxWorkingDir}" "${cp}" "${pythonpath}" "${pythonInterpreter}"
                  "${pythonVersion}" "${pythonVirtualEnvironment}" "${pythonPropagateVirtualEnvironment}"
                  "${pythonExtraeFile}" "${provenance}")
    # Shit all parameters except method ones
    shift 9



    implType=$1
    #${arr[@]:s:n}	Retrieve n elements starting at index s
    specificSandbox=false;
     case "${implType}" in
          "METHOD" | "MULTI_NODE")
            implNumArguments=2
            implArguments=(${@:2:$implNumArguments});;
          "CONTAINER")
            implNumArguments=10
            implArguments=(${@:2:$implNumArguments});;
          "COMPSs")
            implNumArguments=7
            implArguments=(${@:2:$implNumArguments});;
          "PYTHON_MPI")
            implNumArguments=6
            implArguments=(${@:2:$implNumArguments});;
          "MPI")
            implNumArguments=8;
            implArguments=(${@:2:$implNumArguments})
            if [ ! -z "${implArguments[2]}" ] && [ "${implArguments[2]}" != "null" ]; then
              specificSandbox=true;
            fi ;;
          "DECAF")
            implNumArguments=6;
            implArguments=(${@:2:$implNumArguments})
            if [ ! -z "${implArguments[4]}" ] && [ "${implArguments[4]}" != "null" ]; then
              specificSandbox=true;
            fi ;;
          "OMPSS")
            implNumArguments=3;
            implArguments=(${@:2:$implNumArguments})
            if [ ! -z "${implArguments[1]}" ] && [ "${implArguments[1]}" != "null" ]; then
              specificSandbox=true;
            fi ;;
          "OPENCL")
            implNumArguments=2;
            implArguments=(${@:2:$implNumArguments})
            if [ ! -z "${implArguments[1]}" ] && [ "${implArguments[1]}" != "null" ]; then
              specificSandbox=true;
            fi ;;
          "BINARY")
            implNumArguments=4;
            implArguments=(${@:2:$implNumArguments})
            if [ ! -z "${implArguments[1]}" ] && [ "${implArguments[1]}" != "null" ]; then
              specificSandbox=true;
            fi ;;
          *)
            echo "[WORKER_COMMONS.SH] NOT supported implementation type: $implType"
            mark_as_fail "$responseDir" "$responsePath" "$programID"
            exit 1
            ;;
    esac

    implFlags=("${implType}" "${implArguments[@]}" )
    shift $((1+$implNumArguments))
    # Get method parameters
    # shellcheck disable=SC2206
    remainingParams="$@"
}


get_invocation_params () {
    jobId=$1
    taskId=$2
    jobHistory=$3
    timeout=$4
    onFailure=$5
    shift 5

    numSlaves=$1
    # shellcheck disable=SC2206
    slaves=(${@:2:${numSlaves}}) #saves parameters from $5 to $5+$numSlaves to slaves
    shift $((1 + numSlaves)) #removes the slaves and previous parameters from the parameters

    if [ $isBatch = "true" ]; then
        #if is batch, slave nodes are incorrectly named thus it should check variable
        numSlaves=$batchSlaveNodesNum;
        slaves=(${batchSlaveNodesList[*]});
    fi

    if [ $numSlaves -gt 0 ]; then
        sshOptions="-o StrictHostKeyChecking=no -o BatchMode=yes -o ChallengeResponseAuthentication=no -p 22"
        for singleSlave in $slaves ; do
            command="ssh ${sshOptions} ${singleSlave} source ${envScript}"
            eval $command
        done
    fi

    cus=$1
    cpuMap=$2
    gus=$3
    gpuMap=$4
    fpgaus=$5
    fpgaMap=$6

    shift 6

    numParams=$1
    hasTarget=$2
    numResults=$3

    shift 3

    # shellcheck disable=SC2206
    remainingParams="$@"
    # shellcheck disable=SC2206
    taskParams="$@"

    invocationFlags=( "${jobId}" "${taskId}" "${jobHistory}" "${timeout}" "${onFailure}" "$numSlaves" "${slaves[@]}" "${cus}"
                          "${cpuMap}" "${gus}" "${gpuMap}" "${fpgaus}" "${fpgaMap}" "${numParams}"
                          "${hasTarget}" "${numResults}" "${taskParams[@]}")

}

add_to_classpath () {
    local DIRLIBS="${1}/*.jar"
    for i in ${DIRLIBS}; do
      if [ "$i" != "${DIRLIBS}" ] ; then
        CLASSPATH=$CLASSPATH:"$i"
      fi
    done
}



printAllParams(){
    printBatchParams
    printResponseParams
    printHostParams
    printImplementationParams
    printInvocationParams
}

printBatchParams(){
    echo "[WORKER_COMMONS.SH] Batch Parameters ----------------------------"
    echo "[WORKER_COMMONS.SH]         - isBatch                            = ${isBatch}"
    echo "[WORKER_COMMONS.SH]         - programID                          = ${programID}"
}

printResponseParams(){
    echo "[WORKER_COMMONS.SH] Response Parameters --------------------------"
    echo "[WORKER_COMMONS.SH]         - responseDir                        = ${responseDir}"
    echo "[WORKER_COMMONS.SH]         - responsePath                       = ${responsePath}"
    echo "[WORKER_COMMONS.SH]         - killScriptDir                      = ${killScriptDir}"
    echo "[WORKER_COMMONS.SH]         - killScriptPath                     = ${killScriptPath}"
    echo ${responseFlags[*]}
}

printHostParams(){
    echo "[WORKER_COMMONS.SH] Host Parameters    ---------------------------"
    echo "[WORKER_COMMONS.SH]         - Node name                          = ${nodeName}"
    echo "[WORKER_COMMONS.SH]         - Installation Directory             = ${installDir}"
    echo "[WORKER_COMMONS.SH]         - Application path                   = ${appDir}"
    echo "[WORKER_COMMONS.SH]         - Env Script                         = ${envScript}"
    echo "[WORKER_COMMONS.SH]         - LibPath                            = ${libPath}"
    echo "[WORKER_COMMONS.SH]         - Working Directory                  = ${workingDir}"
    echo "[WORKER_COMMONS.SH]         - Storage Configuration              = ${storageConf}"
    echo "[WORKER_COMMONS.SH]         - Streaming Backend                  = ${streaming}"
    echo "[WORKER_COMMONS.SH]         - Streaming Master Node              = ${streamingMasterName}"
    echo "[WORKER_COMMONS.SH]         - Streaming Port                     = ${streamingPort}"
    echo "[WORKER_COMMONS.SH]         - rmfilesNum                         = ${rmfilesNum}"
    echo "[WORKER_COMMONS.SH]         - debug                              = ${debug}"

    if [ "${rmfilesNum}" -gt 0 ]; then
          echo "[WORKER_COMMONS.SH]         - obsolete Files                     = ${obsoletes[*]}"
    fi
    echo ${hostFlags[*]}

    printTracingParams

}

printTracingParams(){
  echo "[WORKER_COMMONS.SH] Tracing Parameters ---------------------------"
    echo "[WORKER_COMMONS.SH]         - Tracing                            = ${tracing}"
    echo "[WORKER_COMMONS.SH]         - Tracing runtimeEventType           = ${runtimeEventType}"
    echo "[WORKER_COMMONS.SH]         - Tracing sandBoxCreationId          = ${sandBoxCreationId}"
    echo "[WORKER_COMMONS.SH]         - Tracing sandBoxRemovalId           = ${sandBoxRemovalId}"
    echo "[WORKER_COMMONS.SH]         - Tracing taskEventType              = ${taskEventType}"
    echo "[WORKER_COMMONS.SH]         - Tracing taskId                     = ${tracingTaskId}"
    echo "[WORKER_COMMONS.SH]         - Tracing slot                       = ${slot}"
    echo ${tracingFlags[*]}
}



printLangParams(){
  echo "[WORKER_COMMONS.SH] Language Parameters ---------------------------"
  echo "[WORKER_COMMONS.SH]         - persistentBinding                  = ${persistentBinding}"
  echo "[WORKER_COMMONS.SH]         - lang                               = ${lang}"
  echo "[WORKER_COMMONS.SH]         - taskSandboxWorkingDir              = ${taskSandboxWorkingDir}"
  echo "[WORKER_COMMONS.SH]         - appDir                             = ${appDir}"
  echo "[WORKER_COMMONS.SH]         - javaClasspath                      = ${cp}"
  echo "[WORKER_COMMONS.SH]         - pythonpath                         = ${pythonpath}"
  echo "[WORKER_COMMONS.SH]         - pythonInterpreter                  = ${pythonInterpreter}"
  echo "[WORKER_COMMONS.SH]         - pythonVersion                      = ${pythonVersion}"
  echo "[WORKER_COMMONS.SH]         - pythonVirtualEnvironment           = ${pythonVirtualEnvironment}"
  echo "[WORKER_COMMONS.SH]         - pythonPropagateVirtualEnvironment  = ${pythonPropagateVirtualEnvironment}"
  echo "[WORKER_COMMONS.SH]         - pythonExtraeFile                   = ${pythonExtraeFile}"
  echo "[WORKER_COMMONS.SH]         - provenance                         = ${provenance}"
  echo "${langFlags[*]}"

}

printImplementationParams(){
  printLangParams

  echo "[WORKER_COMMONS.SH] Implementation Parameters ---------------------"
  echo "[WORKER_COMMONS.SH]         - implType                           = ${implType}"
  echo "[WORKER_COMMONS.SH]         - implementation numParams           = ${implNumArguments}"
  echo "[WORKER_COMMONS.SH]         - specificSandbox                    = ${specificSandbox}"
  echo "[WORKER_COMMONS.SH]         - implementation args                = ${implArguments[*]}"
  case "${implType}" in
      "METHOD")
        echo "[WORKER_COMMONS.SH]         - class name                         = ${implArguments[0]}"
        echo "[WORKER_COMMONS.SH]         - method name                        = ${implArguments[1]}"
        ;;
      "MPI")
        echo "[WORKER_COMMONS.SH]         - mpi                                = ${implArguments[0]}"
        echo "[WORKER_COMMONS.SH]         - mpi binary                         = ${implArguments[1]}"
        echo "[WORKER_COMMONS.SH]         - sandbox                            = ${implArguments[2]}"
        ;;
      "COMPSs")
        echo "[WORKER_COMMONS.SH]         - compss exec                        = ${implArguments[0]}"
        echo "[WORKER_COMMONS.SH]         - compss flags                       = ${implArguments[1]}"
        echo "[WORKER_COMMONS.SH]         - appName                            = ${implArguments[2]}"
        echo "[WORKER_COMMONS.SH]         - appParams                          = ${implArguments[3]}"
        echo "[WORKER_COMMONS.SH]         - workerInMaster                     = ${implArguments[4]}"
        echo "[WORKER_COMMONS.SH]         - workingDir                         = ${implArguments[5]}"
        echo "[WORKER_COMMONS.SH]         - failByEV                           = ${implArguments[6]}"
        ;;
      "DECAF")
        echo "[WORKER_COMMONS.SH]         - Decaf dfScript                     = ${implArguments[0]}"
        echo "[WORKER_COMMONS.SH]         - Decaf dfExecutor                   = ${implArguments[1]}"
        echo "[WORKER_COMMONS.SH]         - Decaf dfLib                        = ${implArguments[2]}"
        echo "[WORKER_COMMONS.SH]         - mpi runner                         = ${implArguments[3]}"
        echo "[WORKER_COMMONS.SH]         - sandbox                            = ${implArguments[4]}"
        ;;
      "OMPSS")
        echo "[WORKER_COMMONS.SH]         - ompss binary                       = ${implArguments[0]}"
        echo "[WORKER_COMMONS.SH]         - sandbox                            = ${implArguments[1]}"
        ;;
      "OPENCL")
        echo "[WORKER_COMMONS.SH]         - opencl kernel                      = ${implArguments[0]}"
        echo "[WORKER_COMMONS.SH]         - sandbox                            = ${implArguments[1]}"
        ;;
      "BINARY")
        echo "[WORKER_COMMONS.SH]         - binary                             = ${implArguments[0]}"
        echo "[WORKER_COMMONS.SH]         - sandbox                            = ${implArguments[1]}"
        ;;
      *)
        echo 1>&2 "Unsupported implementation Type ${implType}"
        exit 7
        ;;
    esac
    echo "${implFlags[*]}"
}


printInvocationParams(){
  echo "[WORKER_COMMONS.SH] Invocation Parameters     ---------------------"
  echo "[WORKER_COMMONS.SH]         - jobId                              = ${jobId}"
  echo "[WORKER_COMMONS.SH]         - taskId                             = ${taskId}"
  echo "[WORKER_COMMONS.SH]         - history                            = ${jobHistory}"
  echo "[WORKER_COMMONS.SH]         - timeout                            = ${timeout}"
  echo "[WORKER_COMMONS.SH]         - onFailure                          = ${onFailure}"
  echo "[WORKER_COMMONS.SH]         - numSlaves                          = ${numSlaves}"
    if [ "${numSlaves}" -gt 0 ]; then
      for inx in ${!slaves[@]}; do
        echo "[WORKER_COMMONS.SH]              - slave $inx node                  = ${slaves[inx]}"
      done
    fi
  echo "[WORKER_COMMONS.SH]         - cpuMap                             = ${cpuMap}"
  echo "[WORKER_COMMONS.SH]         - cus                                = ${cus}"
  echo "[WORKER_COMMONS.SH]         - gpuMap                             = ${gpuMap}"
  echo "[WORKER_COMMONS.SH]         - gus                                = ${gus}"
  echo "[WORKER_COMMONS.SH]         - fpgaMap                            = ${fpgaMap}"
  echo "[WORKER_COMMONS.SH]         - fpgaus                             = ${fpgaus}"
  echo "[WORKER_COMMONS.SH]         - numParams                          = ${numParams}"
  echo "[WORKER_COMMONS.SH]         - hasTarget                          = ${hasTarget}"
  echo "[WORKER_COMMONS.SH]         - numResults                         = ${numResults}"
  echo "[WORKER_COMMONS.SH]         - task parameters                    = ${taskParams[*]}"
  echo "${invocationFlags[*]}"
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
      export EXTRAE_HOME=${SCRIPT_DIR}/../../../../../Dependencies/extrae/
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
          echo "[  INFO] LD_LIBRARY_PATH not defined set to LIBRARY_PATH"
      fi
  fi

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
  #added
  genCoredump=false
  if [ "$genCoredump" == "true" ]; then
      ulimit -c unlimited
  fi

  # Export environment
  #added
  #export CLASSPATH=$cpNW:$CLASSPATH
  COMPSS_HOME="${SCRIPT_DIR}/../../../../.."
  export PYTHONPATH=$pythonpath:$PYTHONPATH:${COMPSS_HOME}/Dependencies/threadpoolctl/
  #export LD_LIBRARY_PATH=$libPathNW:${SCRIPT_DIR}/../../../../../Bindings/bindings-common/lib:${SCRIPT_DIR}/../../../../../Bindings/c/lib:$LD_LIBRARY_PATH
}

get_command(){
  # Prepare the worker command
    local JAVA="java"
    local worker_jar=${SCRIPT_DIR}/../../../../adaptors/gos/worker/compss-adaptors-gos-worker.jar
    local main_worker_class=es.bsc.compss.gos.worker.GOSWorker

    perf_jvm_flags="-XX:+PerfDisableSharedMem -XX:-UsePerfData -XX:+UseG1GC"

    compss_jvm_flags="-Dlog4j.configurationFile=${installDir}/Runtime/configuration/log/${itlog4j_file} \
    -Dcompss.streaming=${streaming} \
    -Dcompss.python.interpreter=${pythonInterpreter} \
    -Dcompss.python.version=${pythonVersion} \
    -Dcompss.python.virtualenvironment=${pythonVirtualEnvironment} \
    -Dcompss.python.propagate_virtualenvironment=${pythonPropagateVirtualEnvironment} \
    -Dcompss.extrae.file.python=${pythonExtraeFile} \
    -Dcompss.data_provenance=${provenance}"
     #\
    #-Djava.library.path=$LD_LIBRARY_PATH"

    #added
    jvmFlags=""

    if [ "$(uname -m)" == "riscv64" ]; then
        worker_jvm_flags="${jvmFlags} ${compss_jvm_flags}"
    else
        worker_jvm_flags="${jvmFlags} ${perf_jvm_flags} ${compss_jvm_flags}"
    fi

    if [ "$lang" = "c" ] && [ "${persistentBinding}" = "true" ]; then
      generate_jvm_opts_file
        # shellcheck disable=SC2034
      cmd="${appDir}/worker/gos_worker_c"
    else
        # shellcheck disable=SC2034
        cmd="$JAVA ${worker_jvm_flags} -classpath $cp:${worker_jar} ${main_worker_class}"
        cmd="$cmd ${hostFlags[*]} ${tracingFlags[*]} ${langFlags[*]} ${implFlags[*]} ${invocationFlags[*]}"
    fi
}

set_env() {
    local bindingsDir
    bindingsDir=$(dirname "$0")/../../../../../Bindings
    # Set LD_LIBRARY_PATH related env
    export LD_LIBRARY_PATH=${bindingsDir}/c/lib:${bindingsDir}/bindings-common/lib:$LD_LIBRARY_PATH

    # Activate bindings debug if debug activated
    if [ "$debug" == "true" ]; then
      export COMPSS_BINDINGS_DEBUG=1
    fi

    # Look for the JVM Library
    if [ -n "${JAVA_HOME}" ]; then
      if [ -d "${JAVA_HOME}/jre/lib/" ]; then #Java 8 case
        libjava=$(find "${JAVA_HOME}"/jre/lib/ -name libjvm.so | head -n 1)
        if [ -z "$libjava" ]; then
          libjava=$(find "${JAVA_HOME}"/jre/lib/ -name libjvm.dylib | head -n 1)
          if [ -z "$libjava" ]; then
            echo "WARNNING: Java lib dir not found."
          fi
        fi
      else # Java 9+
        libjava=$(find "${JAVA_HOME}"/lib/ -name libjvm.so | head -n 1)
        if [ -z "$libjava" ]; then
          libjava=$(find "${JAVA_HOME}"/lib/ -name libjvm.dylib | head -n 1)
          if [ -z "$libjava" ]; then
            echo "WARNNING: Java lib dir not found."
          fi
        fi
      fi

      if [ -n "$libjava" ]; then
        libjavafolder=$(dirname "$libjava")
        export LD_LIBRARY_PATH=${libjavafolder}:$LD_LIBRARY_PATH
      fi
    fi

    # Set classpath related env
    local gosworker_jar
    gosworker_jar=$(dirname "$0")/../../../../adaptors/gos/worker/compss-adaptors-gos-worker.jar
    add_to_classpath "$app_dir"
    add_to_classpath "$app_dir/lib"
    export CLASSPATH=$cp:$CLASSPATH:$app_dir:$gosworker_jar

    # Set python home related env
    export PYCOMPSS_HOME=${bindingsDir}/python
}

compute_generic_sandbox () {
    sandbox="${workingDir}/sandBox/job_${jobId}/"
}

numRenames=0
moveFileToSandbox () {
    if [ -f "$1" ]; then
        if [ ! -f "${sandbox}/${2}" ]; then
          echo "[WORKER_COMMONS.SH] Link ${1} -> ${sandbox}/${2}"
      	  ln -s "${1}" "${sandbox}/${2}"
        else
          newVer=$(basename "$1" | tr "_" "\\t" | awk '{ print $1 }' | tr "v" "\\t" | awk '{ print $2 }')
          oldVer=$(basename readlink -f "${sandbox}/${2}" | tr "_" "\\t" | awk '{ print $1 }' | tr "v" "\\t" | awk '{ print $2 }')
          if (( newVer > oldVer )); then
            ln -sf "$1" "${sandbox}/${2}"
            echo "[WORKER_COMMONS.SH] WARN: Updating link ${sandbox}/$2 that already exists"
          else
            echo "[WORKER_COMMONS.SH] WARN: Cannot create link because ${sandbox}/$2 already exists"
          fi
        fi
      else
        echo "[WORKER_COMMONS.SH] WARN: Cannot create link because $1 doesn't exists"
      fi

      # Add to treat after task management
      if [ ${numRenames} -eq 0 ]; then
        renames="$1 ${sandbox}/$2"
        numRenames=1
      else
        renames="$renames $1 ${sandbox}/$2"
      fi
}

moveFilesOutFromSandbox () {
    removeOrMove=0
    renamedFile=""
    for element in $renames; do
      # Check pair if first
      if [ $removeOrMove -eq 0 ]; then
        if [ -f "$element" ]; then
    	    removeOrMove=1
    	  else
    	    removeOrMove=2
    	    renamedFile=$element
    	  fi
      else
        if [ $removeOrMove -eq 1 ]; then
          echo "[WORKER_COMMONS.SH] Removing link $element"
          if [ -f "$element" ]; then
            rm "${element}"
          fi
    	elif [ $removeOrMove -eq 2 ]; then
          echo "[WORKER_COMMONS.SH] Moving $element to $renamedFile"
          if [ -f "$element" ]; then
            mv "${element}" "${renamedFile}"
          fi
    	else
    	  echo 1>&2 "Incorrect operation when managing rename symlinks "
          exit 7
        fi
        removeOrMove=0
        renamedFile=""
      fi
    done
}


get_all_parameters(){
  remainingParams="$@"

  #Changes if debug is true
  itlog4j_file=COMPSsWorker-log4j.off

  get_taskID ${remainingParams[@]}
  get_batch_parameters ${remainingParams[@]}
  get_response_parameters ${remainingParams[@]}
  get_host_parameters ${remainingParams[@]}
  get_implementation_parameters ${remainingParams[@]}
  get_invocation_params ${remainingParams[@]}

}
