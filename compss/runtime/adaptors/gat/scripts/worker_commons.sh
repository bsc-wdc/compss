#
# This script defines common methods that can be sourced and executed
#

# shellcheck disable=SC2034

##########################
# HELPER FUNCTIONS
##########################

get_host_parameters () {
    nodeName=$1
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
    storageConf=$7
    streaming=$8
    streamingMasterName=$9
    streamingPort=${10}
    debug=${11}
    rmfilesNum=${12}
    shift $((12 + rmfilesNum))

    tracing=$1
    shift 1
    export EXTRAE_BUFFER_SIZE=100
    if [ "${tracing}" == "true" ]; then
      runtimeEventType=$1
      sandBoxCreationId=$2
      sandBoxRemovalId=$3
      taskEventType=$4
      taskId=$5
      slot=$6
      shift 6
    fi
    hostFlags=( "${nodeName}" "${workingDir}" "${debug}" "${installDir}" "${appDir}" "${storageConf}" "${streaming}" "${streamingMasterName}" "${streamingPort}" )
    # shellcheck disable=SC2206
    invocation=($@)
}

get_invocation_params () {
    jobId=$1
    taskId=$2
    timeout=$3

    numSlaves=$4
    # shellcheck disable=SC2206
    slaves=(${@:5:${numSlaves}}) #saves parameters from $5 to $5+$numSlaves to slaves
    shift $((4 + numSlaves)) #removes the slaves and previos parameters from the parameters

    cus=$1
    numParams=$2
    hasTarget=$3
    numResults=$4

    shift 4
    # shellcheck disable=SC2206
    params=($@)
    # shellcheck disable=SC2206
    invocationParams=( "${jobId}" "${taskId}" "${timeout}" "$numSlaves" ${slaves[@]} "${cus}" "${numParams}" "${hasTarget}" "${numResults}" ${params[@]})
}

add_to_classpath () {
    local DIRLIBS="${1}/*.jar"
    for i in ${DIRLIBS}; do
      if [ "$i" != "${DIRLIBS}" ] ; then
        CLASSPATH=$CLASSPATH:"$i"
      fi
    done
}

get_parameters() {
    # Get parameters
    taskSandboxWorkingDir=$1
    app_dir=$2
    cp=$3
    pythonpath=$4
    pythonInterpreter=$5
    pythonVersion=$6
    pythonVirtualEnvironment=$7
    pythonPropagateVirtualEnvironment=$8
    pythonExtraeFile=$9
    debug=$9
    storageConf=${10}
    methodType=${11}

    # Shit all parameters except method ones
    shiftSizeForApp=8
    shift $shiftSizeForApp

    # Get method parameters
    # shellcheck disable=SC2206
    params=($@)

    # Log status if needed
    if [ "$debug" == "true" ]; then
      echo "[WORKER_COMMONS.SH] - app_dir                            $app_dir"
      echo "[WORKER_COMMONS.SH] - classpath                          $cp"
      echo "[WORKER_COMMONS.SH] - pythonpath                         $pythonpath"
      echo "[WORKER_COMMONS.SH] - pythonInterpreter                  $pythonInterpreter"
      echo "[WORKER_COMMONS.SH] - pythonVersion                      $pythonVersion"
      echo "[WORKER_COMMONS.SH] - pythonVirtualEnvironment           $pythonVirtualEnvironment"
      echo "[WORKER_COMMONS.SH] - pythonPropagateVirtualEnvironment  $pythonPropagateVirtualEnvironment"
      echo "[WORKER_COMMONS.SH] - pythonExtraeFile                   $pythonExtraeFile"
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
    local gatworker_jar
    gatworker_jar=$(dirname "$0")/../../../../adaptors/gat/worker/compss-adaptors-gat-worker.jar
    add_to_classpath "$app_dir"
    add_to_classpath "$app_dir/lib"
    export CLASSPATH=$cp:$CLASSPATH:$app_dir:$gatworker_jar

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
          echo "[WORKER.SH] Removing link $element"
          if [ -f "$element" ]; then
            rm "${element}"
          fi
    	elif [ $removeOrMove -eq 2 ]; then
          echo "[WORKER.SH] Moving $element to $renamedFile"
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
