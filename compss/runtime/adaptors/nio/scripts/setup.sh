#!/bin/bash

  ######################
  # INTERNAL FUNCTIONS
  ######################
  add_to_classpath () {
    local DIRLIBS=${1}/*.jar
    for i in ${DIRLIBS}; do
      if [ "$i" != "${DIRLIBS}" ] ; then
        CLASSPATH=$CLASSPATH:"$i"
      fi
    done
    export CLASSPATH=$CLASSPATH
  }


  ######################
  # COMMON HELPER FUNCTIONS
  ######################
  load_parameters() {
    # Script Variables
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
  
    # Get parameters
    libPath=$1
    appDir=$2
    cp=$3
    numJvmFlags=$4

    jvmFlags=""
    for i in $(seq 1 "$numJvmFlags"); do
      pos=$((4 + i))
      jvmFlags="${jvmFlags} ${!pos}"
    done
  
    # Shift parameters for script and leave only the NIOWorker parameters
    paramsToShift=$((4 + numJvmFlags))
    shift ${paramsToShift}
    paramsToCOMPSsWorker=$@
 
    # Catch some NIOWorker parameters
    debug=$1
    hostName=$4
    worker_port=$5
    cusCPU=$7
    cusGPU=$8
    lot=${11}
    appUuid=${12}
    lang=${13}
    workingDir=${14}
    installDir=${15}
    appDirNW=${16}
    libPathNW=${17}
    cpNW=${18}
    pythonpath=${19}
    tracing=${20}
    extraeFile=${21}
    hostId=${22}

    if [ "$debug" == "true" ]; then
      echo "PERSISTENT_WORKER.sh"
      echo "- HostName:            $hostName"
      echo "- WorkerPort:          ${worker_port}"
      echo "- WorkingDir:          $workingDir"
      echo "- InstallDir:          $installDir"

      echo "- Computing Units CPU: ${cusCPU}"
      echo "- Computing Units GPU: ${cusGPU}"
      echo "- Limit Of Tasks:      ${lot}"
      echo "- JVM Opts:            $jvmFlags"

      echo "- AppUUID:             ${appUuid}"
      echo "- Lang:                ${lang}"
      echo "- AppDir:              $appDirNW"
      echo "- libPath:             $libPathNW"
      echo "- Classpath:           $cpNW"
      echo "- Pythonpath:          $pythonpath"

      echo "- Tracing:             $tracing"
      echo "- ExtraeFile:          ${extraeFile}"
      echo "- HostId:              ${hostId}"
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
  }
  
  setup_environment(){
    # Added for SGE queue systems which do not allow to copy LD_LIBRARY_PATH
    if [ -z "$LD_LIBRARY_PATH" ]; then
        if [ -n "$LIBRARY_PATH" ]; then
            export LD_LIBRARY_PATH=$LIBRARY_PATH
            echo "[  INFO] LD_LIBRARY_PATH not defined set to LIBRARY_PATH"
        fi
    fi

    # Create sandbox
    if [ ! -d "$workingDir" ]; then
  	mkdir -p "$workingDir"
    fi
    export COMPSS_WORKING_DIR=$workingDir
    mkdir -p "$workingDir"/log
    mkdir -p "$workingDir"/jobs

    # Look for the JVM Library
    libjava=$(find "${JAVA_HOME}"/jre/lib/ -name libjvm.so | head -n 1)
    if [ -z "$libjava" ]; then
        libjava=$(find "${JAVA_HOME}"/jre/lib/ -name libjvm.dylib | head -n 1)
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

    # Export environment
    export CLASSPATH=$cpNW:$CLASSPATH
    export PYTHONPATH=$pythonpath:$PYTHONPATH
    export LD_LIBRARY_PATH=$libPathNW:$LD_LIBRARY_PATH
  }
  
  setup_jvm() {
    # Prepare the worker command
    local JAVA=java
    local worker_jar=${SCRIPT_DIR}/../../../../adaptors/nio/worker/compss-adaptors-nio-worker.jar
    local main_worker_class=es.bsc.compss.nio.worker.NIOWorker
    cmd=$JAVA" \
      ${jvmFlags} \
      -XX:+PerfDisableSharedMem \
      -XX:-UsePerfData \
      -XX:+UseG1GC \
      -XX:+UseThreadPriorities \
      -XX:ThreadPriorityPolicy=42 \
      -Dlog4j.configurationFile=${installDir}/Runtime/configuration/log/${itlog4j_file} \
      -classpath $CLASSPATH:${worker_jar} \
      ${main_worker_class}"
  }
  
  pre_launch() {
    cd "$workingDir" || exit 1
  
    # Trace initialization
    if [ "$tracing" -gt 0 ]; then
      if [ -z "${extraeFile}" ] || [ "${extraeFile}" == "null" ]; then
        # Only define extraeFile if it is not a custom location
        extraeFile=${SCRIPT_DIR}/../../../../configuration/xml/tracing/extrae_basic.xml
        if [ "$tracing" -gt 1 ]; then
          extraeFile=${SCRIPT_DIR}/../../../../configuration/xml/tracing/extrae_advanced.xml
        fi
      fi
      
      if [ -z "$EXTRAE_HOME" ]; then
        export EXTRAE_HOME=${SCRIPT_DIR}/../../../../../Dependencies/extrae/
      fi
      
      export EXTRAE_LIB=${EXTRAE_HOME}/lib
      export LD_LIBRARY_PATH=${EXTRAE_LIB}:${LD_LIBRARY_PATH}
      export EXTRAE_CONFIG_FILE=${extraeFile}
      export LD_PRELOAD=${EXTRAE_HOME}/lib/libpttrace.so
    fi
  }
  
  post_launch() {
    if [ "$tracing" -gt 0 ]; then
      unset LD_PRELOAD
    fi
  }

  clean_env() {
    if [ "$eraseWD" = "true" ]; then
      echo "[persistent_worker.sh] Clean WD ${workingDir}"
      rm -rf "${workingDir}"
    else
      echo "[persistent_worker.sh] Not cleaning WD ${workingDir}"
    fi
  }

