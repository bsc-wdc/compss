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
    scriptDir=$(dirname $0)
  
    # Get parameters
    libPath=$1
    appDir=$2
    cp=$3
    numJvmFlags=$4

    jvmFlags=""
    for i in $(seq 1 $numJvmFlags); do
      pos=$((4 + i))
      jvmFlags="${jvmFlags} ${!pos}"
    done
  
    # Shift parameters for script and leave only the NIOWorker parameters
    paramsToShift=$((4 + numJvmFlags))
    shift ${paramsToShift}
    paramsToCOMPSsWorker="${*:1:21}"
 
    # Catch some NIOWorker parameters
    debug=$1
    numThreads=$2
    hostName=$5
    worker_port=$6
    appUuid=$8
    lang=$9
    workingDir=${10}
    installDir=${11}
    appDirNW=${12}
    libPathNW=${13}
    cpNW=${14}
    pythonpath=${15}
    tracing=${16}
    extraeFile=${17}
    hostId=${18}
    gpus=${21}
    amountSockets=${*:22:1}
    socketString=${*:23:1}
    #No data set by the user
    if [ "$amountSockets" == "" -o "$amountSockets" == "0" ]; then
        amountSockets=$(lscpu | grep "Socket(s)" | awk '{ print $2 }')
        CoresPerSocket=$(lscpu | grep "Core(s) per socket" | awk '{ print $4 }')
        CUperSocket=$(($(lscpu | grep "Thread(s) per core" | awk '{ print $4 }') * $CoresPerSocket))
        socketString="0-"$(($CUperSocket - 1))
        for i in $(seq 1 $(($amountSockets - 1))); do
            socketString=$socketString"/"$((i * CUperSocket))"-"$(($((i + 1)) * CUperSocket - 1))
        done
    fi   
    #No data set by the user and lscpu didn't manage to get the good architecture
    #It is assumed that the amount of threads specified in the resources file is equal to the number of cores in theworker
    if [ "$amountSockets" == "" -o "$amountSockets" == "" ]; then
        amountSockets="1"
        realAmountThreads=$(lscpu | grep "CPU(s)" | grep -v -E ",|-" | awk '{ print $2 }')
        socketString="0-"$(($realAmountThreads - 1))
    fi 

    paramsToCOMPSsWorker="$paramsToCOMPSsWorker $amountSockets $socketString"

    if [ "$debug" == "true" ]; then
      echo "PERSISTENT_WORKER.sh"
      echo "- HostName:          $hostName"
      echo "- WorkerPort:        ${worker_port}"
      echo "- WorkingDir:        $workingDir"
      echo "- InstallDir:        $installDir"
      echo "- NumThreads:        $numThreads"
      echo "- Gpus/node:         $gpus"
      echo "- Amount of sockets: $amountSockets"
      echo "- Socket string:     $socketString"
      echo "- JVM Opts:          $jvmFlags"

      echo "- AppUUID:           ${appUuid}"
      echo "- Lang:              ${lang}"
      echo "- AppDir:            $appDirNW"
      echo "- libPath:           $libPathNW"
      echo "- Classpath:         $cpNW"
      echo "- Pythonpath:        $pythonpath"
      echo "- Tracing:           $tracing"
      echo "- ExtraeFile:        ${extraeFile}"
    fi

    # Calculate Log4j file
    if [ "${debug}" == "true" ]; then
      itlog4j_file=COMPSsWorker-log4j.debug
    else
      itlog4j_file=COMPSsWorker-log4j.off
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
    if [ ! -d $workingDir ]; then
  	/bin/mkdir -p $workingDir
    fi
    export IT_WORKING_DIR=$workingDir
    mkdir -p $workingDir/log
    mkdir -p $workingDir/jobs
  
    # Set lib path
    if [ "$libPath" != "null" ]; then
  	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$libPath
    fi
  
    # Set appDir
    export IT_APP_DIR=$appDir
    if [ "$appDir" != "null" ]; then
    	add_to_classpath "$appDir"
    	add_to_classpath "$appDir/lib"
    fi
  
    # Set the classpath
    if [ "$cp" == "null" ]; then
  	cp=""
    fi
  }
  
  setup_jvm() {
    # Prepare the worker command
    local JAVA=java
    local worker_jar=${scriptDir}/../../../../adaptors/nio/worker/compss-adaptors-nio-worker.jar
    local main_worker_class=integratedtoolkit.nio.worker.NIOWorker
    cmd=$JAVA" \
      ${jvmFlags} \
      -XX:+PerfDisableSharedMem \
      -XX:-UsePerfData \
      -XX:+UseG1GC \
      -XX:+UseThreadPriorities \
      -XX:ThreadPriorityPolicy=42 \
      -Dlog4j.configurationFile=${installDir}/Runtime/configuration/log/${itlog4j_file} \
      -classpath $cp:$CLASSPATH:${worker_jar} \
      ${main_worker_class}"
  }
  
  pre_launch() {
    cd $workingDir
  
    # Trace initialization
    if [ $tracing -gt 0 ]; then
      if [ -z "${extraeFile}" ] || [ "${extraeFile}" == "null" ]; then
        # Only define extraeFile if it is not a custom location
        extraeFile=${scriptDir}/../../../../configuration/xml/tracing/extrae_basic.xml
        if [ $tracing -gt 1 ]; then
          extraeFile=${scriptDir}/../../../../configuration/xml/tracing/extrae_advanced.xml
        fi
      fi
      
      if [ -z "$EXTRAE_HOME" ]; then
        export EXTRAE_HOME=${scriptDir}/../../../../../Dependencies/extrae/
      fi
      
      export EXTRAE_LIB=${EXTRAE_HOME}/lib
      export LD_LIBRARY_PATH=${EXTRAE_LIB}:${LD_LIBRARY_PATH}
      export EXTRAE_CONFIG_FILE=${extraeFile}
      export LD_PRELOAD=${EXTRAE_HOME}/lib/libpttrace.so
    fi
  }
  
  post_launch() {
    if [ $tracing -gt 0 ]; then
      unset LD_PRELOAD
    fi
  }

  clean_env() {
    echo "[persistent_worker.sh] Clean WD ${workingDir}"
    rm -rf ${workingDir}
  }
