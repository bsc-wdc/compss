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
      pos=$((4+i))
      jvmFlags="${jvmFlags} ${!pos}"
    done
  
    # Shift parameters for script and leave only the NIOWorker parameters
    paramsToShift=$((4+numJvmFlags))
    shift ${paramsToShift}
    #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    #!!!! REMEMBER TO SHIFT THEM IN THE CALLER CODE 
    #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 
    # Catch some NIOWorker parameters
    debug=$1
    numThreads=$2
    hostName=$5
    worker_port=$6
    appUuid=$8
    workingDir=$9
    installDir=${10}
    tracing=${11}
    hostId=${12}
    
    if [ "$debug" == "true" ]; then
      echo "PERSISTENT_WORKER.sh"
      echo "- HostName:   $hostName"
      echo "- WorkerPort: ${worker_port}"
      echo "- WorkingDir: $workingDir"
      echo "- NumThreads: $numThreads"
      echo "- JVM Opts:   $jvmFlags"

      echo "- AppUUID:    ${appUuid}"
      echo "- AppDir:     $appDir"
      echo "- libPath:    $libPath"
      echo "- Classpath:  $cp"
      echo "- Tracing:    $tracing"
    fi
  }
  
  setup_environment(){
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
      -classpath $cp:$CLASSPATH:${worker_jar} \
      ${main_worker_class}"
  }
  
  pre_launch() {
    cd $workingDir
  
    # Trace initialization
    if [ $tracing -gt 0 ]; then
          local extraeFile="extrae_basic.xml"
          if [ $tracing -gt 1 ]; then
              extraeFile="extrae_advanced.xml"
          fi
          export EXTRAE_HOME=${scriptDir}/../../../../../Dependencies/extrae/
          export EXTRAE_LIB=${EXTRAE_HOME}/lib
          export LD_LIBRARY_PATH=${EXTRAE_LIB}:${LD_LIBRARY_PATH}
          export EXTRAE_CONFIG_FILE=${scriptDir}/../../../../configuration/xml/tracing/${extraeFile}
          export LD_PRELOAD=${EXTRAE_HOME}/lib/libpttrace.so
    fi
  }
  
  post_launch() {
    if [ $tracing -gt 0 ]; then
      unset LD_PRELOAD
    fi
  }

