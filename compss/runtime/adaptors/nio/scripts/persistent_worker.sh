#!/bin/bash

  ######################
  # FUNCTIONS
  ######################
  add_to_classpath () {
    local DIRLIBS=${1}/*.jar
    for i in ${DIRLIBS}; do
      if [ "$i" != "${DIRLIBS}" ] ; then
        CLASSPATH=$CLASSPATH:"$i"
      fi
    done
  }


  ######################
  # MAIN PROGRAM
  ######################

  # Script Variables
  scriptDir=$(dirname $0)

  # Get parameters
  libPath=$1
  appDir=$2
  cp=$3

  # Shift parameters for script and leave only the NIOWorker parameters
  shift 3

  # Catch some NIOWorker parameters
  debug=$1
  workingDir=$2
  numThreads=$3
  hostName=$6
  worker_port=$7
  tracing=$9
  appUuid=${12}

  if [ "$debug" == "true" ]; then
    echo "PERSISTENT_WORKER.sh"
    echo "- libPath:    $libPath"
    echo "- AppDir:     $appDir"
    echo "- Classpath:  $cp"
    echo "- WorkingDir: $workingDir"
    echo "- HostName:   $hostName"
    echo "- WorkerPort: ${worker_port}"
    echo "- AppUUID:    ${appUuid}"
  fi

  # BLAUNCH start --------------------------------------------------------
  # Check that the current machine has not already awaken any WORKER in PORT and for app UUID
  worker_class="integratedtoolkit.nio.worker.NIOWorker"
  pid=$(ps -elfa | grep ${worker_class} | grep ${appUuid} | grep ${worker_port} | grep -v grep | awk '{print $4}')
  if [ "$pid" != "" ]; then
    if [ "$debug" == "true" ]; then
       echo "Worker already awaken. Nothing to do"
    fi
    echo $pid
    exit 0
  fi

  # Normal start -----------------------------------------------------
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

  # Prepare the worker command
  JAVA=java
  jvm_mem_xms=1024m
  jvm_mem_xmx=1024m
  jvm_mem_xmn=400m
  worker_jar=${scriptDir}/../../../../adaptors/nio/worker/compss-adaptors-nio-worker.jar
  main_worker_class=integratedtoolkit.nio.worker.NIOWorker
  cmd=$JAVA" \
    -Xms${jvm_mem_xms} \
    -Xmx${jvm_mem_xmx} \
    -Xmn${jvm_mem_xmn} \
    -XX:+HeapDumpOnOutOfMemoryError \
    -XX:+PerfDisableSharedMem \
    -XX:-UsePerfData \
    -XX:+UseG1GC \
    -XX:+UseThreadPriorities \
    -XX:ThreadPriorityPolicy=42 
    -classpath $CLASSPATH:$cp:${worker_jar} \
    ${main_worker_class}"

  if [ "$debug" == "true" ]; then
    echo "Calling NIOWorker"
    echo "Cmd: $cmd $*"
  fi

  cd $workingDir

  # Trace initialization
  if [ $tracing -gt 0 ]; then
        hostId=${10}
        extraeFile="extrae_basic.xml"
        if [ $tracing -gt 1 ]; then
            extraeFile="extrae_advanced.xml"
        fi
        export EXTRAE_HOME=${scriptDir}/../../../../../Dependencies/extrae/
        export EXTRAE_LIB=${EXTRAE_HOME}/lib
        export LD_LIBRARY_PATH=${EXTRAE_LIB}:${LD_LIBRARY_PATH}
        export EXTRAE_CONFIG_FILE=${scriptDir}/../../../../configuration/xml/tracing/${extraeFile}
        export LD_PRELOAD=${EXTRAE_HOME}/lib/libpttrace.so
  fi

  # Launch the Worker JVM
  nohup $cmd $* 1>$workingDir/log/worker_${hostName}.out 2> $workingDir/log/worker_${hostName}.err | echo $! &
  endCode=$?

  if [ $tracing -gt 0 ]; then
    unset LD_PRELOAD
  fi

  #Exit
  if [ $endCode -eq 0 ]; then
	exit 0
  else
	echo 1>&2 "Worker could not be initalized"
	exit 7
  fi
