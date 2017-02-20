#!/bin/bash

  ##########################
  # HELPER FUNCTIONS
  ##########################

  add_to_classpath () {
    local DIRLIBS=${1}/*.jar
    for i in ${DIRLIBS}; do
      if [ "$i" != "${DIRLIBS}" ] ; then
        CLASSPATH=$CLASSPATH:"$i"
      fi
    done
  }

  get_parameters() {
    # Get paramters
    taskSandboxWorkingDir=$1
    app_dir=$2
    cp=$3
    pythonpath=$4
    debug=$5
    storageConf=$6
    methodType=$7

    # Shit all parameters except method ones
    shiftSizeForApp=4
    shift $shiftSizeForApp
   
    # Get method parameters
    params=$@

    # Log status if needed
    if [ "$debug" == "true" ]; then
      echo "[WORKER_COMMONS.SH] - app_dir     $app_dir"
      echo "[WORKER_COMMONS.SH] - classpath   $cp"
      echo "[WORKER_COMMONS.SH] - pythonpath  $pythonpath"
    fi
  }

  set_env() {
    local bindingsDir=$(dirname $0)/../../../../../Bindings
	# Set LD_LIBRARY_PATH related env
    export LD_LIBRARY_PATH=${bindingsDir}/c/lib:${bindingsDir}/bindings-common/lib:$LD_LIBRARY_PATH
	
	# Look for the JVM Library
  	if [ -n "${JAVA_HOME}" ]; then
  		libjava=$(find ${JAVA_HOME}/jre/lib/ -name libjvm.so | head -n 1)
  		if [ -z "$libjava" ]; then
    		libjava=$(find ${JAVA_HOME}/jre/lib/ -name libjvm.dylib | head -n 1)
    		if [ -z "$libjava" ]; then
      			echo "WARNNING: Java lib dir not found."
    		fi
  		fi
  		if [ -n "$libjava" ]; then 
  			libjavafolder=$(dirname $libjava)
  			export LD_LIBRARY_PATH=${libjavafolder}:$LD_LIBRARY_PATH
		fi
	fi

    # Set classpath related env
    local gatworker_jar=$(dirname $0)/../../../../adaptors/gat/worker/compss-adaptors-gat-worker.jar
    add_to_classpath "$app_dir"
    add_to_classpath "$app_dir/lib"
    export CLASSPATH=$cp:$CLASSPATH:$app_dir:$gatworker_jar

    # Set pythonpath related env
    export PYCOMPSS_HOME=${bindingsDir}/python
    export PYTHONPATH=$pythonpath:$app_dir:$PYCOMPSS_HOME:$PYTHONPATH
  }

