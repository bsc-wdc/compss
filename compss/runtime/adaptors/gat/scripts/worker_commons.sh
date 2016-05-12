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
    app_dir=$1
    cp=$2
    pythonpath=$3
    debug=$4
    app=$5
    method=$6
    has_target=$7
    nparams=$8

    # Shit all parameters except method ones
    shiftSizeForApp=3
    shiftSizeForTaskParams=8
    shift $shiftSizeForTaskParams
   
    # Get method parameters
    params=$@

    # Log status if needed
    if [ "$debug" == "true" ]; then
      echo "app         $app"
      echo "app_dir     $app_dir"
      echo "classpath   $cp"
      echo "pythonpath  $pythonpath"
      echo "method      $method"
      echo "has_target  $has_target"
      echo "nparams     $nparams"
      echo "params      $params"
    fi
  }

  set_env() {
    local bindingsDir=$(dirname $0)/../../../../../Bindings

    # Set LD_LIBRARY_PATH related env
    export LD_LIBRARY_PATH=${bindingsDir}/c/lib:${bindigsDir}/bindings-common/lib:$LD_LIBRARY_PATH

    # Set classpath related env
    local gatworker_jar=$(dirname $0)/../../../../adaptors/gat/worker/compss-adaptors-gat-worker.jar
    add_to_classpath "$app_dir"
    add_to_classpath "$app_dir/lib"
    export CLASSPATH=$cp:$CLASSPATH:$app_dir:$gatworker_jar

    # Set pythonpath related env
    export PYCOMPSS_HOME=${bindingsDir}/python
    export PYTHONPATH=$pythonpath:$app_dir:$PYCOMPSS_HOME:$PYTHONPATH
  }

