#!/bin/bash

  ##########################
  # HELPER FUNCTIONS
  ##########################
  add_to_classpath () {
          DIRLIBS=${1}/*.jar
          for i in ${DIRLIBS}
          do
                  if [ "$i" != "${DIRLIBS}" ] ; then
                          CLASSPATH=$CLASSPATH:"$i"
                  fi
          done
  }


  ##########################
  # MAIN
  ##########################
  
  # Script variables
  script_dir=$(dirname $0)
  gatworker_jar=$script_dir/../../../../adaptors/gat/worker/compss-adaptors-gat-worker.jar

  # Get parameters
  app_dir=$1
  cp=$2
  debug=$3
  app=$4
  method=$5
  has_target=$6
  nparams=$7
  shift 7
  params=$*
  
  # LOG
  if [ "$debug" == "true" ]; then
      echo app_dir $app_dir
      echo classpath $cp
      echo debug $debug
      echo app $app
      echo method $method
      echo has_target $has_target
      echo nparams $nparams
      echo params $params
  fi
 
  # Add application to classpath     
  add_to_classpath "$app_dir"
  add_to_classpath "$app_dir/lib"
  
  # Launch the JVM to run the task
  java -Xms128m -Xmx2048m -classpath $cp:$CLASSPATH:$app_dir:$gatworker_jar integratedtoolkit.gat.worker.GATWorker $debug $app $method $has_target $nparams $params
 
  # Exit  
  if [ $? -eq 0 ]; then
        exit 0
  else
        echo 1>&2 "Task execution failed"
        exit 7
  fi

