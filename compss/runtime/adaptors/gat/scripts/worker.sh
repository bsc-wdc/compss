#!/bin/bash

  #-------------------------------------
  # Define script variables and exports
  #-------------------------------------
  scriptDir=$(dirname $0)

  #-------------------------------------
  # Get script parameters
  #-------------------------------------
  lang=$1
  workingDir=$2
  libPath=$3
  rmfilesNum=$4
  shift 4

  #-------------------------------------
  # Create sandbox
  #-------------------------------------
  if [ ! -d $workingDir ]; then
        /bin/mkdir -p $workingDir
  fi
  export IT_WORKING_DIR=$workingDir
  sandbox=$(/bin/mktemp -d -p $workingDir)
  cd $workingDir

  echo "** Start worker.sh"
  echo " - WorkingDir = $workingDir"
  echo " - LibPath    = $libPath"

  #-------------------------------------
  # Remove obsolete files
  #-------------------------------------
  for ((i=0;i<$rmfilesNum;i++)); do
	echo $1
 	rm -f $1
 	shift 1
  done

  #-------------------------------------
  # Get tracing status
  #-------------------------------------
  tracing=$1
  nodeName=$2
  shift 2

  #-------------------------------------
  # Set lib path
  #-------------------------------------
  if [ "$libPath" != "null" ]; then
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$libPath
  fi


  #-------------------------------------
  # Trace start event if needed
  #-------------------------------------
  export EXTRAE_BUFFER_SIZE=100
  if [ $tracing == "true" ]; then
    eventType=$1
    taskId=$2
    slot=$3
    shift 3
    $scriptDir/../../trace.sh start $workingDir $eventType $taskId $slot
  fi

  #-------------------------------------
  # Move to app dir and execute
  #-------------------------------------
  appDir=$1
  export IT_APP_DIR=$appDir

  # Add support for non-native tasks
  methodType=$6
  if [ "$methodType" != "METHOD" ]; then
    lang=java
  fi

  cd $sandbox
  # Run the task with the language-dependent script
  echo "** Starting language dependant script"
  $scriptDir/worker_$lang.sh $@
  endCode=$?
  echo "** EndStatus = $endCode"
  cd $workingDir

  #-------------------------------------
  # Trace end event if needed
  #-------------------------------------
  if [ $tracing == "true" ]; then
	$scriptDir/../../trace.sh end $workingDir $eventType $slot
  fi

  #-------------------------------------
  # Clean sandbox
  #-------------------------------------
  rm -rf $sandbox

  #-------------------------------------
  # Exit
  #-------------------------------------
  if [ $endCode -eq 0 ]; then
	exit 0
  else
	echo 1>&2 "Task execution failed"
	exit 7
  fi

