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
    mkdir -p $workingDir
  fi
  export IT_WORKING_DIR=$workingDir
  
  cd $workingDir

  echo "[WORKER.SH] Starting GAT Worker"
  echo "[WORKER.SH]    - Lang       = $lang"
  echo "[WORKER.SH]    - WorkingDir = $workingDir"
  echo "[WORKER.SH]    - LibPath    = $libPath"

  #-------------------------------------
  # Remove obsolete files
  #-------------------------------------
  echo "[WORKER.SH] Removing $rmfilesNum obsolete files"
  for ((i=0;i<$rmfilesNum;i++)); do
    echo $1
    rm -f $1
    shift 1
  done
  
  #-------------------------------------
  # Managing Symlinks for files
  #-------------------------------------
  symlinkfilesNum=$1
  shift 1
  renames=""
  if [ $symlinkfilesNum -ne 0 ]; then
  	sandbox=$1
  	shift 1
  	if [ ! -d $sandbox ]; then
    	mkdir -p $sandbox
  	fi
  	echo "[WORKER.SH] Creating $symlinkfilesNum symlink files"
  	for ((i=0;i<$symlinkfilesNum;i=i+2)); do
    	#Create symlink for in inout files
    	if [ -f "$1" ]; then
    		echo "[WORKER.SH] Link $1 -> ${sandbox}/${2}"
    		ln -s $1 ${sandbox}/${2}
    	fi
    	# Add to treat after task management
    	if [ $i -eq 0 ]; then
    		renames="$1 ${sandbox}/$2"
    	else
    		renames="$renames $1 ${sandbox}/$2"
    	fi
    	shift 2 
  	done
  else
  	sandbox=$(mktemp -d -p $workingDir)
  fi
  
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
    echo "[WORKER.SH] Non-native task detected. Switching to JAVA invoker."
    lang=java
  fi

  cd $sandbox
  # Run the task with the language-dependent script
  echo " "
  echo "[WORKER.SH] Starting language $lang dependant script"
  $scriptDir/worker_$lang.sh $@
  endCode=$?
  echo " "
  echo "[WORKER.SH] EndStatus = $endCode"
  cd $workingDir

  #-------------------------------------
  # Trace end event if needed
  #-------------------------------------
  if [ $tracing == "true" ]; then
	$scriptDir/../../trace.sh end $workingDir $eventType $slot
  fi

  #-------------------------------------
  # Undo symlinks and rename files
  #-------------------------------------
  if [ $symlinkfilesNum -ne 0 ]; then
  	removeOrMove=0
  	renamedFile=""
  	echo "[WORKER.SH] Undo $symlinkfilesNum symlink files"
  	for element in $renames; do
    	#Check pair if firs
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
    			rm $element
    		elif [ $removeOrMove -eq 2 ]; then
    			echo "[WORKER.SH] Moving $element to $renamedFile"
    			mv $element $renamedFile
    		else
    			echo 1>&2 "Incorrect operation when managing rename symlinks "
				exit 7
			fi
    		removeOrMove=0
  			renamedFile=""
  		fi
   
  	done
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

