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
  isSpecific=$1
  sandbox=$2
  shift 2
  echo "[WORKER.SH]    - Sandbox    = $sandbox"
  echo "[WORKER.SH]    - isSpecific = $isSpecific"
  if [ ! -d $sandbox ]; then
    mkdir -p $sandbox
  fi
  
  symlinkfilesNum=$1
  shift 1
  renames=""
  if [ $symlinkfilesNum -ne 0 ]; then
    echo "[WORKER.SH] Creating $symlinkfilesNum symlink files"
    for ((i=0;i<$symlinkfilesNum;i=i+2)); do
      # Create symlink for in inout files
      if [ -f "$1" ]; then
        if [ ! -f "${sandbox}/$2" ]; then
          echo "[WORKER.SH] Link $1 -> ${sandbox}/${2}"
      	  ln -s $1 ${sandbox}/${2}
        else
          newVer=$(basename $1 | tr "_" "\t" | awk '{ print $1 }' | tr "v" "\t" | awk '{ print $2 }')
          oldVer=$(basename $(echo $(readlink -f ${sandbox}/${2})) | tr "_" "\t" | awk '{ print $1 }' | tr "v" "\t" | awk '{ print $2 }')
          if (( newVer > oldVer )); then
            ln -sf $1 ${sandbox}/${2}
            echo "[WORKER.SH] WARN: Updating link ${sandbox}/$2 that already exists"
          else
            echo "[WORKER.SH] WARN: Cannot create link because ${sandbox}/$2 already exists"
          fi
        fi
      else
        echo "[WORKER.SH] WARN: Cannot create link because $1 doesn't exists"
      fi
      
      # Add to treat after task management
      if [ $i -eq 0 ]; then
        renames="$1 ${sandbox}/$2"
      else
        renames="$renames $1 ${sandbox}/$2"
      fi
      shift 2 
    done
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
  methodType=$7
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
            rm $element
          fi
    	elif [ $removeOrMove -eq 2 ]; then
          echo "[WORKER.SH] Moving $element to $renamedFile"
          if [ -f "$element" ]; then
            mv $element $renamedFile
          fi
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
  if [ "${isSpecific}" != "true" ]; then
    rm -rf $sandbox
  fi

  #-------------------------------------
  # Exit
  #-------------------------------------
  if [ $endCode -eq 0 ]; then
    exit 0
  else
    echo 1>&2 "Task execution failed"
    exit 7
  fi

