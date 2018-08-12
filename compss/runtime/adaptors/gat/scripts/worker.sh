#!/bin/bash
echo $0 $*
  #-------------------------------------
  # Define script variables and exports
  #-------------------------------------
  if [ -z "${COMPSS_HOME}" ]; then
    scriptDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    export COMPSS_HOME=${scriptDir}/../../../../../
  else
    scriptDir="${COMPSS_HOME}/Runtime/scripts/system/adaptors/gat"
  fi

  # shellcheck source=./worker_commons.sh
  source "${scriptDir}"/worker_commons.sh
 
  #-------------------------------------
  # Remove obsolete files
  #-------------------------------------
  rmfilesNum=$8
  obsoletes=${@:9:${rmfilesNum}}

  if [ ${rmfilesNum} -gt 0 ]; then
    echo "[WORKER.SH] Removing $rmfilesNum obsolete files"
    rm ${obsoletes}
  fi

  #-------------------------------------
  # Determine Language-dependent script
  #-------------------------------------
  get_host_parameters "$@"
  echo "HOSTFLAGS NODENAME = ${hostFlags[0]}"


  echo "[WORKER.SH] Starting GAT Worker"
  if [ "${debug}" == "true" ]; then
    echo "[WORKER.SH]         - Node name                          = ${nodeName}"
    echo "[WORKER.SH]         - Installation Directory             = ${installDir}"
    echo "[WORKER.SH]         - Application path                   = ${appDir}"
    echo "[WORKER.SH]         - LibPath                            = ${libPath}"
    echo "[WORKER.SH]         - Working Directory                  = ${workingDir}"
    echo "[WORKER.SH]         - Storage Configuration              = ${storageConf}"
    if [ "${tracing}" == "true" ]; then
      echo "[WORKER.SH]         - Tracing                            = enabled" 
    else
      echo "[WORKER.SH]         - Tracing                            = disabled" 
    fi
  fi

  implType=${invocation[0]}
  if [ "${implType}" == "METHOD" ]; then
    lang=${invocation[1]}
  else
    echo "[WORKER.SH] Non-native task detected. Switching to JAVA invoker."
    lang="java"
  fi

  #-------------------------------------
  # Determine Language-dependent script
  #-------------------------------------
  cd "$workingDir" || exit 1
  echo "[WORKER.SH] Starting language $lang script"
  "${scriptDir}"/worker_$lang.sh "$@"
  endCode=$?
  echo " "
  echo "[WORKER.SH] EndStatus = $endCode"
  cd "$workingDir" || exit 1

exit




  #-------------------------------------
  # Task Information
  #-------------------------------------
  jobId=$1
  taskId=$2
  isSpecific=$3
  shift 3
  if [ "${isSpecific}" == "true" ]; then
    sandboxDir=$1
    shift 1
  fi

  

  implType=$1
  if [ "${implType}" == "METHOD" ]; then
    lang=$2
  else
    echo "[WORKER.SH] Non-native task detected. Switching to JAVA invoker."
    lang="java"
  fi

  export COMPSS_APP_DIR=$appDir
  cd "$workingDir" || exit 1
  # Run the task with the language-dependent script
  echo " "
  echo "[WORKER.SH] Starting language $lang dependant script"
  "${scriptDir}"/worker_$lang.sh "$@"
  endCode=$?
  echo " "
  echo "[WORKER.SH] EndStatus = $endCode"
  cd "$workingDir" || exit 1


ls -la $workingDir

  #-------------------------------------
  # Exit
  #-------------------------------------
  if [ $endCode -eq 0 ]; then
    exit 0
  else
    echo 1>&2 "Task execution failed"
    exit 7
  fi

























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
  if [ ! -d "$workingDir" ]; then
    mkdir -p "$workingDir"
  fi
  export COMPSS_WORKING_DIR=$workingDir

  cd "$workingDir" || exit 1

  echo "[WORKER.SH] Starting GAT Worker"
  echo "[WORKER.SH]    - Lang              = $lang"
  echo "[WORKER.SH]    - WorkingDir        = $workingDir"
  echo "[WORKER.SH]    - LibPath           = $libPath"

  #-------------------------------------
  # Remove obsolete files
  #-------------------------------------
  echo "[WORKER.SH] Removing $rmfilesNum obsolete files"
  for ((i=0;i<rmfilesNum;i++)); do
    echo "$1"
    rm -f "$1"
    shift 1
  done


  #-------------------------------------
  # Get tracing Setup
  #-------------------------------------
  tracing=$1
  nodeName=$2
  shift 2
  export EXTRAE_BUFFER_SIZE=100
  echo "[WORKER.SH]    - Tracing           = $tracing"
  echo "[WORKER.SH]    - Tracing node name = $nodeName"
  if [ "$tracing" == "true" ]; then
    runtimeEventType=$1
    sandBoxCreationId=$2
    sandBoxRemovalId=$3
    taskEventType=$4
    taskId=$5
    slot=$6
    shift 6
  fi

  #-------------------------------------
  # Set lib path
  #-------------------------------------
  if [ "$libPath" != "null" ]; then
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$libPath
  fi


  #-------------------------------------
  # Managing Symlinks for files
  #-------------------------------------

  # Trace sandbox creation start event if needed
  if [ "$tracing" == "true" ]; then
    "${scriptDir}"/../../trace.sh start "$workingDir" "$runtimeEventType" "$sandBoxCreationId" "$slot"
  fi

  isSpecific=$1
  sandbox=$2
  shift 2
  echo "[WORKER.SH]    - Sandbox    = $sandbox"
  echo "[WORKER.SH]    - isSpecific = $isSpecific"
  if [ ! -d "$sandbox" ]; then
    mkdir -p "$sandbox"
  fi

  symlinkfilesNum=$1
  shift 1
  renames=""
  if [ "$symlinkfilesNum" -ne 0 ]; then
    echo "[WORKER.SH] Creating $symlinkfilesNum symlink files"
    for ((i=0;i<symlinkfilesNum;i=i+2)); do
      # Create symlink for in inout files
      if [ -f "$1" ]; then
        if [ ! -f "${sandbox}/$2" ]; then
          echo "[WORKER.SH] Link $1 -> ${sandbox}/${2}"
      	  ln -s "$1" "${sandbox}/$2"
        else
          newVer=$(basename "$1" | tr "_" "\t" | awk '{ print $1 }' | tr "v" "\t" | awk '{ print $2 }')
          oldVer=$(basename "$(readlink -f "${sandbox}/$2")" | tr "_" "\t" | awk '{ print $1 }' | tr "v" "\t" | awk '{ print $2 }')
          if (( newVer > oldVer )); then
            ln -sf "$1" "${sandbox}/$2"
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

  # Trace sandbox creation end event if needed
  if [ "$tracing" == "true" ]; then
    "${scriptDir}"/../../trace.sh end "$workingDir" "$runtimeEventType" "$slot"
  fi





  #-------------------------------------
  # Move to app dir and execute
  #-------------------------------------
  appDir=$1
  export COMPSS_APP_DIR=$appDir

  # Add support for non-native tasks
  implType=${13}
  if [ "$implType" != "METHOD" ]; then
    echo "[WORKER.SH] Non-native task detected. Switching to JAVA invoker."
    lang=java
  fi

  # Trace task start event if needed
  if [ "$tracing" == "true" ]; then
    "${scriptDir}"/../../trace.sh start "$workingDir" "$taskEventType" "$taskId" "$slot"
  fi




  # Trace task end event if needed
  if [ "$tracing" == "true" ]; then
    "${scriptDir}"/../../trace.sh end "$workingDir" "$taskEventType" "$slot"
  fi

  #-------------------------------------
  # Undo symlinks and rename files
  #-------------------------------------
  # Trace sanbox removal start event if needed
  if [ "$tracing" == "true" ]; then
    "${scriptDir}"/../../trace.sh start "$workingDir" "$taskEventType" "$taskId" "$slot"
  fi

  if [ "$symlinkfilesNum" -ne 0 ]; then
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
            rm "$element"
          fi
    	elif [ $removeOrMove -eq 2 ]; then
          echo "[WORKER.SH] Moving $element to $renamedFile"
          if [ -f "$element" ]; then
            mv "$element" "$renamedFile"
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
    rm -rf "$sandbox"
  fi

  # Trace sandbox removal end event if needed
  if [ "$tracing" == "true" ]; then
    "${scriptDir}"/../../trace.sh end "$workingDir" "$runtimeEventType" "$slot"
  fi

