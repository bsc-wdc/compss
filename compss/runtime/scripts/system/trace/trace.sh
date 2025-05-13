#!/bin/bash

  #-------------------------------------
  # Define script variables and exports
  #-------------------------------------
  if [ -z "$EXTRAE_HOME" ]; then
    if [ -z "${COMPSS_HOME}" ]; then
      COMPSS_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/../../../.. && pwd )/"
    fi
    extraeDir="${COMPSS_HOME}Dependencies/extrae/"
  else
    extraeDir="${EXTRAE_HOME}"
  fi

  export LD_LIBRARY_PATH=$extraeDir/lib:$LD_LIBRARY_PATH

  #-------------------------------------
  # Get common parameters
  #-------------------------------------
  action=$1
  shift 1

  #-------------------------------------
  # MAIN actions
  #-------------------------------------
  if [ "$action" == "start" ]; then
    workingDir=$1
    mkdir -p "$workingDir"
    cd "$workingDir" || exit 1
    shift 1

    eventType=$1
    taskId=$2
    slot=$3
    #echo "trace::emit-start,  emit $slot $eventType $taskId"
    "$extraeDir"/bin/extrae-cmd emit "$slot" "$eventType" "$taskId"
    endCode=$?

  elif [ "$action" == "end" ]; then
    workingDir=$1
    mkdir -p "$workingDir"
    cd "$workingDir" || exit 1
    shift 1

    eventType=$1
    slot=$2
    #echo "trace::emit-end,  emit $slot $eventType 0"
    "$extraeDir"/bin/extrae-cmd emit "$slot" "$eventType" 0

    #echo "trace::fini, fini"
    "$extraeDir"/bin/extrae-cmd fini
    endCode=$?

  elif [ "$action" == "init" ]; then
    workingDir=$1
    mkdir -p "$workingDir"
    cd "$workingDir" || exit 1
    shift 1

    rm -rf TRACE.mpits set-* *_compss_trace.tar.gz
    node=$1
    nslots=$2
    #echo "trace::init, init $node $nslots"
    "$extraeDir"/bin/extrae-cmd init "$node" "$nslots"
    endCode=$?

  elif [ "$action" == "package" ]; then
    workingDir=$1
    mkdir -p "$workingDir"
    cd "$workingDir" || exit 1
    shift 1

    package_path=$1

    # These lines are commented because on NIOWorker extrae opens an additional process
    # that never creates the mpit file

    ## waiting for temporary files to be fully emptied
    #stmp_count=$(find . -name "*.stmp"|wc -l)
    #ttmp_count=$(find . -name "*.ttmp"|wc -l)
    #while [ "${stmp_count}" != 0 ] || [ "${ttmp_count}" != 0 ]; do
    #  sleep 1
    #  stmp_count=$(find . -name "*.stmp"|wc -l)
    #  ttmp_count=$(find . -name "*.ttmp"|wc -l)
    #done

    hostID=$2
    echo "${hostID}" >> "./hostID"
    files="./hostID"
    files+=" ./TRACE.mpits"
    files+=" ./set-*"

    if [ -f "./TRACE.sym" ]; then
        files+=" ./TRACE.sym"
    fi

    if [ -d "./python" ] ; then
        if [ -f "./python/TRACE.mpits" ]; then
          # Include a copy of the libseqtrace.so
          cp "${extraeDir}/lib/libseqtrace.so" "./python/.libseqtrace-subprocess.so"
          files+=" ./python"
        fi
    fi

    if [ -d "./R" ] ; then
        if [ -f "./R/TRACE.mpits" ]; then
          files+=" ./R"
        fi
    fi

    echo "Creating package ${package_path} with files: ${files}"
    mkdir -p "${package_path%/*}/"
    tar czf "${package_path}" ${files}

    # shellcheck disable=SC2086
    echo "Package created $(ls -la  "${package_path}")"

    endCode=$?
    # shellcheck disable=SC2086
    rm -rf ${files}
  else
    echo 1>&2 "Unknown tracing action"
    exit 1
  fi

  #-------------------------------------
  # Exit
  #-------------------------------------
  if [ $endCode -eq 0 ]; then
        exit 0
  else
        echo 1>&2 "Tracing action $action failed"
        exit 1
  fi
