#!/bin/bash

  #-------------------------------------
  # Define script variables and exports
  #-------------------------------------
  SCRIPT_DIR="${COMPSS_HOME}/Runtime/scripts/system"

  if [ -z "$EXTRAE_HOME" ]; then
    extraeDir="${SCRIPT_DIR}/../../../Dependencies/extrae/"
  else
    extraeDir=$EXTRAE_HOME
  fi

  MIN_MPITS_PARALLEL_MERGE=1000
  export LD_LIBRARY_PATH=$extraeDir/lib:$LD_LIBRARY_PATH

  mpi2prv() {
    local mpits="${1}"
    local prv="${2}"
    # Check machine max open files
    local openFilesLimit=$(ulimit -Sn)
    local maxMpitNumber=0
    if [ "$openFilesLimit" -eq "$openFilesLimit" ] 2>/dev/null; then
      # ulimit reported a valid number of open files
      maxMpitNumber=$((openFilesLimit - 20))
    else
      maxMpitNumber=$MIN_MPITS_PARALLEL_MERGE
    fi

    # Check if parallel merge is available / should be used
    configuration=$("${extraeDir}"/etc/configured.sh | grep "enable-parallel-merge")

    # Check if parallel merge is available / should be used
    if [ -z "${configuration}" ] || [ "$(wc -l < "${mpits}")" -lt ${maxMpitNumber} ] ; then
      "${extraeDir}/bin/mpi2prv" -f "${mpits}" -no-syn -o "${prv}"
    else
      mpirun -np "$numberOfResources" "${extraeDir}/bin/mpimpi2prv" -f "${mpits}" -no-syn -o "${prv}"
    fi
  }

  #-------------------------------------
  # Get common parameters
  #-------------------------------------
  action=$1
  workingDir=$2

  shift 2
  mkdir -p "$workingDir"
  cd "$workingDir" || exit 1

  #-------------------------------------
  # MAIN actions
  #-------------------------------------
  if [ "$action" == "start" ]; then
    eventType=$1
    taskId=$2
    slot=$3
    #echo "trace::emit-start,  emit $slot $eventType $taskId"
    "$extraeDir"/bin/extrae-cmd emit "$slot" "$eventType" "$taskId"
    endCode=$?

  elif [ "$action" == "end" ]; then
    eventType=$1
    slot=$2
    #echo "trace::emit-end,  emit $slot $eventType 0"
    "$extraeDir"/bin/extrae-cmd emit "$slot" "$eventType" 0
    endCode=$?

  elif [ "$action" == "init" ]; then
    rm -rf TRACE.mpits set-* *_compss_trace.tar.gz
    node=$1
    nslots=$2
    #echo "trace::init, init $node $nslots"
    "$extraeDir"/bin/extrae-cmd init "$node" "$nslots"
    endCode=$?

  elif [ "$action" == "package" ]; then
    package_path=$1
    # echo "trace::packaging ${package_path}"
    files="TRACE.mpits set-*"
    if [ -d "./python" ] ; then
        if [ -f ./python/TRACE.mpits ]; then
          hostID=$2
          echo "${hostID}" >> ./python/hostID
          files+=" ./python"
        fi
    fi

    if [ -f TRACE.sym ]; then
        files+=" TRACE.sym"
    fi
    # shellcheck disable=SC2086
    tar czf "${package_path}" ${files}
    echo "Package created $(ls -la  "${package_path}")"
    endCode=$?
    # shellcheck disable=SC2086
    rm -rf ${files}

  elif [ "$action" == "gentrace" ]; then
    appName=$1
    numberOfResources=$2

    traceDir="$(pwd)/trace/"
    pythonDir="${traceDir}python/"
    mpits="TRACE.mpits"
    prv="${traceDir}/${appName}_compss.prv"

    packages=$(find ${traceDir}/*_compss_trace.tar.gz)
    #echo "trace::gentrace"
    for package in ${packages[*]}; do
      tmpDir=$(mktemp -d)
      tar -C "$tmpDir" -xzf "${package}"

      #echo "trace:: $tmpDir -xvzf $file"
      cat "${tmpDir}/TRACE.mpits" >> "${mpits}"

      setFolder=$(ls "${tmpDir}" | grep "set" )
      setFolder="${tmpDir}/${setFolder}"
      cp -r "${setFolder}" . 

      if [ -d "${tmpDir}/python" ]; then
        hostId=$(cat "${tmpDir}/python/hostID")
        python_mpits="${tmpDir}/python/TRACE.mpits"
        python_prv="${pythonDir}/${hostId}_python_trace.prv"

        mpi2prv "${python_mpits}" "${python_prv}"
      fi

      if [ -f "${tmpDir}/TRACE.sym" ]; then
        cp "${tmpDir}/TRACE.sym" .
      fi

      rm -rf "$tmpDir" "${package}"
    done

    mpi2prv "${mpits}" "${prv}"

    endCode=$?
    rm -rf set-0/ "${mpits}" TRACE.sym
    

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
