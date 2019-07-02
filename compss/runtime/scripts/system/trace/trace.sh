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
    node=$1
    # echo "trace::packaging ${node}_compss_trace.tar.gz"
    files="TRACE.mpits set-*"
    if [ "$node" != "master" ] && [ -d "./python" ] ; then
        hostID=$2
        if [ -f ./python/TRACE.mpits ]; then
            "${extraeDir}"/bin/mpi2prv -f ./python/TRACE.mpits -no-syn -o "./${hostID}_python_trace.prv"
            files+=" ${hostID}_python_trace.*"
        else
            mv ./python ./python_${node}
            files+=" ./python_${node}"
        fi
    fi
    if [ -f TRACE.sym ]; then
        files+=" TRACE.sym"
    fi
    # shellcheck disable=SC2086
    tar czf "${node}_compss_trace.tar.gz" ${files}
    echo "Package created $(ls -la  ${node}_compss_trace.tar.gz)"
    endCode=$?
    # shellcheck disable=SC2086
    rm -rf ${files}

  elif [ "$action" == "package-scorep" ]; then
    node=$1
    # echo "trace::packaging ${node}_compss_trace.tar.gz"
    files=""
    if [ "$node" != "master" ] && [ -d "./python" ] ; then
        if [ -f ./python/pycompss.log ]; then
            rm -f ./python/pycompss.log
        fi
        if ls ./python/core.* 1> /dev/null 2>&1; then
            # Remove the current corefiles generated to avoid big package
            # Notified this issue.
            # TODO: remove this if block when the issue is fixed.
            rm -f ./python/core*
        fi
        mv ./python ./python_${node}
        files+=" ./python_${node}"
    fi
    # shellcheck disable=SC2086
    tar czf "${node}_compss_trace.tar.gz" ${files}
    echo "Package created $(ls -la  ${node}_compss_trace.tar.gz)"
    endCode=$?
    # shellcheck disable=SC2086
    rm -rf ${files}

  elif [ "$action" == "package-map" ]; then
    node=$1
    # echo "trace::packaging ${node}_compss_trace.tar.gz"
    files=""
    if [ "$node" != "master" ] && [ -d "./python" ] ; then
        sleep 5 # The processes must have been killed. This waits for them.
                # Otherwise, it raises an exception (piped mirror) - suspect map interaction.
        if [ -f ./python/pycompss.log ]; then
            rm -f ./python/pycompss.log
        fi
        mv ./python ./python_${node}
        files+=" ./python_${node}"
    fi
    # shellcheck disable=SC2086
    tar czf "${node}_compss_trace.tar.gz" ${files}
    echo "Package created $(ls -la  ${node}_compss_trace.tar.gz)"
    endCode=$?
    # shellcheck disable=SC2086
    rm -rf ${files}

  elif [ "$action" == "gentrace" ]; then
    appName=$1
    numberOfResources=$2
    python_version=$3  # not used with extrae

    # Check machine max open files
    openFilesLimit=$(ulimit -Sn)
    if [ "$openFilesLimit" -eq "$openFilesLimit" ] 2>/dev/null; then
      # ulimit reported a valid number of open filesz
      maxMpitNumber=$((openFilesLimit - 20))
    else
      maxMpitNumber=$MIN_MPITS_PARALLEL_MERGE
    fi

    traceFiles=$(find trace/*_compss_trace.tar.gz)
    #echo "trace::gentrace"
    for file in ${traceFiles[*]}; do
        tmpDir=$(mktemp -d)
        tar -C "$tmpDir" -xzf "$file"
        #echo "trace:: $tmpDir -xvzf $file"
        cat "$tmpDir"/TRACE.mpits >> TRACE.mpits
        cp -r "$tmpDir"/set-* .
        files=$(find "$tmpDir" -name "*_python_trace.*")
        if [ ! -z "$files" ]; then
            nodeDir="$(pwd)/trace/python/"
            mkdir -p "$nodeDir"
            # shellcheck disable=SC2086
            cp ${files} "$nodeDir"
        fi
        if [ -f "$tmpDir"/TRACE.sym ]; then
            cp "$tmpDir"/TRACE.sym .
        fi
        rm -rf "$tmpDir" "$file"
    done
    sec=$(/bin/date +%s)
    # Check if parallel merge is available / should be used
    configuration=$("${extraeDir}"/etc/configured.sh | grep "enable-parallel-merge")
    if [ -z "${configuration}" ] || [ "$(wc -l < TRACE.mpits)" -lt ${maxMpitNumber} ] ; then
        "${extraeDir}"/bin/mpi2prv -f TRACE.mpits -no-syn -o "./trace/${appName}_compss_trace_${sec}.prv"
    else
        mpirun -np "$numberOfResources" "${extraeDir}"/bin/mpimpi2prv -f TRACE.mpits -no-syn -o "./trace/${appName}_compss_trace_${sec}.prv"
    fi
    endCode=$?
    rm -rf set-0/ TRACE.mpits TRACE.sym

  elif [ "$action" == "scorep-gentrace" ]; then
    appName=$1
    numberOfResources=$2
    python_version=$3

    # We require otf2-merger module loaded
    source ${SCRIPT_DIR}/trace/scorep.sh $python_version

    # Unpack the tar.gz of each worker
    traceFiles=$(find trace/*_compss_trace.tar.gz)
    #echo "trace::scorep_gentrace"
    tmpDir=$(mktemp -d)
    for file in ${traceFiles[*]}; do
        tar -C "$tmpDir" -xzf "$file"
        #echo "trace:: $tmpDir -xvzf $file"
    done
    trace_files=$(find "$tmpDir" -name "*.otf2*")
    # Merge the traces.
    for trace in $trace_files; do
        params+=" --traceFile $trace"
    done
    # Example: otf2-merger --traceFile /path/to/trace/A.otf2 --traceFile /path/to/trace/B.otf2 --traceFile /path/to/trace/C.otf2 --outputPath /path/to/output/dir
    otf2-merger $params --outputPath ./trace/${appName}_scorep_trace
    endCode=$?

    # Clean the temporary directory
    rm -rf "$tmpDir" # "$file"  # keep the original tar.gz

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
