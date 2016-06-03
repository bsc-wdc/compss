#!/bin/bash
  
  #-------------------------------------
  # Define script variables and exports
  #-------------------------------------
  scriptDir=$(dirname $0)
  extraeDir=$EXTRAE_HOME
 
  export LD_LIBRARY_PATH=$extraeDir/lib:$LD_LIBRARY_PATH
  #-------------------------------------
  # Get common parameters
  #-------------------------------------
  action=$1
  workingDir=$2
  
  shift 2
  mkdir -p $workingDir
  cd $workingDir

  #-------------------------------------
  # MAIN actions
  #-------------------------------------
  if [ $action == "start" ]; then
    eventType=$1
    taskId=$2
    slot=$3
    #echo "trace::emit-start,  emit $slot $eventType $taskId"
    $extraeDir/bin/extrae-cmd emit $slot $eventType $taskId
    endCode=$?
  elif [ $action == "end" ]; then
    eventType=$1
    slot=$2
    #echo "trace::emit-end,  emit $slot $eventType 0"
    $extraeDir/bin/extrae-cmd emit $slot $eventType 0
    endCode=$?
  elif [ $action == "init" ]; then
    rm -rf TRACE.mpits set-* *_compss_trace.tar.gz
    node=$1
    nslots=$2
    #echo "trace::init, init $node $nslots"
    $extraeDir/bin/extrae-cmd init $node $nslots
    endCode=$?
  elif [ $action == "package" ]; then
    node=$1
    #echo "trace::packaging ${node}_compss_trace.tar.gz"
    files="TRACE.mpits set-*"
    tasksTraces=$(find . -name "*.prv")
    if [ ! -z "$tasksTraces" ]; then
	files+=" $tasksTraces"
    fi
    if [ -f TRACE.sym ]; then
        files+=" TRACE.sym"
    fi    
    tar czf ${node}_compss_trace.tar.gz $files
    endCode=$?
    rm -rf $files
  elif [ $action == "gentrace" ]; then
    appName=$1
    numberOfResources=$2
    traceFiles=$(find trace/*_compss_trace.tar.gz)
    #echo "trace::gentrace"
    for file in ${traceFiles[*]}; do
        tmpDir=$(mktemp -d)
        tar -C $tmpDir -xzf $file
        #echo "trace:: $tmpDir -xvzf $file"
        cat $tmpDir/TRACE.mpits >> TRACE.mpits
        cp -r $tmpDir/set-* .
	find $tmpDir -name "*.prv" -exec cp {} ./trace \;
        if [ -f $tmpDir/TRACE.sym ]; then
            cp $tmpDir/TRACE.sym .
        fi
        rm -rf $tmpDir $file
    done
    sec=$(/bin/date +%s)
    # Check if parallel merge is available
    configuration=$(${extraeDir}/etc/configured.sh | grep "enable-parallel-merge")
    if [ -z "${configuration}" ]; then
        ${extraeDir}/bin/mpi2prv -f TRACE.mpits -o ./trace/${appName}_compss_trace_${sec}.prv
    else
        mpirun -np $numberOfResources ${extraeDir}/bin/mpimpi2prv -f TRACE.mpits -o ./trace/${appName}_compss_trace_${sec}.prv
    fi
    endCode=$?
    rm -rf set-0/ TRACE.mpits TRACE.sym
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

