#!/bin/bash
  #Get parameters
  workingDir=$1
  tracing=$2

  cd $workingDir

  if [ $tracing == "true" ]; then
    node=$3
    scriptDir=$(dirname $0)
    $scriptDir/trace.sh package $workingDir $node
  fi

  rm -rf *.IT

