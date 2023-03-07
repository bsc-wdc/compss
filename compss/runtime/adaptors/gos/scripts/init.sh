#!/bin/bash

  workingDir=$1
  jobStatus=$2
  cancelScript=$3
  batchOutput=$4
  #-------------------------------------
  # Create sandbox
  #-------------------------------------
  mkdir -p "$workingDir"
  mkdir -p "${workingDir}/${jobStatus}"
  mkdir -p "${workingDir}/${cancelScript}"
  mkdir -p "${workingDir}/${batchOutput}"

  if [ ! -d "${workingDir}/${batchOutput}" ]; then
      exit 7
  fi

