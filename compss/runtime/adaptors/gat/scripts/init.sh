#!/bin/bash

  workingDir=$1

  #-------------------------------------
  # Create sandbox
  #-------------------------------------
  if [ ! -d $workingDir ]; then
        /bin/mkdir -p $workingDir
  fi
