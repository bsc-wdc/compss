#!/bin/bash

  workingDir=$1

  #-------------------------------------
  # Create sandbox
  #-------------------------------------
  if [ ! -d "$workingDir" ]; then
    mkdir -p "$workingDir"
  fi
