#!/bin/bash

  scriptDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

  # Change to root compss directory
  cd $scriptDir/../../../compss/

  # Add java headers
  find . -name "*.java" -exec $scriptDir/replace_header.sh {} java_c \; 

  # Add c headers
  find . -name "*.c" -exec $scriptDir/replace_header.sh {} java_c \;
  find . -name "*.cc" -exec $scriptDir/replace_header.sh {} java_c \;
  find . -name "*.h" -exec $scriptDir/replace_header.sh {} java_c \;
  find . -name "Makefile*" -exec $scriptDir/replace_header.sh {} python \;

  # Add python headers
  find . -name "*.py" -exec $scriptDir/replace_header.sh {} python \;
