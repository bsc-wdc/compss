#!/bin/bash

  #
  # Displays script usage
  #
  usage() {
    echo "Usage: $0 <dlbSrcDir> <dlbTargetDir>"
  }

  #
  # Private function to check if a command is available
  #
  command_exists () {
    type "$1" &> /dev/null ;
  }

  #
  # Commands to install DLB
  #
  install() {
    local dlbSrc=$1
    local dlbTarget=$2

    # Move to sources directory
    cd "${dlbSrc}" || exit 1

    ./bootstrap
    ev=$?
    if [ "$ev" -ne 0 ]; then
      exit $ev
    fi

    # Check if python3 command exists
    if ! command_exists "python3" ; then
      echo "ERROR: Could not find python3 command."
      exit 1
    fi

    # Configure, compile and install
    ./configure \
      --prefix="${dlbTarget}" \
      PYTHON=python3
    ev=$?
    if [ "$ev" -ne 0 ]; then
      exit $ev
    fi

    make
    ev=$?
    if [ "$ev" -ne 0 ]; then
      exit $ev
    fi

    make clean install
    ev=$?
    if [ "$ev" -ne 0 ]; then
      exit $ev
    fi
  }


  #
  # MAIN METHOD
  #
  main() {
    # Retrieve script variables
    if [ "$#" -lt 1 ]; then
       echo "ERROR: Invalid number of parameters"
       usage
       exit 1
    fi
    dlbSrcDir=$1
    dlbTargetDir=$2

    echo "Install script parameters:"
    echo " * DLB Source Dir: ${dlbSrcDir}"
    echo " * DLB Target Dir: ${dlbTargetDir}"

    # Create installation folder
    mkdir -p "${dlbTargetDir}" || exit 1

    # Install
    install "${dlbSrcDir}" "${dlbTargetDir}"

    # Exit all ok
    echo "DLB successfully installed!"
    exit 0
  }


  #
  # ENTRY POINT
  #
  main "$@"
