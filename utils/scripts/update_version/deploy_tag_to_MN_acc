#!/bin/bash -e

  usage() {
    exitValue=$1
  
    echo " "
    echo "Usage $0 <version>"
    echo " "
  
    exit "$exitValue"
  }

 
  #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
  # MAIN
  #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

  # Check parameters
  if [ $# -ne 1 ]; then
    echo "ERROR: Incorrect number of parameters"
    usage 1
  fi

  # Set parameters
  version=$1
  buildFolder="mn5_acc"
  buildScript="buildMN5_acc"
  user=compss
  endpoint=alogin1.bsc.es
  modules_folder="/apps/ACC/modulefiles/applications/COMPSs/"
  install_folder="/apps/ACC/COMPSs/"
  # Call generic script
  ./deploy_tag_to_SC "${version}" ${buildFolder} ${buildScript} ${user} ${endpoint} "${modules_folder}" "${install_folder}"

  # END
  exit 0

