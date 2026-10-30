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
  buildFolder="mn5"
  buildScript="buildMN5"
  user=compss
  endpoint=glogin2.bsc.es
  modules_folder="/apps/GPP/modulefiles/applications/COMPSs/"
  install_folder="/apps/GPP/COMPSs/"
  # Call generic script
  ./deploy_tag_to_SC "${version}" ${buildFolder} ${buildScript} ${user} ${endpoint} "${modules_folder}" "${install_folder}"

  # END
  exit 0

