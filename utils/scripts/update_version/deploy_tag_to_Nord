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
  buildFolder="nord4"
  buildScript="buildNord4"
  user=compss
  endpoint=n4login1.bsc.es
  modules_folder="/apps/modules/modulefiles/tools/compss/"
  install_folder="/apps/COMPSs/"
  # Call generic script
  ./deploy_tag_to_SC "${version}" ${buildFolder} ${buildScript} ${user} ${endpoint} "${modules_folder}" "${install_folder}"

  # END
  exit 0
