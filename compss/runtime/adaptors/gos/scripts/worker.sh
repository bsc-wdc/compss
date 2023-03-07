#!/bin/bash

  #Arguments Required
  #   rmFilesName + obsoletesPath
  # worker_commons.sh loads some variables
  # shellcheck disable=SC2154

  #-------------------------------------
  # Define script variables and exports
  #-------------------------------------
  if [ -z "${COMPSS_HOME}" ]; then
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    export COMPSS_HOME=${SCRIPT_DIR}/../../../../../
  else
    SCRIPT_DIR="${COMPSS_HOME}/Runtime/scripts/system/adaptors/gos"
  fi

  # shellcheck source=./worker_commons.sh
    # shellcheck disable=SC1091
    source "${SCRIPT_DIR}"/worker_commons.sh

      # shellcheck source=./response.sh
      # shellcheck disable=SC1091
      source "${SCRIPT_DIR}"/response.sh

  on_error(){
    echo "[WORKER.SH] An unexpected error ocurred, aborting proccess."
    mark_as_fail "$responseDir" "$responsePath" "$programID"
    exit 1
  }

  REDIRECT_ERROR_TRAP="true";
  trap 'on_error' ERR


    #-------------------------------------
    # Initialize Response Files
    #-------------------------------------



  #-------------------------------------
  # Determine Language-dependent script
  #-------------------------------------
  # Loads variables
  get_all_parameters "$@"


  if [ "${debug}" == "true" ]; then
    printAllParams
  fi


  if [ "${rmfilesNum}" -gt 0 ]; then
      echo "[WORKER.SH] Removing $rmfilesNum obsolete files $obsoletes"
      # shellcheck disable=SC2086
      rm -f ${obsoletes}
  fi

  # shellcheck source=setup.sh
  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}"/setup.sh

  echo "[WORKER.SH] Starting GOS Worker"

  # Normal start -----------------------------------------------------
  # Setup
  setup_environment
  setup_extrae #tracing initialization
  #setup_jvm
  get_command
  echo "CMD: $cmd"
  # Launch the Worker JVM
  pre_launch
  # Pre-execution
  #set_env

  cd "$workingDir" || exit 1
  echo "[WORKER.SH] Starting GOSWorker for $lang"
  mark_as_run  "$responseDir" "$responsePath" "$programID"
  bash -c "$cmd"
  endCode=$?
  #Remote code execution
  #"${SETSID}" $cmd ${paramsToCOMPSsWorker} 1> "$workingDir/log/worker_${hostName}.out" 2> "$workingDir/log/worker_${hostName}.err" < /dev/null | echo "$!" &
  #  endCode=$?
  echo " "
  echo "[WORKER.SH] EndStatus = $endCode"


  #-------------------------------------
  # Exit
  #-------------------------------------

  if [ $endCode -eq 0 ]; then
    mark_as_end  "$responseDir" "$responsePath" "$programID"
    exit 0
  else
    echo 1>&2 "Task execution failed"
    mark_as_fail  "$responseDir" "$responsePath" "$programID"
    exit 7
  fi



