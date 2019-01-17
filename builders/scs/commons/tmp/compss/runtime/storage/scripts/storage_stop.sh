#!/bin/bash 
 
  #############################################################
  # Name: storage_stop.sh
  # Description: Storage API script for COMPSs
  # Parameters: <jobId>              Queue Job Id 
  #             <masterNode>         COMPSs Master Node
  #             <storageMasterNode>  Node reserved for Storage Master Node (if needed)
  #             "<workerNodes>"      Nodes set as COMPSs workers
  #             <network>            Network type
  #############################################################

  #---------------------------------------------------------
  # ERROR CONSTANTS
  #---------------------------------------------------------
  ERROR_STOP_DATACLAY="Error: Cannot stop dataClay"


  #---------------------------------------------------------
  # HELPER FUNCTIONS
  #---------------------------------------------------------

  ####################
  # Function to display usage
  ####################
  usage() {
    local exitValue=$1

    echo " Usage: $0 <jobId> <masterNode> <storageMasterNode> \"<workerNodes>\" <network>"
    echo " "

    exit $exitValue
  }

  ####################
  # Function to display error
  ####################
  display_error() {
    local errorMsg=$1
    
    echo "ERROR: $errorMsg"
    exit 1
  }


  #---------------------------------------------------------
  # MAIN FUNCTIONS
  #---------------------------------------------------------

  ####################
  # Function to get args
  ####################
  get_args() {
    NUM_PARAMS=5

    # Check parameters
    if [ $# -eq 1 ]; then
      if [ "$1" == "usage" ]; then
        usage 0
      fi
    fi
    if [ $# -ne ${NUM_PARAMS} ]; then
      echo "Incorrect number of parameters"
      usage 1
    fi

    # Get parameters
    jobId=$1
    master_node=$2
    storage_master_node=$3
    worker_nodes=$4
    network=$5
  }

  ####################
  # Function to check and arrange args
  ####################
  check_args() {
    # Convert network to suffix
    if [ "${network}" == "ethernet" ]; then
      network=""
    elif [ "${network}" == "infiniband" ]; then
      network="-ib0"
    elif [ "${network}" == "data" ]; then
      network=""
    fi
  }

  ####################
  # Function to log received arguments
  ####################
  log_args() {
    echo "--- STORAGE_STOP.SH ---"
    echo "Job ID:              $jobId"
    echo "Master Node:         $master_node"
    echo "Storage Master Node: $storage_master_node"
    echo "Worker Nodes:        $worker_nodes"
    echo "Network:             $network"
    echo "-----------------------"
  }


  #---------------------------------------------------------
  # MAIN FUNCTIONS
  #---------------------------------------------------------
  STORAGE_HOME=$(dirname $0)/../

  get_args "$@"
  check_args
  log_args

  ############################
  ## STORAGE DEPENDENT CODE ##
  ############################

  # Stop dataClay
  ${STORAGE_HOME}/scripts/_stopDataClay.sh \
     --lmnode ${storageMasterNode} \
     --dsnodes "${workerNodes:1}" \
     --dcdir ${STORAGE_HOME} \
     --jobid ${jobId} \
     --networksuffix "${network}"
  if [ $? -ne 0 ]; then
    display_error "${ERROR_STOP_DATACLAY}"
  fi

  ############################
  ## END                    ##
  ############################
  exit

