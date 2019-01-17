#!/bin/bash 
 
  #############################################################
  # Name: storage_init.sh
  # Description: Storage API script for COMPSs
  # Parameters: <jobId>              Queue Job Id 
  #             <masterNode>         COMPSs Master Node
  #             <storageMasterNode>  Node reserved for Storage Master Node (if needed)
  #             "<workerNodes>"      Nodes set as COMPSs workers
  #             <network>            Network type
  #             <storageProps>       Properties file for storage specific variables
  #############################################################


  #---------------------------------------------------------
  # ERROR CONSTANTS
  #---------------------------------------------------------
  ERROR_PROPS_FILE="Cannot find storage properties file"
  ERROR_GENERATE_CONF="Cannot generate conf file"
  ERROR_START_DATACLAY="Cannot start dataClay"
 

  #---------------------------------------------------------
  # HELPER FUNCTIONS
  #---------------------------------------------------------

  ####################
  # Function to display usage
  ####################
  usage() {
    local exitValue=$1

    echo " Usage: $0 <jobId> <masterNode> <storageMasterNode> \"<workerNodes>\" <network> <storageProps>"
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
    NUM_PARAMS=6

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
    storageProps=$6
  }

  ####################
  # Function to check and arrange args
  ####################
  check_args() {
    # Check storage Props file exists
    if [ ! -f ${storageProps} ]; then
      # PropsFile doesn't exist
      display_error "${ERROR_PROPS_FILE}"   
    fi
    source ${storageProps}
  
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
    echo "--- STORAGE_INIT.SH ---"
    echo "Job ID:              $jobId"
    echo "Master Node:         $master_node"
    echo "Storage Master Node: $storage_master_node"
    echo "Worker Nodes:        $worker_nodes"
    echo "Network:             $network"
    echo "Storage Props:       $storageProps"
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

  # Create storage sandboxes
  baseSandbox=$HOME/.COMPSs/${jobId}/storage
  cfgDir=${baseSandbox}/cfgfiles
  stubsDir=${baseSandbox}/stubs
  mkdir -p ${baseSandbox}
  mkdir -p ${cfgDir}
  mkdir -p ${stubsDir}

  # Generate configuration
  cfgFile=${cfgDir}/client.properties
  cfgFileStorage=${cfgDir}/storage.properties
  ${STORAGE_HOME}/scripts/generateStorageConf.sh \
     --jobdir ${baseSandbox} \
     --lmnode ${storage_master_node} \
     --account ${APPUSER} \
     --stubsdir ${stubsDir} \
     --dataset ${DATASET} \
     --outputfile ${cfgFileStorage} \
     --networksuffix ${network}
  if [ $? -ne 0 ]; then
    display_error "${ERROR_GENERATE_CONF}"
  fi
  
  # Start dataClay
  ${STORAGE_HOME}/scripts/_startDataClay.sh \
     --lmnode ${storage_master_node} \
     --dsnodes "${worker_nodes}" \
     --dcdir ${STORAGE_HOME} \
     --jobid ${jobId} \
     --networksuffix "${network}" &
  if [ $? -ne 0 ]; then
    display_error "${ERROR_START_DATACLAY}"
  fi
 
  # Sleep to allow dataClay start
  sleep 60

  ############################
  ## END                    ##
  ############################
  exit 

