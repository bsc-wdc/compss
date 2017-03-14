#!/bin/bash

  # MUST BE FIXED -------------------
  ulimit -s unlimited
  # END MUST BE FIXED ----------------
 
  # Define script variables
  scriptDir=$(pwd)/$(dirname $0)
  propertiesFile=${scriptDir}/nmmb_compss.properties

  # Define NMMB.jar environment constants
  export NEMS_NODES=4
  export NEMS_CUS_PER_NODE=16

  # Enqueue
  jobDepFlag="--job_dependency=None"
  debugFlags="--log_level=debug --summary"
  toolsFlags="--graph=true --tracing=false"

  enqueue_compss \
    --exec_time=15 \
    --num_nodes=5 \
    --tasks_per_node=16 \
    ${jobDepFlag} \
    --master_working_dir=. \
    --worker_working_dir=scratch \
    --network=infiniband \
    ${debugFlags} \
    ${toolsFlags} \
    --classpath=${scriptDir}/../nmmb.jar \
    nmmb.Nmmb ${propertiesFile}

