#!/bin/bash

  TYPE=$1
  MSIZE=$2
  BSIZE=$3
  NPMT=$4

  EXEC_TIME=$5
  NUM_NODES=$6
  JOBDEP=$7

  scriptDir=$(pwd)/$(dirname $0)  
  binary=${scriptDir}/bin/matmul

  export MATMUL_BINARY=${binary}
  export NODES_PER_MPI_TASK=${NPMT}
  export CUS_PER_MPI_TASK=16

  enqueue_compss \
    --exec_time=$EXEC_TIME \
    --num_nodes=$NUM_NODES \
    --tasks_per_node=16 \
    --master_working_dir=. \
    --worker_working_dir=scratch \
    --classpath=${scriptDir}/matmul.jar \
    --network=infiniband \
    --log_level=debug \
    --tracing=false \
    --graph=false \
    --summary \
    --job_dependency=$JOBDEP \
    matmul.files.Matmul $TYPE $MSIZE $BSIZE
 
