#!/bin/bash

  MSIZE=$1
  BSIZE=$2
  NPMT=$3
  JOBDEP=$4

  scriptDir=$(pwd)/$(dirname $0)  
  binary=${scriptDir}/bin/matmul

  export MATMUL_BINARY=${binary}
  export NODES_PER_MPI_TASK=${NPMT}
  export CUS_PER_MPI_TASK=16

  enqueue_compss \
    --exec_time=10 \
    --num_nodes=2 \
    --job_dependency=None \
    --tasks_per_node=16 \
    --master_working_dir=. \
    --worker_working_dir=scratch \
    --classpath=${scriptDir}/matmul.jar \
    --network=infiniband \
    --log_level=debug \
    --tracing=false \
    --graph=true \
    --summary \
    --job_dependency=$JOBDEP \
    matmul.files.Matmul $MSIZE $BSIZE
 
