#!/bin/bash

  TYPE=$1
  MSIZE=$2
  NUM_NODES=$3
  EXEC_TIME=$4

  scriptDir=$(pwd)
  binary=${scriptDir}/bin/matmul
  export MATMUL_BINARY=${binary}
  export MPI_PROCS=1
  export CUS=4
  export BSIZE=1024

  echo "Enqueueing job for TYPE=$TYPE ; MSIZE=$MSIZE ; NUM_NODES=$NUM_NODES"
  enqueue_compss \
    --exec_time=$EXEC_TIME \
    --num_nodes=$NUM_NODES \
    --tasks_per_node=16 \
    --master_working_dir=. \
    --worker_working_dir=scratch \
    --classpath=${scriptDir}/bin/matmul.jar \
    --network=infiniband \
    --log_level=debug \
    --tracing=false \
    --graph=false \
    --summary \
    matmul.files.Matmul $TYPE $MSIZE $BSIZE
 
