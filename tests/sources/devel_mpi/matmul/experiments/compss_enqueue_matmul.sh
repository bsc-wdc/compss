#!/bin/bash

  cus=$1
  bsize=$2
  EXEC_TIME=$3

  scriptDir=$(pwd)

  # NOT USED
  binary=${scriptDir}/bin/matmul
  export MATMUL_BINARY=${binary}
  # NOT USED
  export MPI_PROCS=1

  # CUS of each task (16 4 1)
  export BSIZE=$bsize
  export CUS=$cus

  echo "Enqueueing job for BSIZE=$BSIZE ; CUS=$CUS"
  enqueue_compss \
    --exec_time=$EXEC_TIME \
    --num_nodes=2 \
    --tasks_per_node=16 \
    --master_working_dir=. \
    --worker_working_dir=scratch \
    --classpath=${scriptDir}/bin/matmul.jar \
    --network=infiniband \
    --log_level=off \
    --tracing=false \
    --graph=false \
    --summary \
    matmul.files.Matmul 1 4 $BSIZE
 
