#!/bin/bash

  BLOCK_SIZES="128 256 512 1024 2048 4096 8192"
  PROCS="1 4 16"

  for proc in $PROCS; do
    for bsize in $BLOCK_SIZES; do
      export MPI_PROCS=$proc
      export BSIZE=$bsize
      bsub < mpi_matmul.cmd
      sleep 40s
    done
  done

