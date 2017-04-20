#!/bin/bash

  wait_and_get_jobId() {
    sleep 5s
    jobDep=$(bjobs | awk {' print $1 '} | tail -n +2 | sort | tail -n 1)
    if [ "$jobDep" == "No unfinished job found" ] || [ "$jobDep" == "" ]; then
      jobDep=None
    fi
    echo "JOB ID = $jobDep"
  }

  NUM_PROCS="1 2 4 16"
  BSIZES="512 1024 2048 4096"
  MSIZE="4"
  jobDep=None
  for proc in $NUM_PROCS; do
    for bsize in $BSIZES; do
      # COMPSs msize bsize numProcs numNodes execTime jobDep
      ./enqueue_matmul_compss.sh $MSIZE $bsize $proc 2 15 $jobDep
      wait_and_get_jobId
      # MPI
      ./enqueue_matmul_mpi.sh $MSIZE $bsize $proc 16 15 $jobDep
      wait_and_get_jobId
      # Hybrid
      ./enqueue_matmul_hybrid.sh $MSIZE $bsize $proc 2 15 $jobDep
      wait_and_get_jobId
    done
  done

