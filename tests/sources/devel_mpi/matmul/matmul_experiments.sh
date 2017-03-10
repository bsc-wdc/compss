#!/bin/bash 

  wait_and_get_jobId() {

  }

  log_information() {
    echo "Enqueued job with ID = $jobDep"
    echo "  - MSIZE = $msize"
    echo "  - BSIZE = $bsize"
    echo "  - NPMT  = $NPMT"
    echo ""
  }

  mSizes="2 4 8"
  bSizes="2 4 8"
  nodesXmpi="1 2 4 8"
  jobDep=None

  for msize in $mSizes; do
    for bsize in $bSizes; do
      for npmt in $nodesXmpi; do
        ./enqueue_matmul $MSIZE $BSIZE $NPMT $jobDep
        wait_and_get_jobId
        log_information
      done
    done
  done

