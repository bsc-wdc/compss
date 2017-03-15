#!/bin/bash 

  wait_and_get_jobID() {
    # Wait
    sleep 5s
    
    # Get jobID
    jobID=$(bjobs | tail -n 1 | cut -c -7)
    echo "jobID = $jobID"
  }

  log_information() {
    echo "Enqueued job with ID = $jobID"
    echo "  - TYPE      = $t"
    echo "  - MSIZE     = $m"
    echo "  - BSIZE     = $b"
    echo "  - NUM NODES = $nn"
    echo "  - MPI NODES = $mpiN"
    echo ""
  }

  #numWorkers="1 2 4 8 16 32"
  numNodes="2 3 5 17"
  mpiNodes="1 2 4 8 16"
  mSizes="4 6 8 16"
  bSizes="1024 2048"

  for nn in $numNodes; do
    for m in $mSizes; do
      for b in $bSizes; do
        # Enqueue normal
        t=1
        mpiN=1
        ./enqueue_matmul $t $m $b $mpiN $execTime $nn $jobID
        wait_and_get_jobID
        log_information

        # Enqueue hybrid
        t=2
        for mpiN in $mpiNodes; do
          if [ $mpiN -lt $nn ]; then
            ./enqueue_matmul $t $m $b $mpiN $execTime $nn $jobID
            wait_and_get_jobID
            log_information
          fi
        done
      done
    done
  done

