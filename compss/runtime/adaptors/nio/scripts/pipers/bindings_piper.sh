#!/bin/bash

  ########################################
  # SCRIPT HELPER FUNCTIONS
  ########################################
  get_args() {
    # Get number of threads
    numThreads=$1
    shift 1

    # Get CMD pipes
    CMDpipes=()
    numPipesCMD=$1
    shift 1
    local i=0
    while [ $i -lt $numPipesCMD ]; do
      local arg_pos=$((i+1))
      CMDpipes[$i]=${!arg_pos}
      i=$((i+1))
    done
    shift ${numPipesCMD}

    # Get RESULT pipes
    RESULTpipes=()
    numPipesRESULT=$1
    shift 1
    i=0
    while [ $i -lt $numPipesRESULT ]; do
      local arg_pos=$((i+1))
      RESULTpipes[$i]=${!arg_pos}
      i=$((i+1))
    done
    shift ${numPipesRESULT}

    # Get binding
    binding=$1
    shift 1

    # Get binding additional executable and arguments
    bindingExecutable=$1
    shift 1
    bindingArgs=$@
  }

  clean_env() {
    echo "[BINDINGS PIPER] Cleaning environment"

    if [ -n "$bindingPID" ]; then
      # Send SIGTERM to specific binding process

      # Ignore kill output because if it doesn't exist it means that
      # the subprocess has correctly finished
      kill -15 $bindingPID > /dev/null
    fi

    for i in "${CMDpipes[@]}"; do
      rm -f $i
    done

    for i in "${RESULTpipes[@]}"; do
      rm -f $i
    done
  }

  ########################################
  # MAIN
  ########################################

  # Arguments
  get_args $@

  # Create TRAP to ensure pipes are deleted if something goes wrong
  trap clean_env EXIT

  # Log
  echo "[BINDINGS PIPER] NumThreads: $numThreads"

  # Clean and Create CMD Pipes
  for i in "${CMDpipes[@]}"; do
    echo "[BINDINGS PIPER] CMD Pipe: $i"
    rm -f $i
    mkfifo $i
  done

  # Clean and Create RESULT Pipes
  for i in "${RESULTpipes[@]}"; do
    echo "[BINDINGS PIPER] RESULT Pipe: $i"
    rm -f $i
    mkfifo $i
  done

  # Perform specific biding call (THE CALL IS BLOCKING)
  echo "[BINDINGS PIPER] Initializing specific binding for $binding"
  echo "[BINDINGS PIPER] Making call: $bindingExecutable $bindingArgs"
  $bindingExecutable $bindingArgs &
  bindingPID=$!

  # Wait for binding executable completion
  wait $bindingPID
  exitValue=$?

  # Clean environment
  # Cleaned on TRAP

  # Exit message
  echo "[BINDINGS PIPER] Finished with status $exitValue"
  exit $exitValue
