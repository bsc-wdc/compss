#!/bin/bash -e
  
  # Kill possible remaining processes of tests
  exitValue=$(ps -elfa | grep java | grep "integratedtoolkit.nio.worker.NIOWorker" | awk {' print $4 '} | xargs -r kill -9)

  exit $exitValue

