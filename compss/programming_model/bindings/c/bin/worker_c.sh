#!/bin/sh

  scriptDir=$(dirname $0)
  bindingsDir=${scriptDir}/../../../../../Bindings
  app_dir=$1
  export LD_LIBRARY_PATH=${bindingsDir}/c/lib:${bindigsDir}/bindings-common/lib:$LD_LIBRARY_PATH

  exec ${app_dir}/worker/worker_c $@

