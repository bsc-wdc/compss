#!/bin/bash -e

  # Define script variables
  scriptDir=$(pwd)/$(dirname $0)
  execFile=${scriptDir}/examples.py
  appClasspath=${scriptDir}/
  appPythonpath=${scriptDir}/

  # Retrieve arguments
  jobDependency=$1
  numNodes=$2
  executionTime=$3
  tasksPerNode=$4
  tracing=$5

  # Leave application args on $@
  shift 5

  # Enqueue the application
  enqueue_compss \
    --job_dependency=$jobDependency \
    --num_nodes=$numNodes \
    --exec_time=$executionTime \
    --tracing=$tracing \
    --graph=true \
    --classpath=$appClasspath \
    --pythonpath=$appPythonpath \
    --lang=python \
    --qos=debug \
    $execFile $@


######################################################
# APPLICATION EXECUTION EXAMPLE
# Call:
#       ./launch.sh jobDependency numNodes executionTime tasksPerNode tracing datasetPath resultFile blockSize
#
# Example:
#       ./launch.sh None 2 10 16 false /gpfs/projects/bsc19/COMPSs_DATASETS/wordcount/data/dataset_64f_16mb ddh_result.txt 10000
#
