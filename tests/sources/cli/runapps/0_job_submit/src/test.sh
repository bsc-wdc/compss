#!/bin/bash -e

user_cluster=$1

pycompss init --name cluster_env cluster -l $user_cluster
pycompss app deploy matmul --local_source $(pwd)/src/matmul
pycompss job submit -e ComputingUnits=1 -app matmul --num_nodes=2 --exec_time=10 --job_execution_dir={COMPS_APP_PATH} --worker_working_dir=local_disk --tracing=false --pythonpath={COMPS_APP_PATH} --lang=python --qos=debug {COMPS_APP_PATH}/matmul_files.py 4 4
pycompss app remove matmul
pycompss environment remove cluster_env
