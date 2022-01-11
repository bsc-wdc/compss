#!/bin/bash -e

pycompss job submit -e ComputingUnits=1 matmul --num_nodes=2 --exec_time=10 --master_working_dir={COMPS_APP_PATH} --worker_working_dir=local_disk --tracing=false --pythonpath={COMPS_APP_PATH} --lang=python --qos=debug {COMPS_APP_PATH}/matmul_files.py 4 4