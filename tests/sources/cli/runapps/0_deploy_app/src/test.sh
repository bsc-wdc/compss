#!/bin/bash -e

user_cluster=$1

pycompss init --name cluster_env cluster -l $user_cluster
pycompss app deploy matmul --local_source $(pwd)/src/matmul
pycompss app remove matmul
pycompss environment remove cluster_env