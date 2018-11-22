#!/bin/sh

df_script=$1
df_executor=$2
#df_lib=$3
#mpirunner=$4
num_nodes=$6
hostfile=$8
args=${10}

echo "Executing decaf data-flow generator: $df_script -n ${num_nodes} --hostfile ${hostfile} --args \"${args}\"" 
python "$df_script" -n "${num_nodes}" --hostfile "${hostfile}" --args "${args}"
ev=$?
if [ $ev -ne 0 ]; then
  echo "Error running data-flow generator"
  exit $ev
fi

echo "Executing decaf data-flow: $df_executor" 
$df_executor
ev=$?
if [ $ev -ne 0 ]; then
  echo "Error running data-flow"
  exit $ev
fi
