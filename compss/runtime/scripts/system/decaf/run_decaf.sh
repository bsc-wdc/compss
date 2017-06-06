#!/bin/sh
df_script=$1
df_executor=$2
df_lib=$3
shift 3
echo "Executing decaf data-flow generator: $df_script $@" 
python $df_script $@

if [ $? -ne 0 ]; then
  echo "Error running data-flow generator"
  exit 1
fi
echo "Executing decaf data-flow: $df_executor" 

$df_executor
if [ $? -ne 0 ]; then
  echo "Error running data-flow"
  exit 1
fi
