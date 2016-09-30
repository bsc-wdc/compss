#!/bin/bash
cd $(dirname "$0")

# This script saves csvs for different applications like Matmul, KMeans, etc.
AUTO_CBM_DIR=../../..
RES_DIR=${AUTO_CBM_DIR}/results
MATMUL_RES_DIR=${RES_DIR}/matmul-csvs
CSV=${MATMUL_RES_DIR}/matmul.csv

mkdir -p "${MATMUL_RES_DIR}"
touch ${CSV}
echo "BSIZE,MSIZE,Matrix_Size,Elapsed_Seconds" >> ${CSV}

for FILE_OUT in ${RES_DIR}/matmul-data/*
do
  CONTENTS="$(cat $FILE_OUT)"
  MSIZE="$(echo "$CONTENTS" | grep "MSIZE" | cut -d= -f2 | cut -d" " -f2)"
  BSIZE="$(echo "$CONTENTS" | grep "BSIZE" | cut -d= -f2 | cut -d" " -f2)"
  INIT_TIME="$(echo "$CONTENTS" | grep "No more tasks" | cut -d")" -f1 | cut -d"(" -f2)"
  END_TIME="$(echo "$CONTENTS" | grep "Getting Result" | cut -d")" -f1 | cut -d"(" -f2)"
  INIT_TIME="$(date -d "$INIT_TIME" +%s)" # Just convert to epoch
  END_TIME="$(date -d "$END_TIME" +%s)" # Just convert to epoch
  ELAPSED_SECS=$(($END_TIME - $INIT_TIME))
  
  MATRIX_SIZE=$(($BSIZE * $MSIZE))
  echo "${BSIZE},${MSIZE},${MATRIX_SIZE},${ELAPSED_SECS}" >> ${CSV}
done
