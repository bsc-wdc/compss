#!/bin/bash

  TYPES="1 2"
  MSIZES=(2 4 8)
  NUM_NODES=(2 2 2 5 5 5 9 9 9)
  EXEC_TIMES=(15 15 15 20 20 20 25 25 25)

  for i in "${!MSIZES[@]}"; do
    for t in $TYPES; do
      mSize=${MSIZES[$i]}
      nn=${NUM_NODES[$i]}
      et=${EXEC_TIMES[$i]}
      ./hybrid_enqueue_matmul.sh $t $mSize $nn $et
      sleep 30s
    done
  done

