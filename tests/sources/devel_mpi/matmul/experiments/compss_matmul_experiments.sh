#!/bin/bash

  BLOCK_SIZES="32 64 128 256 512 1024"
  PROCS="16 4 1"
  execTime=20

  for proc in $PROCS; do
    for bsize in $BLOCK_SIZES; do
      ./compss_enqueue_matmul.sh $proc $bsize $execTime
      sleep 40s
    done
  done

