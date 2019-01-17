#!/bin/bash -e

  # Clean previous plots
  rm -rf output

  # Generate plots directory
  mkdir -p output

  # Generate new plots
  gnuplot -e "application='cholesky'" -e "ysize=100" exec-time_and_speed-up.gpl
  gnuplot -e "application='lu'" -e "ysize=200" exec-time_and_speed-up.gpl
  gnuplot -e "application='qr'" -e "ysize=500" exec-time_and_speed-up.gpl

  # DONE
  exit

