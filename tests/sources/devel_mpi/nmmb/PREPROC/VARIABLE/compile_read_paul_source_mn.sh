#!/bin/bash 

  # Get parameters
  src=$1
  dest=$2

  # Compile font (linking to libraries)
  echo "[RPS_SH] Compile F90 file"
  ifort \
    -traceback \
    -assume byterecl \
    -O3 \
    -fp-model precise \
    -fp-stack-check \
    -mcmodel=large \
    -shared-intel \
    -convert big_endian \
    -I/gpfs/apps/MN3/NETCDF/3.6.3/include \
    -L/gpfs/apps/MN3/NETCDF/3.6.3/lib \
    -lnetcdff \
    -lnetcdf \
    ${src} \
    -o ${dest}

  # End
  exit

