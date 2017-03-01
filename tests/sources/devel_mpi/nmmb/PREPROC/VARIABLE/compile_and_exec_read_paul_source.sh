#!/bin/bash 

  clean() {
    rm -f source_out.nc
    rm -f read_paul_source.x
  }

  # Add trap to clean env
  trap clean EXIT

  # Erase previous executable if exists
  rm -f read_paul_source.x

  # Compile font (linking to libraries)
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
    read_paul_source.f90 \
    -o read_paul_source.x
  if [ $? -ne 0 ]; then
    echo "[ERROR] Cannot compile read_paul_source.f90"
    exit 1
  fi

  # Execute
  ./read_paul_source.x
  if [ $? -ne 0 ]; then
    echo "[ERROR] Exception executing read_paul_source.x"
    exit 1
  fi

  # End
  exit

