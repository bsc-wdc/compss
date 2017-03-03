#!/bin/bash 

  clean() {
    rm -f read_paul_source.x
  }

  # Script dir
  scriptDir=$(dirname $0)

  # Add trap to clean env
  trap clean EXIT

  # Erase previous executable if exists
  rm -f ${scriptDir}/read_paul_source.x

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
    ${scriptDir}/read_paul_source.f90 \
    -o ${scriptDir}/read_paul_source.x
  if [ $? -ne 0 ]; then
    echo "[ERROR] Cannot compile read_paul_source.f90"
    exit 1
  fi

  # Execute
  echo "[RPS_SH] Execute RPS with $@"
  ${scriptDir}/read_paul_source.x $@
  if [ $? -ne 0 ]; then
    echo "[ERROR] Exception executing read_paul_source.x"
    exit 1
  fi

  # End
  exit

