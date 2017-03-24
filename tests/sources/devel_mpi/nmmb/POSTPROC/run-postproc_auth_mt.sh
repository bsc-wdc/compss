#!/bin/bash

  clean() {
    rm -f ${folderOutput}/namelist.newpost
    rm -f ${folderOutput}/new_postall.f
    rm -f ${folderOutput}/lmimjm.inc

    rm -f ${folderOutput}/new_postall.x
  }

  # Get arguments
  folderOutput=$1
  domain=$2
  dateHour=$3

  # Setup script variables
  scriptDir=$(dirname $0)

  # Add trap for clean at the end
  trap clean EXIT

  # Copy dependant files
  echo "[DEBUG] Copy dependant files"
  cp -f ${scriptDir}/namelist.newpost ${folderOutput}
  cp -f ${scriptDir}/new_postall.f ${folderOutput}
  cp -f ${scriptDir}/lmimjm.inc ${folderOutput}

  # Clean previous files
  rm -f ${folderOutput}/new_postall.x
  rm -f ${folderOutput}/*.nc

  # Compile fortran source
  echo "[DEBUG] Compile the fortran source"
  ifort \
    -mcmodel=large \
    -shared-intel \
    -assume byterecl \
    -fixed \
    -132 \
    -O3 \
    -fp-model \
    fast=2 \
    -convert big_endian \
    -I/gpfs/apps/MN3/NETCDF/3.6.3/include \
    ${folderOutput}/new_postall.f \
    -L/gpfs/apps/MN3/NETCDF/3.6.3/lib \
    -lnetcdf \
    -lnetcdff \
    -L/gpfs/apps/NVIDIA/HDF5/1.8.8/lib/ \
    -lhdf5 \
    -lhdf5_hl \
    -lhdf5_fortran \
    -lhdf5_hl_fortran \
    -o ${folderOutput}/new_postall.x
  if [ $? -ne 0 ]; then
    echo "[ERROR] Cannot compile postall src"
    exit 1
  fi
  chmod 755 ${folderOutput}/new_postall.x 

  # Execute
  echo "[DEBUG] Execute the postall script"
  cd ${folderOutput}
  ./new_postall.x
  if [ $? -ne 0 ]; then
    echo "[ERROR] Cannot execute postall. Check errors above."
    exit 1
  fi

  ncrcat ${folderOutput}/new_pout_*.nc ${folderOutput}/NMMB-BSC-CTM_${dateHour}_${domain}.nc
  if [ $? -ne 0 ]; then
    echo "[ERROR] Cannot execute ncrcat. Check errors above."
    exit 1
  fi

  # Normal exit
  exit

