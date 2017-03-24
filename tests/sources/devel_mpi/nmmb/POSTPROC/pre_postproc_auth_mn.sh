#!/bin/bash

  # Get arguments
  mustCompile=$1
  folderOutput=$2

  # Setup script variables
  scriptDir=$(dirname $0)

  # Copy dependant files
  echo "[DEBUG] Copy dependant files"
  cp -f ${scriptDir}/namelist.newpost ${folderOutput}
  cp -f ${scriptDir}/new_postall.f ${folderOutput}
  cp -f ${scriptDir}/lmimjm.inc ${folderOutput}

  # Clean previous files
  rm -f ${folderOutput}/new_postall.x
  rm -f ${folderOutput}/*.nc

  # Compile fortran source
  if [ "${mustCompile}" == "true" ]; then
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
      -o ${folderOutput}/new_postall.x
    if [ $? -ne 0 ]; then
      echo "[ERROR] Cannot compile postall src"
      exit 1
    fi
    chmod 755 ${folderOutput}/new_postall.x 

    # Save a copy of the executable
    cp ${folderOutput}/new_postall.x ${scriptDir}/new_postall.x
  else
    # Copy the previous copy to the dest dir
    cp ${scriptDir}/new_postall.x ${folderOutput}/new_postall.x
    if [ $? -ne 0 ]; then
      echo "[ERROR] Cannot copy the previous executable"
      exit 1
    fi
  fi

  # Normal exit
  exit

