#!/bin/bash

  #--------------------------------------------------------- 
  # HELPER FUNCTIONS
  #---------------------------------------------------------
  compile() {
    local src=$1
    local dest=$2

    ifort \
      -mcmodel=large \
      -shared-intel \
      -convert big_endian \
      -traceback \
      -assume byterecl \
      -O3 \
      -fp-model precise \
      -fp-stack-check \
      $src \
      -o $dest
    if [ $? -ne 0 ]; then
      echo "[ERROR] Cannot compile $src"
      exit 1
    fi
  }

  #--------------------------------------------------------- 
  # MAIN PROCESS
  #---------------------------------------------------------
  compile botsoiltype.f90 botsoiltype.x
  compile deeptemperature.f90 deeptemperature.x
  compile envelope.f90 envelope.x
  compile gfdlco2.f gfdlco2.x
  compile landuse.f90 landuse.x
  compile landusenew.f90 landusenew.x
  compile roughness.f90 roughness.x
  compile smmount.f90 smmount.x
  compile snowalbedo.f90 snowalbedo.x
  compile stdh.f90 stdh.x
  compile stdhtopo.f90 stdhtopo.x
  compile topo.f90 topo.x
  compile toposeamask.f90 toposeamask.x
  compile topsoiltype.f90 topsoiltype.x
  compile vcgenerator.f90 vcgenerator.x

  cd lookup_tables
  ./compile_aerosol.sh
  if [ $? -ne 0 ]; then
    echo "[ERROR] Cannot compile lookup tables"
    exit 1
  fi

  # Normal exit
  exit

