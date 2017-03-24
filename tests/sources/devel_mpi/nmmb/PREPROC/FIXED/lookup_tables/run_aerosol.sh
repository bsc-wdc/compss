#!/bin/bash

  #--------------------------------------------------------- 
  # HELPER FUNCTIONS
  #---------------------------------------------------------
  process_lookup() {
    local lookupSrcFile=$1
    local lookupExeFile=$2
    local datarh=$3
    local lookupOutFile=$4

    echo "SRC FILE: $lookupSrcFile"
    echo "EXE FILE: $lookupExeFile"
    echo "DATA RH:  $datarh"
    echo "OUT FILE: $lookupOutFile"

    if [ "${MUST_COMPILE}" == "true" ]; then  
      # Clean previous binary
      rm -f ${lookupExeFile}
  
      # Compile
      ifort -mcmodel=large \
            -shared-intel \
            -convert big_endian \
            -traceback \
            -assume byterecl \
            -O3 \
            -fp-model precise \
            -fp-stack-check \
            ${lookupSrcFile} \
            -o ${lookupExeFile}
      if [ $? -ne 0 ]; then
        echo "[ERROR] Cannot compile ${lookupSrcFile}"
        exit 1 
      fi
    fi
 
    # Clean temp source
    rm -f ${lookupSrcFile}
  
    # Execute
    ${lookupExeFile} ${datarh} > ${lookupOutFile}
    if [ $? -ne 0 ]; then
      echo "[ERROR] Cannot execute ${lookupSrcFile} with ${datarh}"
      exit 1 
    fi

    cat fort.7 >> ${lookupOutFile}
    if [ $? -ne 0 ]; then
      echo "[ERROR] Cannot retrieve fort.7 file"
      exit 1 
    fi

    cat fort.8 >> ${lookupOutFile}
    if [ $? -ne 0 ]; then
      echo "[ERROR] Cannot retrieve fort.8 file"
      exit 1 
    fi

    # Clean fort files
    rm -f fort.?
  }

  clean() {
    if [ "${MUST_CLEAN}" == "true" ]; then
      rm -f ${scriptDir}/lookup_aerosol_rh??.x
    fi

    rm -f ${scriptDir}/LOOKUP_aerosol_ssa_rh??.f
    rm -f fort.?
  }


  #--------------------------------------------------------- 
  # MAIN PROCESS
  #---------------------------------------------------------
  
  # Trap exit to clean tmp files
  trap clean EXIT

  scriptDir=$(dirname $0)
  SRC_FILE=${scriptDir}/LOOKUP_aerosol_ssa-rh.f

  # This script receives a parameter for each lookup_aerosol2.dat.rhXX file
  MUST_COMPILE=$1
  MUST_CLEAN=$2
  datarh00=$3
  datarh50=$4
  datarh70=$5
  datarh80=$6
  datarh90=$7
  datarh95=$8
  datarh99=$9

  #--------------------------------------------------------- 
  # lookup_aerosol2.dat.rh00         D2WF=1.0
  tmpSrcFile=${scriptDir}/LOOKUP_aerosol_ssa_rh00.f
  tmpExeFile=${scriptDir}/lookup_aerosol_rh00.x
  outFile=${scriptDir}/lookup_aerosol.out.rh00
  
  sed -e 's/CCCC/1.0/g' ${SRC_FILE} > ${tmpSrcFile}
  
  process_lookup ${tmpSrcFile} ${tmpExeFile} $datarh00 ${outFile}
  
  #---------------------------------------------------------
  # lookup_aerosol2.dat.rh50         D2WF=1.6
  tmpSrcFile=${scriptDir}/LOOKUP_aerosol_ssa_rh50.f
  tmpExeFile=${scriptDir}/lookup_aerosol_rh50.x
  outFile=${scriptDir}/lookup_aerosol.out.rh50
  
  sed -e 's/CCCC/1.6/g' ${SRC_FILE} > ${tmpSrcFile}
  
  process_lookup ${tmpSrcFile} ${tmpExeFile} $datarh50 ${outFile}
  
  #---------------------------------------------------------
  # lookup_aerosol2.dat.rh70         D2WF=1.8
  tmpSrcFile=${scriptDir}/LOOKUP_aerosol_ssa_rh70.f
  tmpExeFile=${scriptDir}/lookup_aerosol_rh70.x
  outFile=${scriptDir}/lookup_aerosol.out.rh70
  
  sed -e 's/CCCC/1.8/g' ${SRC_FILE} > ${tmpSrcFile}
  
  process_lookup ${tmpSrcFile} ${tmpExeFile} $datarh70 ${outFile}
  
  #---------------------------------------------------------
  # lookup_aerosol2.dat.rh80         D2WF=2.0
  tmpSrcFile=${scriptDir}/LOOKUP_aerosol_ssa_rh80.f
  tmpExeFile=${scriptDir}/lookup_aerosol_rh80.x
  outFile=${scriptDir}/lookup_aerosol.out.rh80
  
  sed -e 's/CCCC/2.0/g' ${SRC_FILE} > ${tmpSrcFile}
  
  process_lookup ${tmpSrcFile} ${tmpExeFile} $datarh80 ${outFile}
  
  #---------------------------------------------------------
  # lookup_aerosol2.dat.rh90         D2WF=2.4
  tmpSrcFile=${scriptDir}/LOOKUP_aerosol_ssa_rh90.f
  tmpExeFile=${scriptDir}/lookup_aerosol_rh90.x
  outFile=${scriptDir}/lookup_aerosol.out.rh90
  
  sed -e 's/CCCC/2.4/g' ${SRC_FILE} > ${tmpSrcFile}
  
  process_lookup ${tmpSrcFile} ${tmpExeFile} $datarh90 ${outFile}
  
  #---------------------------------------------------------
  # lookup_aerosol2.dat.rh95         D2WF=2.9
  tmpSrcFile=${scriptDir}/LOOKUP_aerosol_ssa_rh95.f
  tmpExeFile=${scriptDir}/lookup_aerosol_rh95.x
  outFile=${scriptDir}/lookup_aerosol.out.rh95
  
  sed -e 's/CCCC/2.9/g' ${SRC_FILE} > ${tmpSrcFile}
  
  process_lookup ${tmpSrcFile} ${tmpExeFile} $datarh95 ${outFile}
  
  #---------------------------------------------------------
  # lookup_aerosol2.dat.rh99         D2WF=4.8
  tmpSrcFile=${scriptDir}/LOOKUP_aerosol_ssa_rh99.f
  tmpExeFile=${scriptDir}/lookup_aerosol_rh99.x
  outFile=${scriptDir}/lookup_aerosol.out.rh99
  
  sed -e 's/CCCC/4.8/g' ${SRC_FILE} > ${tmpSrcFile}
  
  process_lookup ${tmpSrcFile} ${tmpExeFile} $datarh99 ${outFile}
  
  #---------------------------------------------------------
  # END
  exit

