#!/bin/bash

  #--------------------------------------------------------- 
  # HELPER FUNCTIONS
  #---------------------------------------------------------
  compile() {
    local lookupSrcFile=$1
    local lookupExeFile=$2

    echo "SRC FILE: $lookupSrcFile"
    echo "EXE FILE: $lookupExeFile"

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
 
    # Clean temp source
    rm -f ${lookupSrcFile}
  }

  #--------------------------------------------------------- 
  # MAIN PROCESS
  #---------------------------------------------------------
  SRC_FILE=${scriptDir}/LOOKUP_aerosol_ssa-rh.f

  #--------------------------------------------------------- 
  # lookup_aerosol2.dat.rh00         D2WF=1.0
  tmpSrcFile=${scriptDir}/LOOKUP_aerosol_ssa_rh00.f
  tmpExeFile=${scriptDir}/lookup_aerosol_rh00.x
  sed -e 's/CCCC/1.0/g' ${SRC_FILE} > ${tmpSrcFile}
  compile ${tmpSrcFile} ${tmpExeFile} $datarh00 ${outFile}
  
  #---------------------------------------------------------
  # lookup_aerosol2.dat.rh50         D2WF=1.6
  tmpSrcFile=${scriptDir}/LOOKUP_aerosol_ssa_rh50.f
  tmpExeFile=${scriptDir}/lookup_aerosol_rh50.x
  sed -e 's/CCCC/1.6/g' ${SRC_FILE} > ${tmpSrcFile}
  compile ${tmpSrcFile} ${tmpExeFile} $datarh50 ${outFile}
  
  #---------------------------------------------------------
  # lookup_aerosol2.dat.rh70         D2WF=1.8
  tmpSrcFile=${scriptDir}/LOOKUP_aerosol_ssa_rh70.f
  tmpExeFile=${scriptDir}/lookup_aerosol_rh70.x
  sed -e 's/CCCC/1.8/g' ${SRC_FILE} > ${tmpSrcFile}
  compile ${tmpSrcFile} ${tmpExeFile} $datarh70 ${outFile}
  
  #---------------------------------------------------------
  # lookup_aerosol2.dat.rh80         D2WF=2.0
  tmpSrcFile=${scriptDir}/LOOKUP_aerosol_ssa_rh80.f
  tmpExeFile=${scriptDir}/lookup_aerosol_rh80.x
  sed -e 's/CCCC/2.0/g' ${SRC_FILE} > ${tmpSrcFile}
  compile ${tmpSrcFile} ${tmpExeFile} $datarh80 ${outFile}
  
  #---------------------------------------------------------
  # lookup_aerosol2.dat.rh90         D2WF=2.4
  tmpSrcFile=${scriptDir}/LOOKUP_aerosol_ssa_rh90.f
  tmpExeFile=${scriptDir}/lookup_aerosol_rh90.x
  sed -e 's/CCCC/2.4/g' ${SRC_FILE} > ${tmpSrcFile}
  compile ${tmpSrcFile} ${tmpExeFile} $datarh90 ${outFile}
  
  #---------------------------------------------------------
  # lookup_aerosol2.dat.rh95         D2WF=2.9
  tmpSrcFile=${scriptDir}/LOOKUP_aerosol_ssa_rh95.f
  tmpExeFile=${scriptDir}/lookup_aerosol_rh95.x
  sed -e 's/CCCC/2.9/g' ${SRC_FILE} > ${tmpSrcFile}
  compile ${tmpSrcFile} ${tmpExeFile} $datarh95 ${outFile}
  
  #---------------------------------------------------------
  # lookup_aerosol2.dat.rh99         D2WF=4.8
  tmpSrcFile=${scriptDir}/LOOKUP_aerosol_ssa_rh99.f
  tmpExeFile=${scriptDir}/lookup_aerosol_rh99.x
  sed -e 's/CCCC/4.8/g' ${SRC_FILE} > ${tmpSrcFile}
  compile ${tmpSrcFile} ${tmpExeFile} $datarh99 ${outFile}
  
  #---------------------------------------------------------
  # END
  exit

