#!/bin/bash

  # Trap to clean files
  clean() {
    rm -f $OUTPUT/content.txt
  }

  # To search for an element on an array
  VALID_LEVELS1=(1000 975 950 925 900 850 800 750 700 650 600 550 500 450 400 350 300 250 200 150 100 70 50 30 20 10)
  contains_vl1() {
    local levelToCheck=$1
    isValid=1
    local levelValid
    for levelValid in "${VALID_LEVELS1[@]}"; do
      if [ "${levelValid}" == "${levelToCheck}" ]; then
        isValid=0
        break
      fi
    done
  }

  VALID_LEVELS2=(1000 975 950 925 900 850 800 750 700 650 600 550 500 450 400 350 300 250 200 150 100)
  contains_vl2() {
    local levelToCheck=$1
    isValid=1
    local levelValid
    for levelValid in "${VALID_LEVELS2[@]}"; do
      if [ "${levelValid}" == "${levelToCheck}" ]; then
        isValid=0
      fi
    done
  }

  # Print arguments for debug
  print_args() {
    echo "***************** ARGUMENTS *******************"
    echo "  CW       = ${CW}"
    echo "  ICEC     = ${ICEC}"
    echo "  SH       = ${SH}"
    echo "  SOILT2   = ${SOILT2}"
    echo "  SOILT4   = ${SOILT4}"
    echo "  SOILW2   = ${SOILW2}"
    echo "  SOILW4   = ${SOILW4}"
    echo "  TT       = ${TT}"
    echo "  VV       = ${VV}"
    echo "  HH       = ${HH}"
    echo "  PRMSL    = ${PRMSL}"
    echo "  SOILT1   = ${SOILT1}"
    echo "  SOILT3   = ${SOILT3}"
    echo "  SOILW1   = ${SOILW1}"
    echo "  SOILW3   = ${SOILW3}"
    echo "  SST_TS   = ${SST_TS}"
    echo "  UU       = ${UU}"
    echo "  WEASD    = ${WEASD}"
    echo "***********************************************"
  }


  #######################################################
  # MAIN
  #######################################################

  # Get arguments
  CW=${1}
  ICEC=${2}
  SH=${3}
  SOILT2=${4}
  SOILT4=${5}
  SOILW2=${6}
  SOILW4=${7}
  TT=${8}
  VV=${9}
  HH=${10}
  PRMSL=${11}
  SOILT1=${12}
  SOILT3=${13}
  SOILW1=${14}
  SOILW3=${15}
  SST_TS=${16}
  UU=${17}
  WEASD=${18}

  # For debug
  print_args

  # Begin process
  echo "Processing Hour 00"
  GRIBFILE=$OUTPUT/gfs.t00z.pgrbf00
  if [ ! -e $GRIBFILE ]; then
      echo "Missing input file $GRIBFILE"
      exit 1
  fi

  echo " ***************************************************"
  echo " **********  GRIB grid processor *******************"
  echo " ******* Dump data from $GRIBFILE file ******"
  echo " ***************************************************"

  # Trap to clean files
  trap clean EXIT
  
  # Get data
  echo "--- WGRIB $GRIBFILE ---"
  wgrib $GRIBFILE > content.txt	
  
  # Process data line by line
  echo "--- CAT CONTENT.TXT ---"
  cat content.txt | while read line
  do
    code=$(echo ${line} | gawk -F":" '{print $1}')
    variable=$(echo ${line} | gawk -F":" '{print $4}')
    level=$(echo ${line} | gawk -F":" '{print $12}' | gawk -F" " '{print $1}')
    
    #echo "[NEW LINE]"
    #echo "  CODE = $code"
    #echo "  VARIABLE = $variable"
    #echo "  LEVEL = $level"
          
    if [ "$variable" == "TMP" ] && [ "$level" == "sfc" ]; then
      echo " Code = $code ; Variable = $variable ; Level = $level"
      echo " Dumping SST/TS from gribfile to ${16}"
      wgrib -d $code $GRIBFILE -o ${16}
    fi
          
    if [ "$variable" == "TMP" ] && [ "$level" == "0-10" ]; then
      echo " Code = $code ; Variable = $variable ; Level = $level"                                                                                                                      
      echo " Dumping SOILT level 1 from gribfile to ${12}"
      wgrib -d $code $GRIBFILE -o ${12}
    fi
          
    if [ "$variable" == "TMP" ] && [ "$level" == "10-40" ]; then
      echo " Code = $code ; Variable = $variable ; Level = $level"                                                                                                                      
      echo " Dumping SOILT level 2 from gribfile to ${4}"
      wgrib -d $code $GRIBFILE -o ${4}
    fi
              
    if [ "$variable" == "TMP" ] && [ "$level" == "40-100" ]; then
      echo " Code = $code ; Variable = $variable ; Level = $level"
      echo " Dumping SOILT level 3 from gribfile to ${13}"
      wgrib -d $code $GRIBFILE -o ${13}
    fi
              
    if [ "$variable" == "TMP" ] && [ "$level" == "100-200" ]; then
      echo " Code = $code ; Variable = $variable ; Level = $level"
      echo " Dumping SOILT level 4 from gribfile to ${5}"
      wgrib -d $code $GRIBFILE -o ${5}
    fi
              
    if [ "$variable" == "TMP" ] && [ "$level" == "10-200" ]; then
      echo " Code = $code ; Variable = $variable ; Level = $level"
      echo " Dumping SOILT level 2 from gribfile to ${4}"
      wgrib -d $code $GRIBFILE -o ${4}
      echo " Dumping SOILT level 3 from gribfile to ${13}"
      wgrib -d $code $GRIBFILE -o ${13}
      echo " Dumping SOILT level 4 from gribfile to ${5}"
      wgrib -d $code $GRIBFILE -o ${5}
    fi
            
    if [ "$variable" == "SOILW" ] && [ "$level" == "0-10" ]; then
      echo " Code = $code ; Variable = $variable ; Level = $level"
      echo " Dumping SOILW level 1 from gribfile ${14}"
      wgrib -d $code $GRIBFILE -o ${14}
    fi
              
    if [ "$variable" == "SOILW" ] && [ "$level" == "10-40" ]; then
      echo " Code = $code ; Variable = $variable ; Level = $level"
      echo " Dumping SOILW level 2 from gribfile ${6}"
      wgrib -d $code $GRIBFILE -o ${6}
    fi
              
    if [ "$variable" == "SOILW" ] && [ "$level" == "40-100" ]; then
      echo " Code = $code ; Variable = $variable ; Level = $level"
      echo " Dumping SOILW level 3 from gribfile to ${15}"
      wgrib -d $code $GRIBFILE -o ${15}
    fi
              
    if [ "$variable" == "SOILW" ] && [ "$level" == "100-200" ]; then
      echo " Code = $code ; Variable = $variable ; Level = $level"
      echo " Dumping SOILW level 4 from gribfile to ${7}"
      wgrib -d $code $GRIBFILE -o ${7}
    fi

    if [ "$variable" == "SOILW" ] && [ "$level" == "10-200" ]; then
      echo " Code = $code ; Variable = $variable ; Level = $level"
      echo " Dumping SOILW level 2 from gribfile to ${6}"
      wgrib -d $code $GRIBFILE -o ${6}
      echo " Dumping SOILW level 3 from gribfile to ${15}"
      wgrib -d $code $GRIBFILE -o ${15}
      echo " Dumping SOILW level 4 from gribfile to ${7}"
      wgrib -d $code $GRIBFILE -o ${7}
    fi
              
    if [ "$variable" == "WEASD" ] && [ "$level" == "sfc" ]; then
      echo " Code = $code ; Variable = $variable ; Level = $level"
      echo " Dumping SNOW from gribfile to ${18}"
      wgrib -d $code $GRIBFILE -o ${18}
    fi
      
    if [ "$variable" == "ICEC" ] && [ "$level" == "sfc" ]; then
      echo " Code = $code ; Variable = $variable ; Level = $level"
      echo " Dumping SEAICE from gribfile ${2}"
      wgrib -d $code $GRIBFILE -o ${2}
    fi
      
    if [ "$variable" == "PRMSL" ] && [ "$level" == "MSL" ]; then
      echo " Code = $code ; Variable = $variable ; Level = $level"
      echo " Dumping PRMSL from gribfile to ${11}"
      wgrib -d $code $GRIBFILE -o ${11}
    fi
  done
  
  # Process inverse data line by line
  echo "--- TAC CONTENT.TXT ---"
  tac content.txt | while read line
  do
    code=$(echo ${line} | gawk -F":" '{print $1}')
    variable=$(echo ${line} | gawk -F":" '{print $4}')
    level=$(echo ${line} | gawk -F":" '{print $12}' | gawk -F" " '{print $1}')
    units=$(echo ${line} | gawk -F":" '{print $12}' | gawk -F" " '{print $2}')
    
    #echo "[NEW LINE]"
    #echo "  CODE = $code"
    #echo "  VARIABLE = $variable"
    #echo "  LEVEL = $level"
    #echo "  UNITS = $units"
  
    if [ "$variable" == "UGRD" ] && [ "$units" == "mb" ]; then
      contains_vl1 $level
      if [ $isValid -eq 0 ]; then
        echo " Code = $code ; Variable = $variable ; Level = $level ; Units = $units"
        echo " Dumping Upper level fields U Level: $level to ${17}"
        wgrib -d $code $GRIBFILE -append -o ${17}
      fi
    fi
          
    if [ "$variable" == "VGRD" ] && [ "$units" == "mb" ]; then
      contains_vl1 $level
      if [ $isValid -eq 0 ]; then
        echo " Code = $code ; Variable = $variable ; Level = $level ; Units = $units"
        echo " Dumping Upper level fields V Level: $level to ${9}"
        wgrib -d $code $GRIBFILE -append -o ${9}
      fi
    fi
          
    if [ "$variable" == "TMP" ] && [ "$units" == "mb" ]; then
      contains_vl1 $level
      if [ $isValid -eq 0 ]; then
        echo " Code = $code ; Variable = $variable ; Level = $level ; Units = $units"
        echo " Dumping Upper level fields T Level: $level to ${8}"
        wgrib -d $code $GRIBFILE -append -o ${8}
      fi
    fi   
  
    if [ "$variable" == "HGT" ] && [ "$units" == "mb" ]; then
      contains_vl1 $level
      if [ $isValid -eq 0 ]; then
        echo " Code = $code ; Variable = $variable ; Level = $level ; Units = $units"
        echo " Dumping Upper level fields HGT Level: $levs to ${10}"
        wgrib -d $code $GRIBFILE -append -o ${10}
      fi
    fi  
  
    if [ "$variable" == "RH" ] && [ "$units" == "mb" ]; then
      contains_vl2 $level
      if [ $isValid -eq 0 ]; then
        echo " Code = $code ; Variable = $variable ; Level = $level ; Units = $units"
        echo " Dumping Upper level fields RH Level: $levs to ${3}"
        wgrib -d $code $GRIBFILE -append -o ${3}
      fi
    fi  	
  
    if [ "$variable" == "CLWMR" ] && [ "$units" == "mb" ]; then
      contains_vl2 $level
      if [ $isValid -eq 0 ]; then
        echo " Code = $code ; Variable = $variable ; Level = $level ; Units = $units"
        echo " Dumping Upper level fields CLWMR Level: $levs to ${1}"
        wgrib -d $code $GRIBFILE -append -o ${1}
      fi
    fi  
  done

OUTDIR=/home/bsc19/bsc19533/nmmb/nmmb-bsc-ctm-v2.0/PREPROC/output/
cp -f ${1} ${OUTDIR}/00_CW.dump 
cp -f ${2} ${OUTDIR}/00_ICEC.dump
cp -f ${3} ${OUTDIR}/00_SH.dump
cp -f ${4} ${OUTDIR}/00_SOILT2.dump
cp -f ${5} ${OUTDIR}/00_SOILT4.dump
cp -f ${6} ${OUTDIR}/00_SOILW2.dump
cp -f ${7} ${OUTDIR}/00_SOILW4.dump
cp -f ${8} ${OUTDIR}/00_TT.dump
cp -f ${9} ${OUTDIR}/00_VV.dump
cp -f ${10} ${OUTDIR}/00_HH.dump
cp -f ${11} ${OUTDIR}/00_PRMSL.dump
cp -f ${12} ${OUTDIR}/00_SOILT1.dump
cp -f ${13} ${OUTDIR}/00_SOILT3.dump
cp -f ${14} ${OUTDIR}/00_SOILW1.dump
cp -f ${15} ${OUTDIR}/00_SOILW3.dump
cp -f ${16} ${OUTDIR}/00_SST_TS.dump
cp -f ${17} ${OUTDIR}/00_UU.dump
cp -f ${18} ${OUTDIR}/00_WEASD.dump


