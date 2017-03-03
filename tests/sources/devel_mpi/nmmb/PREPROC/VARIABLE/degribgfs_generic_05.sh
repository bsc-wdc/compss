#!/bin/bash 

  # Retrieve directories from environment
  SRC=$VRB
  OUTPUT_DIR=$OUTPUT
  
  # Begin process
  cd $SRC
  
  echo "Processing Hour 00"
  GRIBFILE=$OUTPUT/gfs.t00z.pgrbf00
  
  if [ ! -e $GRIBFILE ]; then
      echo "Missing input file $GRIBFILE"
      exit 1
  fi
  
  # GETTING DATE
  wgrib -s $GRIBFILE | head -1 | cut -d: -f3 | cut -d= -f2 > date.txt
  # GETTING DATA
  wgrib $GRIBFILE > content.txt	
  
  cat content.txt | while read line
  do
    code=$(echo ${line} | gawk -F":" '{print $1}')
    variable=$(echo ${line} | gawk -F":" '{print $4}')
    level=$(echo ${line} | gawk -F":" '{print $12}' | gawk -F" " '{print $1}')
          
    if [ $variable = "TMP" -a $level = "sfc" ]; then
      echo $code $variable $level
      echo " Dumping SST/TS from gribfile "
      wgrib -d $code $GRIBFILE -o ${16}
    fi
          
    if [ $variable = "TMP" -a $level = "0-10" ];then
      echo $code $variable $level
      echo " Dumping SOILT level 1 from gribfile "
      wgrib -d $code $GRIBFILE -o ${12}
    fi
          
    if [ $variable = "TMP" -a $level = "10-40" ]; then
      echo $code $variable $level
      echo " Dumping SOILT level 2 from gribfile "
      wgrib -d $code $GRIBFILE -o $4
    fi
              
    if [ $variable = "TMP" -a $level = "40-100" ]; then
      echo $code $variable $level
      echo " Dumping SOILT level 3 from gribfile "
      wgrib -d $code $GRIBFILE -o ${13}
    fi
              
    if [ $variable = "TMP" -a $level = "100-200" ]; then
      echo $code $variable $level
      echo " Dumping SOILT level 4 from gribfile "
      wgrib -d $code $GRIBFILE -o $5
    fi
            
    if [ $variable = "SOILW" -a $level = "0-10" ]; then
      echo $code $variable $level
      echo " Dumping SOILW level 1 from gribfile "
      wgrib -d $code $GRIBFILE -o ${14}
    fi
              
    if [ $variable = "SOILW" -a $level = "10-40" ]; then
      echo $code $variable $level
      echo " Dumping SOILW level 2 from gribfile "
      wgrib -d $code $GRIBFILE -o $6
    fi
              
    if [ $variable = "SOILW" -a $level = "40-100" ]; then
      echo $code $variable $level
      echo " Dumping SOILW level 3 from gribfile "
      wgrib -d $code $GRIBFILE -o ${15}
    fi
              
    if [ $variable = "SOILW" -a $level = "100-200" ]; then
      echo $code $variable $level
      echo " Dumping SOILW level 4 from gribfile "
      wgrib -d $code $GRIBFILE -o $7
    fi
              
    if [ $variable = "WEASD" -a $level = "sfc" ]; then
      echo $code $variable $level
      echo " Dumping SNOW from gribfile "
      wgrib -d $code $GRIBFILE -o ${18}
    fi
      
    if [ $variable = "ICEC" -a $level = "sfc" ]; then
      echo $code $variable $level
      echo " Dumping SEAICE from gribfile "
      wgrib -d $code $GRIBFILE -o $2
    fi
      
    if [ $variable = "PRMSL" -a $level = "MSL" ]; then
      echo $code $variable $level
      echo " Dumping PRMSL from gribfile "
      wgrib -d $code $GRIBFILE -o ${11}
    fi
  done
  
  tac content.txt | while read line
  do
    code=$(echo ${line} | gawk -F":" '{print $1}')
    variable=$(echo ${line} | gawk -F":" '{print $4}')
    level=$(echo ${line} | gawk -F":" '{print $12}' | gawk -F" " '{print $1}')
    units=$(echo ${line} | gawk -F":" '{print $12}' | gawk -F" " '{print $2}')
  
    if [ $variable = "UGRD" -a $units = "mb" ]; then
      for levs in 1000 975 950 925 900 850 800 750 700 650 600 550 500 450 400 350 300 250 200 150 100 70 50 30 20 10
      do
        if [ $level = $levs ]; then
          echo $code $variable $level $units $levs
          echo " Dumping Upper level fields U Level: " $levs
          wgrib -d $code $GRIBFILE -append -o ${17}
        fi
      done
    fi
          
    if [ $variable = "VGRD" -a $units = "mb" ]; then
      for levs in 1000 975 950 925 900 850 800 750 700 650 600 550 500 450 400 350 300 250 200 150 100 70 50 30 20 10
      do
        if [ $level = $levs ]; then
          echo $code $variable $level $levs
          echo " Dumping Upper level fields V Level: " $levs
          wgrib -d $code $GRIBFILE -append -o $9
        fi
      done
    fi
          
    if [ $variable = "TMP" -a $units = "mb" ]; then
      for levs in 1000 975 950 925 900 850 800 750 700 650 600 550 500 450 400 350 300 250 200 150 100 70 50 30 20 10
      do
        if [ $level = $levs ]; then
          echo $code $variable $level $levs
          echo " Dumping Upper level fields T Level: " $levs
          wgrib -d $code $GRIBFILE -append -o $8
        fi
      done
    fi   
  
    if [ $variable = "HGT" -a $units = "mb" ]; then
      for levs in 1000 975 950 925 900 850 800 750 700 650 600 550 500 450 400 350 300 250 200 150 100 70 50 30 20 10
      do
        if [ $level = $levs ]; then
          echo $code $variable $level $levs
          echo " Dumping Upper level fields HGT Level: " $levs
          wgrib -d $code $GRIBFILE -append -o ${10}
        fi
      done
    fi  
  
    if [ $variable = "RH" -a $units = "mb" ]; then
      for levs in 1000 975 950 925 900 850 800 750 700 650 600 550 500 450 400 350 300 250 200 150 100
      do
        if [ $level = $levs ]; then
          echo $code $variable $level $levs
          echo " Dumping Upper level fields RH Level: " $levs
          wgrib -d $code $GRIBFILE -append -o $3 
        fi
      done
    fi  	
  
    if [ $variable = "CLWMR" -a $units = "mb" ]; then
      for levs in 1000 975 950 925 900 850 800 750 700 650 600 550 500 450 400 350 300 250 200 150 100
      do
        if [ $level = $levs ];then
          echo $code $variable $level $levs
          echo " Dumping Upper level fields CLWMR Level: " $levs
          wgrib -d $code $GRIBFILE -append -o $1
        fi
      done
    fi  
  done

