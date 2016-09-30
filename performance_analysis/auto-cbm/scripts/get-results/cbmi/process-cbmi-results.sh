#!/bin/bash
cd "$(dirname $0)"

# This file will process the results for all the cbmi's
CBMI_NAMES=( "cbm1 cbm2 cbm3" )
STORAGES=( "scratch" "gpfs" )

AUTO_CBM_DIR="$(realpath "../../..")"
RES_DIR="$AUTO_CBM_DIR/results"
ORIGINAL_FORMAT_OUT="./formatOut"
FORMAT_OUT_SANDBOX="./formatOut-sandbox"

PREF="[Auto CBM] -"
# Copy the formatOut directory to a sandbox dir, to modify it without breaking the original one
rm -rf $FORMAT_OUT_SANDBOX &> /dev/null  	    # If it existed from a previous execution, clear it  
cp -r $ORIGINAL_FORMAT_OUT $FORMAT_OUT_SANDBOX   # Copy the original formatOut to a sandbox

echo "$PREF Compiling CBM formatOut's..."
for CBMI in ${CBMI_NAMES[@]}
do  
  (cd $FORMAT_OUT_SANDBOX/$CBMI ; make clean ; make)
done

echo "$PREF Generating plots...."
for STORAGE in ${STORAGES[@]}
do
  echo "$PREF Generating $STORAGE plots"
  
  for CBMI in ${CBMI_NAMES[@]}
  do  
    echo "$PREF Processing $STORAGE $CBMI..."
    
    CBMI_OUT_DIR="${RES_DIR}/${CBMI}-data"
    CBMI_PLOT_DIR="${RES_DIR}/${CBMI}-plots"
    
    # Move .out files to a specific dir depending on the used STORAGE. (cbmi-data/scratch, cbmi-data/gpfs)
    rm -rf ${CBMI_OUT_DIR}/$STORAGE 2> /dev/null
    mkdir ${CBMI_OUT_DIR}/$STORAGE  2> /dev/null 
    OUT_LIST_OF_STORAGE="$(grep -r -l "$STORAGE infini" ${CBMI_OUT_DIR})"
    mv $OUT_LIST_OF_STORAGE ${CBMI_OUT_DIR}/$STORAGE
    
    if [ ! -z "$(ls -1 $CBMI_OUT_DIR/$STORAGE)" ] # If there are files to process
    then 
	  
      # Create needed directories and add all .out to the data directory
      # Copy the *.out result files retrieved from MN, into $FORMAT_OUT_SANDBOX data directories, 
      # which will be used by formatOut.exe.
      
      echo "$PREF Copying $CBMI data files to sandbox..."
      rm -f $FORMAT_OUT_SANDBOX/${CBMI}/data/*
      cp  $CBMI_OUT_DIR/$STORAGE/* $FORMAT_OUT_SANDBOX/${CBMI}/data
      mkdir -p $CBMI_PLOT_DIR/$STORAGE
      
      for GNUPLOT_SCRIPT in $FORMAT_OUT_SANDBOX/$CBMI/src/*.gnuplot
      do  
	  GNUPLOT_SCRIPT="$(realpath ${GNUPLOT_SCRIPT})" # To absolute
	  echo "$PREF Modifying $GNUPLOT_SCRIPT file..."
	  
	  PNG_NAME="${CBMI}_$(basename $GNUPLOT_SCRIPT | sed "s/gp_//g" | sed "s/\.gnuplot//g")_${STORAGE}.png";
	  PNG_PATH="$CBMI_PLOT_DIR/$STORAGE/$PNG_NAME"
	  GNUPLOT_OUTPUT_COMMAND="set output \"${PNG_PATH}\""
	  sed -i "s/.*set\soutput.*//g" $GNUPLOT_SCRIPT # remove any old set output command
	  # Add the line to the script to set the output to a this png file.
	  echo -e "$GNUPLOT_OUTPUT_COMMAND \n\
		  $(cat "$GNUPLOT_SCRIPT") \n " > $GNUPLOT_SCRIPT
      done
    fi
    
  done
  
  ./generate-cbmi-plots.sh # Generate all the plots for all the cbm's
  
done