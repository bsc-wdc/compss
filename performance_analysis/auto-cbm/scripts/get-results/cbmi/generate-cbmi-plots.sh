#!/bin/bash
cd "$(dirname $0)"

AUTO_CBM_DIR="$(realpath "../../..")"
APPS_DIR="$AUTO_CBM_DIR/apps"
RES_DIR=$AUTO_CBM_DIR/results
FORMAT_OUT_SANDBOX="./formatOut-sandbox"

# ALL THE CHANGES DONE TO THE SCRIPTS IN THIS FILE, ARE DONE TO A TMP FORMATOUT DIR, 
#  which is unique in every execution

# This regex is used to get the used parameters in this execution 
# from the "enqueue_cbmi.sh" files, 
#  e.g., "txSizes=(1 2 3 4)", will extract (1 2 3 4)
PARS_ARRAY_REGEX="s/^[a-zA-Z]*\=[(](.*)[)]/\1/g"

# Get the array of parameters from the enqueue_cbmi.sh script
cbm1_numTasks=(      $(echo "$(cat "$APPS_DIR/cbm1/enqueue_cbm1.sh" | grep -E "^numTasks="       | sed -r $PARS_ARRAY_REGEX)"))
cbm1_numWorkers=(    $(echo "$(cat "$APPS_DIR/cbm1/enqueue_cbm1.sh" | grep -E "^numWorkers="     | sed -r $PARS_ARRAY_REGEX)"))
cbm1_deepness=(      $(echo "$(cat "$APPS_DIR/cbm1/enqueue_cbm1.sh" | grep -E "^deepness="       | sed -r $PARS_ARRAY_REGEX)"))
cbm1_taskSleepTimes=($(echo "$(cat "$APPS_DIR/cbm1/enqueue_cbm1.sh" | grep -E "^taskSleepTimes=" | sed -r $PARS_ARRAY_REGEX)"))
cbm2_numTasks=(      $(echo "$(cat "$APPS_DIR/cbm2/enqueue_cbm2.sh" | grep -E "^numTasks="       | sed -r $PARS_ARRAY_REGEX)"))
cbm2_numWorkers=(    $(echo "$(cat "$APPS_DIR/cbm2/enqueue_cbm2.sh" | grep -E "^numWorkers="     | sed -r $PARS_ARRAY_REGEX)"))
cbm2_deepness=(      $(echo "$(cat "$APPS_DIR/cbm2/enqueue_cbm2.sh" | grep -E "^deepness="       | sed -r $PARS_ARRAY_REGEX)"))
cbm2_taskSleepTimes=($(echo "$(cat "$APPS_DIR/cbm2/enqueue_cbm2.sh" | grep -E "^taskSleepTimes=" | sed -r $PARS_ARRAY_REGEX)"))
cbm3_numTasks=(      $(echo "$(cat "$APPS_DIR/cbm3/enqueue_cbm3.sh" | grep -E "^numTasks="       | sed -r $PARS_ARRAY_REGEX)"))
cbm3_numWorkers=(    $(echo "$(cat "$APPS_DIR/cbm3/enqueue_cbm3.sh" | grep -E "^numWorkers="     | sed -r $PARS_ARRAY_REGEX)"))
cbm3_deepness=(      $(echo "$(cat "$APPS_DIR/cbm3/enqueue_cbm3.sh" | grep -E "^deepness="       | sed -r $PARS_ARRAY_REGEX)"))
cbm3_taskSleepTimes=($(echo "$(cat "$APPS_DIR/cbm3/enqueue_cbm3.sh" | grep -E "^taskSleepTimes=" | sed -r $PARS_ARRAY_REGEX)"))

echo
echo "[   -------------------------------------------------------------------    ]"
echo "[                                   CBM1                                   ]"
echo "[   -------------------------------------------------------------------    ]"

############ SPECIAL CBM1 CASE ###############

# Substitute parameters "__PAR__" in every plotted line, modifying the scripts files
# __PAR__=taskSleepTime in these 2 cases
for tst in  "${cbm1_taskSleepTimes[@]}"; do
  #Set TWICE for line (one for numbers, and one for the title, check gnuplot script to understand)
  sed -i "0,/__PAR__/s//$tst/1" $FORMAT_OUT_SANDBOX/cbm1/src/gp_speedup_vs_nworkers.gnuplot
  sed -i "0,/__PAR__/s//$tst/1" $FORMAT_OUT_SANDBOX/cbm1/src/gp_speedup_vs_nworkers.gnuplot
done

#Destroy all __PAR__ occurrences (the ones that havent been used, if any)
echo -e "$(cat $FORMAT_OUT_SANDBOX/cbm1/src/gp_speedup_vs_nworkers.gnuplot)" | grep -v "__PAR__" > tmp
cat tmp > $FORMAT_OUT_SANDBOX/cbm1/src/gp_speedup_vs_nworkers.gnuplot
rm tmp

#Destroy last ,\ occurrence (see the script file to understand this)
echo -e "$(cat $FORMAT_OUT_SANDBOX/cbm1/src/gp_speedup_vs_nworkers.gnuplot)" | \
tr '\n' '@' | sed 's/\(.*\),\\\(.*\)/\1\2/' | tr '@' '\n' > tmp
cat tmp > $FORMAT_OUT_SANDBOX/cbm1/src/gp_speedup_vs_nworkers.gnuplot
rm tmp

############## END OF CBM1 SPECIAL CASE #########################
	
for nt in "${cbm1_numTasks[@]}"; do

	FILENAME_PARS="nt$nt"
	echo "[   AUTO-CBM   ]: Plotting with parameters: $FILENAME_PARS"
	
	# Specify parameters in the png file names, modifying the script file
	sed -i -r "s/\.png/___$FILENAME_PARS\.png/g" $FORMAT_OUT_SANDBOX/cbm1/src/gp_speedup_vs_nworkers.gnuplot
	# Plot files to png
	$FORMAT_OUT_SANDBOX/cbm1/bin/plot_speedup_vs_nworkers.sh $nt
done

echo ; echo
echo "[   -------------------------------------------------------------------    ]"
echo "[                                   CBM2                                   ]"
echo "[   -------------------------------------------------------------------    ]"

for d  in "${cbm2_deepness[@]}"       ; do
for nw in "${cbm2_numWorkers[@]}"     ; do
for nt in "${cbm2_numTasks[@]}"       ; do
for t  in "${cbm2_taskSleepTimes[@]}" ; do

	FILENAME_PARS="d${d}_nw${nw}_nt${nt}_t${t}"
	echo "[   AUTO-CBM   ]: Plotting with parameters: $FILENAME_PARS"
	
	# Specify parameters in the png file names, modifying the script file
	sed -i -r "s/\.png/___$FILENAME_PARS\.png/g" $FORMAT_OUT_SANDBOX/cbm2/src/gp_4_time.gnuplot

	# Plot files to png
	$FORMAT_OUT_SANDBOX/cbm2/bin/plot_4_time.sh $t $nt $nw $d
done; done; done; done


echo ; echo
echo "[   -------------------------------------------------------------------    ]"
echo "[                                   CBM3                                   ]"
echo "[   -------------------------------------------------------------------    ]"

for d  in "${cbm3_deepness[@]}"       ; do
for nw in "${cbm3_numWorkers[@]}"     ; do
for t  in "${cbm3_taskSleepTimes[@]}" ; do

	FILENAME_PARS="d${d}_nw${nw}_t${t}"
	echo "[   AUTO-CBM   ]: Plotting with parameters: $FILENAME_PARS"
	
	# Specify parameters in the png file names, modifying the script file
	sed -i -r "s/\.png/___$FILENAME_PARS\.png/g" $FORMAT_OUT_SANDBOX/cbm3/src/gp_I3_vs_IO3.gnuplot
	$FORMAT_OUT_SANDBOX/cbm3/bin/plot_I3_vs_IO3.sh $t $nw $d
done; done; done
