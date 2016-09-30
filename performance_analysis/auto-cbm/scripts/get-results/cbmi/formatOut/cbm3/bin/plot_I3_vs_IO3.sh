#!/bin/bash
cd "$(dirname $0)"
. ../../commons/commons.sh

if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ]
then
    argsError "Task Sleep Time  " tst  \
              "Number of Workers" nw   \
              "Graph deepness   " deep
fi

gnuplotFile=../src/gp_I3_vs_IO3.gnuplot
preout="$(../bin/show_data_info.sh --print-mode=p | grep "tst:$1 " | grep "nw:$2 " | grep "deep:$3 " | grep "cbm:3 ")"
out=$(echo "$preout" | sed "s/[a-z]*://g")

if [ ! -z "$preout" ]
then
    echo "Opening gnuplot with file: '$gnuplotFile'..."
    echo "With dataset:"
    echo "$preout" ; echo
    echo "$out" > ../data/gnuplotInput.dat
    p1=$1 ; p2=$2 ; p3=$3 ; shift 3
    gnuplot -e "taskSleepTime=$p1;workers=$p2;deepness=$p3" "$gnuplotFile" -p $*
else
    errorNoExit "The provided data to plot is empty. Are the data constraints you provided in the set of available constraints for this data set? And, are you sure the directory '$CBM_DIR/data' contains the *.out files?"
    argsError "Task Sleep Time  " tst  \
              "Number of Workers" nw   \
              "Graph deepness   " deep
fi