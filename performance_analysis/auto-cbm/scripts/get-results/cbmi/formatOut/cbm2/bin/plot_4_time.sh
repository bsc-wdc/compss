#!/bin/bash
cd "$(dirname $0)"
. ../../commons/commons.sh

if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ]
then
    argsError "Task Sleep Time  " tst  \
              "Number of Tasks  " nt   \
              "Number of Workers" nw   \
              "Graph deepness   " deep
fi

gnuplotFile="../src/gp_4_time.gnuplot"
preout="$(../bin/show_data_info.sh --print-mode=p | grep "tst:$1 " | grep "nt:$2 " | grep "nw:$3 " | grep "deep:$4 ")"
../bin/show_data_info.sh --print-mode=p
out=$(echo "$preout" | sed "s/[a-z]*://g") # Delete labels such as nw:, deep:, etc.

if [ ! -z "$preout" ]
then
    echo "Opening gnuplot with file: '$gnuplotFile'..."
    echo "With dataset:"
    echo "$preout" ; echo
    echo "$out" > ../data/gnuplotInput.dat
    p1=$1 ; p2=$2 ; p3=$3 ; p4=$4 ;shift 4
    gnuplot -e "taskSleepTime=$p1;numTasks=$p2;workers=$p3;deepness=$p4" "$gnuplotFile" -p $*
else
    errorNoExit "The provided data to plot is empty. Are the data constraints you provided in the set of available constraints for this data set? And, are you sure the directory '../data' contains the *.out files?"
    argsError "Task Sleep Time  " tst  \
              "Number of Tasks  " nt   \
              "Number of Workers" nw   \
              "Graph deepness   " deep
fi