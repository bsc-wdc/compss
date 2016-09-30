#!/bin/bash
cd "$(dirname $0)"
. ../../commons/commons.sh

if [ -z "$1" ] ;  then argsError "Number of tasks" nt ; fi

gnuplotFile="../src/gp_speedup_vs_nworkers.gnuplot"
preout="$(../bin/show_data_info.sh --print-mode=p | grep "nt:$1 ")"
out=$(echo "$preout" | sed "s/[a-z]*://g")


if [ ! -z "$preout" ]
then
    echo "Opening gnuplot with file: '$gnuplotFile'..."
    echo "With dataset:"
    echo "$preout" ; echo
    echo "$out" > ../data/gnuplotInput.dat
    p1=$1 ; shift 1
    gnuplot -e "numTasks=$p1;" "$gnuplotFile" -p $*
else
    errorNoExit "The provided data to plot is empty. Are the data constraints you provided in the set of available constraints for this data set? And, are you sure the directory '../data' contains the *.out files?"
    argsError "Number of Tasks  " nt
fi