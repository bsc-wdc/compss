#!/bin/bash

function getSetsArrayFromSetsInfo
{
    PAR=$1
    SETS_INFO=$2
    echo "$(echo -e "$SETS_INFO" | grep "($PAR)" | cut -d[ -f2- | rev | cut -c2- | rev )"
}

function argsError
{    
    SETS_INFO="$(../bin/show_data_info.sh --p=s)"
    
    echo -e "\e[91m
[  ERROR  ]:
    Please provide the data constraints for this plot in this order: ("
    i=0
    while [ ! -z "$1" ]
    do
        LABEL=$1    # Number of Tasks, Number of Workers, Task Sleep Time, etc.
        PAR=$2      # nt, nw, deep, etc.
        i=$(($i+1))
        echo -e "\
        \$$i = $LABEL (available values in this dataset: \e[93m[$(getSetsArrayFromSetsInfo "$PAR" "$SETS_INFO")]\e[91m)"
        shift 2
    done
    echo -e "\
    )\e[39m"
    
    echo
    exit 2
}

function errorNoExit
{
    echo -e "\e[91m[  ERROR  ]: $*\e[39m" ; echo
}

function error
{
    errorNoExit $* ; exit 1
}


# currently not used in the other scripts :)
function errorDataEmpty
{
    errorNoExit "\
    The provided data to plot is empty. \n \
    Are you sure the directory '$1/data' contains the *.out files? \n\
    Are the data constraints you provided in the set of available constraints for this data set? \n\
    Maybe it just means there is no data set with those constraints combination. \
    "
    argsError "$2"
    echo
}
