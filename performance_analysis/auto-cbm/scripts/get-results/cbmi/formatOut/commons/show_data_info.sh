#!/bin/bash
. $(dirname "$0")/commons.sh

# WE STAY IN THE "INCLUDER" DIR!!! (no cd)

PRINT_MODE="v"
ALL_ARGS=( "$@" )

for ARG in "${ALL_ARGS[@]}"
do
    argName="$(echo $ARG | cut -c 3- | cut -d= -f1)"
    argValue="$(echo $ARG | cut -d= -f2)"
    
    if [ "$argName" = "print-mode" ] || [ "$argName" = "p" ];
    then
            PRINT_MODE=$argValue  # v="verbose with colors",
				  # p="plain, to provide input to gnuplot"
				  # s="sets (just print the test sets info)"
    fi
done

EXEC_DIR=../bin
DATA_DIR=../data

fileArgs=""
for f in $DATA_DIR/*.out
do
    fileArgs="$fileArgs $f"
done

VERBOSE="v"
if [ "$PRINT_MODE" = "p" ]; then VERBOSE="-" ; fi

INFO=$($EXEC_DIR/formatOut.exe $VERBOSE $fileArgs)
GP_INPUT=$(echo -e "$INFO" | grep "#" -A9999999999 -B0 | grep -v "#")

function printTestSet
{
    #This will print the data set values for the $PAR(nt(Num tasks), deep(Deepness), tst(Task Sleep Time)) argument.
    # It will take these out from the Gnuplot labeled input which comes out from formatOut.
    LABEL=$1
    PAR=$2
    if [ ! -z "$(echo -e "$GP_INPUT" | grep -E ".*$PAR:.*" )" ] ; then
        echo -e "$LABEL [ $(echo -e "$GP_INPUT" | sed "s/.*\($PAR:.*\)/\1/g" | cut -d":" -f2- | cut -d" " -f1 | sort -g | uniq | xargs | sed 's/\([0-9]\) /\1, /g') ]"
    fi
}

#Get the datasets info into an array.
SETS_INFO=""
SETS_INFO="\n---- Test datasets information ----\n"
  NUM_TASKS="$(printTestSet "    Number of Tasks (nt):" nt   )"
       DEEP="$(printTestSet "   Graph deepness (deep):" deep )"
        TST="$(printTestSet "  Task sleep times (tst):" tst  )"
NUM_WORKERS="$(printTestSet "  Number of workers (nw):" nw   )"
         TX="$(printTestSet "  TX transfer sizes (tx):" tx   )"

SETS_INFO=$(echo -e "$SETS_INFO\n$NUM_TASKS\n$DEEP\n$TST\n$NUM_WORKERS\n$TX" | sed -e "s/-1, //g" | sed -e "s/-1//g")
SETS_INFO="$SETS_INFO\n\n-------------------------------"
###########
    
if [ "$PRINT_MODE" = "v" ]  # Put colors using sed
then
    DEFAULT="\\\e[39m"
    RED="\\\e[91m"
    YELLOW="\\\e[93m"
    INFO=$(echo -e "$INFO" | sed -e "s/\*\*/\*/g") #put colours
    INFO=$(echo -e "$INFO" | sed -e "/ERROR/ s/^/$RED/g ; /ERROR/ s/$/$DEFAULT/g") #put colours
    INFO=$(echo -e "$INFO" | sed -e "s|\"../data/\(.*\)\"|\"\1\"|g")  #only file name 
    INFO=$(echo -e "$INFO" | sed -e "s|idealTime:[0-9]*,||g")  #no ideal time
    INFO=$(echo -e "$INFO" | sed -e "s|elapsedTime:[ ]*\([0-9]\+\),|elapsedTime: \1 ms,   |g")  #ms in elapsedTime
    INFO=$(echo -e "$INFO" | sed -e "s|sleepTime:[ ]*\([0-9]\+\),|sleepTime: \1 ms,|g")  #ms in sleepTime
    INFO=$(echo -e "$INFO" | sed -e "s/\?/$RED\?$DEFAULT/g") #yellow question marks
fi

INFO=$(echo -e "$INFO" | sed -e "s/^-1.*//g")                  # Solves an output bug
INFO=$(echo -e "$INFO" | sed -e "s/#.*//g")                    # Removes comments
INFO=$(echo -e "$INFO" | sed -e "s/\([0-9]\+\)\.[0-9]\+/\1/g") # Times with no decimal places
    
if [ ! "$PRINT_MODE" = "s" ] ; then
    if [ "$PRINT_MODE" = "p" ]; then 
        INFO_WITHOUT_COMMENTS=$(echo "$INFO" | grep -v "#")
        echo -e "$INFO_WITHOUT_COMMENTS"
    else
        echo -e "$INFO\n$SETS_INFO\n"
    fi
else
    echo -e "$SETS_INFO\n"   # Print just sets
fi
