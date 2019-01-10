#!/usr/bin/env bash

echo ""
echo "This is a script that checks that the filename is correct in two cases:"
echo "    - As a parameter."
echo "    - As a file on a prefix."
echo ""
echo "File path received: $1"
echo "File path received with prefix: $2"
echo "File name: $3"
echo "Output file: $4"
echo ""

ok=true

if [[ "$1" != *"$3" ]];then
    echo "ERROR: The file path received does NOT match the file name that should receive." 2>&1 | tee $4
    echo "ERROR: $1 != *$3"  2>&1 | tee $4
    ok=false
fi

if [[ "$2" != *"$3" ]];then
    echo "ERROR: The file path received in the prefix does NOT match the file name that should receive."  2>&1 | tee $4
    echo "ERROR: $2 != *$3"  2>&1 | tee $4
    ok=false
fi

if [[ $ok == true ]]; then
    echo "Both file paths received are OK." | tee $4
    exit 0
else
    exit 1
fi
