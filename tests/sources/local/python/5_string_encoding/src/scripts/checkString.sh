#!/usr/bin/env bash

echoerr() { echo "$@" 1>&2; }

echo ""
echo "This is a script that checks that the string received as a parameter:"
echo ""
echo "String received: $1"
echo ""

echoerr "String received: $1"

if [[ "$1" != "testing string" ]];then
    echo "ERROR: The string received does NOT match the expected string."
    exit 1
else
    echo "Received the expeted string: OK."
    exit 0
fi
