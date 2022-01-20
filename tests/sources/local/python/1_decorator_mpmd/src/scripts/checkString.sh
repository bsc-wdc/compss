#!/usr/bin/env bash

echo ""
echo "This is a script that checks that the string received as a parameter:"
echo ""
echo "a string before the string param: $1"
echo "String param received: $2"
echo ""

if [[ "$1" != "test" ]] || [[ "$2" != "string" ]];then
    echo "ERROR: The string received does NOT match the expected string."
    exit 1
else
    echo "Received the expected string: OK."
fi

if [ "$#" -ne 2 ]; then
    echo "ERROR: Illegal number of binary parameters from 'params' string"
    exit 1
fi

exit 0