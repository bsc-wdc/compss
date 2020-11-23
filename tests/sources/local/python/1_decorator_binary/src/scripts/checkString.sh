#!/usr/bin/env bash

echo ""
echo "This is a script that checks that the string received as a parameter:"
echo ""
echo "String received: $1"
echo ""

if [[ "$1" == "string." ]];then
    echo "Exit value will be 15."
    exit 15
elif [[ "$1" != "This is a string." ]];then
    echo "ERROR: The string received does NOT match the expected string."
    exit 1
else
    echo "Received the expeted string: OK."
    exit 0
fi
