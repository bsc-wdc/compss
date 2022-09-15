#!/usr/bin/env bash

echo ""
echo "String param received: $1"
echo ""

if [[ "$2" != "this is an mpi task with no task decorator" ]];then
    echo "ERROR: The string received does NOT match the expected string."
    exit 1
else
    echo "Received the expected string: OK."
    exit 0
fi
