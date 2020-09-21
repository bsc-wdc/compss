#!/usr/bin/env bash

echo ""
echo "This is a script that checks that the string received as a parameter"
echo "and fails the first time. This forces the runtime to resubmit the task"
echo "and the second time it will not fail."
echo "String received: $1"
echo ""

if [[ "$1" != "testing string" ]];then
    echo "ERROR: The string received does NOT match the expected string."
    exit 1
else
    echo "Received the expeted string: OK."
fi


check_file="/tmp/has_failed.task"

if test -f "$check_file"; then
  # If file exists == OK
  rm $check_file   # Clean on Resubmission
  exit 0
else
  # If file does not exist == FAIL
  echo $1 > $check_file
  exit 1
fi
