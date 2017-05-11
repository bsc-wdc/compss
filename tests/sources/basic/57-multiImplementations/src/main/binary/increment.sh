#!/bin/bash -e

  counterFile=$1
  echo "[DEBUG] Increasing value on $counterFile"

  # Read value
  counterValue=$(cat $counterFile | head -n 1)
  echo "[DEBUG] Value read: $counterValue"

  # Update value
  counterValue=$((counterValue + 1))
  echo "[DEBUG] Value updated: $counterValue"

  # Write final value
  echo "$counterValue" > ${counterFile}

  # End
  echo "DONE"
  exit 0

