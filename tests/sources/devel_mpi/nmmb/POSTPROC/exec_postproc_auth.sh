#!/bin/bash

  # Get arguments
  folderOutput=$1
  pout=$2
  ctm=$3

  # Execute
  echo "[DEBUG] Execute the postall script"
  cd ${folderOutput}
  ./new_postall.x
  if [ $? -ne 0 ]; then
    echo "[ERROR] Cannot execute postall. Check errors above."
    exit 1
  fi

  ncrcat ${pout} ${ctm}
  if [ $? -ne 0 ]; then
    echo "[ERROR] Cannot execute ncrcat. Check errors above."
    exit 1
  fi

  # Normal exit
  exit

