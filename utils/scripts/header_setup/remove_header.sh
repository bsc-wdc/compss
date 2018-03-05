#!/bin/bash

  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

  # Check and get parameters
  if [ $# != 2 ]; then
    echo "Usage: replace_header.sh <file> <language>"
    exit 1
  fi

  file=$1
  lang=$2

  # Remove file header if it exists
  awk -f "$SCRIPT_DIR/remove_header_$lang.awk" < "$file" >> "$SCRIPT_DIR/tmp"
  mv "$SCRIPT_DIR/tmp" "$file"

