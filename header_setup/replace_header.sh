#!/bin/bash

  SOURCE_DIR=$(dirname $0)

  # Check and get parameters
  if [ $# != 2 ]; then
    echo "Usage: replace_header.sh <file> <language>"
    exit 1
  fi

  file=$1
  lang=$2

  # Replace header
  cat $SOURCE_DIR/header.template.$lang > $SOURCE_DIR/tmp
  cat $file | awk -f $SOURCE_DIR/remove_header_$lang.awk >> $SOURCE_DIR/tmp
  mv $SOURCE_DIR/tmp $file

