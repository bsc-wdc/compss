#!/bin/bash

  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

  # Change to root compss directory
  cd "${SCRIPT_DIR}/../../../compss/" || exit 1

  # Remove java headers
  find . -name "*.java" -exec "${SCRIPT_DIR}/remove_header.sh" {} java_c \; 

  # Remove c headers
  find . -name "*.c" -exec "${SCRIPT_DIR}/remove_header.sh" {} java_c \;
  find . -name "*.cc" -exec "${SCRIPT_DIR}/remove_header.sh" {} java_c \;
  find . -name "*.h" -exec "${SCRIPT_DIR}/remove_header.sh" {} java_c \;
  find . -name "Makefile*" -exec "${SCRIPT_DIR}/remove_header.sh" {} python \;

  # Remove python headers
  find . -name "*.py" -exec "${SCRIPT_DIR}/remove_header.sh" {} python \;

