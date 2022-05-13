#!/bin/bash

if [ -n "${LOADED_SYSTEM_COMMONS_LOGGER}" ]; then
  return 0
fi

###############################################
# SCRIPT CONSTANTS DECLARATION
###############################################
RED=$(tput setaf 1 2>/dev/null)
if [ ! "${?}" == "0" ]; then
  RED='\033[0;31m'
fi

GREEN=$(tput setaf 2 2>/dev/null)
if [ ! "${?}" == "0" ]; then
  GREEN='\033[0;32m'
fi

ORANGE=$(tput setaf 3 2>/dev/null)
if [ ! "${?}" == "0" ]; then
  ORANGE='\033[0;33m'
fi

NC=$(tput sgr0 2>/dev/null)
if [ ! "${?}" == "0" ]; then
  NC='\033[0m'
fi



###############################################
###############################################
# Logging functions
###############################################
###############################################

###############################################
# Displays info message
###############################################
display_info() {
  local msg=$1
  local file=$2

  echo "[ INFO ] ${msg}"
  if [ -n "${file}" ]; then
    echo "[ INFO ] ${msg}" >> "${file}"
  fi
}

###############################################
# Displays success messages
###############################################
display_success() {
  local msg=$1
  local file=$2

  echo -e "${GREEN}[ OK ] ${msg}${NC}"
  if [ -n "${file}" ]; then
    echo -e "[ OK ] ${msg}" >> "${file}"
  fi
}

###############################################
# Displays warning messages
###############################################
display_warning() {
  local msg=$1
  local file=$2

  echo -e "${ORANGE}[ WARNING ] ${msg}${NC}"
  if [ -n "${file}" ]; then
    echo -e "[ WARNING ] ${msg}" >> "${file}"
  fi
}

###############################################
# Displays errors
###############################################
display_error() {
  local error_msg=$1
  local file=$2

  echo -e "${RED}${error_msg}${NC}" 1>&2
  if [ -n "${file}" ]; then
    echo -e "${error_msg}" >> "${file}"
  fi
}

###############################################
# Displays errors and exits
###############################################
fatal_error() {
  local error_msg=$1
  local error_code=$2
  local file=$3

  # Display error
  display_error "${error_msg}" "${file}"

  # Exit
  exit "${error_code}"
}

LOADED_SYSTEM_COMMONS_LOGGER=1