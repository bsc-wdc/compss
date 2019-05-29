#!/bin/bash

# This script builds a bundle with all the needed stuff (aside from Redis backend)
# to use the Redis storage API with COMPSs


#
# BASH OPTIONS
#

set -e # Exit when command fails
set -u # Exit when undefined variable
#set -x # Enable bash trace


#
# SCRIPT VARIABLES
#
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUNDLE_NAME=COMPSs-Redis-bundle
BUNDLE_PATH="${SCRIPT_DIR}/${BUNDLE_NAME}"


#
# MAIN
#

main() {
  # Move to root folder
  cd "${SCRIPT_DIR}"
  if [ ! -f "${SCRIPT_DIR}/target/compss-redisPSCO.jar" ]; then
     # Compile JAVA sources
     mvn -U clean package
  fi
  # Clean and create bundle directory
  rm -rf "${BUNDLE_PATH}"
  mkdir -p "${BUNDLE_PATH}"

  # Copy the Java JAR that contains the implementation of the Redis API
  cp "${SCRIPT_DIR}"/target/compss-redisPSCO.jar "${BUNDLE_PATH}"
  # Remove .pyc files from Python code
  rm -f "${SCRIPT_DIR}"/python/storage/*.pyc
  # Copy Python API
  cp -rf "${SCRIPT_DIR}"/python "${BUNDLE_PATH}"

  # Move the scripts folder to the bundle
  cp -rf "${SCRIPT_DIR}"/scripts "${BUNDLE_PATH}"

  # Restore user directory
  cd -
}

#
# ENTRY POINT
#
main
