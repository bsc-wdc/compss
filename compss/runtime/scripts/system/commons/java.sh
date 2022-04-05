#!/bin/bash

if [ -n "${LOADED_SYSTEM_COMMONS_JAVA}" ]; then
  return 0
fi

# Checking COMPSs_HOME definition
if [ -z "${COMPSS_HOME}" ]; then
  echo "COMPSS_HOME not defined"
  exit 1
fi

# Load auxiliar scripts
# shellcheck source=./logger.sh"
# shellcheck disable=SC1091
source "${COMPSS_HOME}Runtime/scripts/system/commons/logger.sh"

#---------------------------------------------------
# ERROR CONSTANTS DECLARATION
#---------------------------------------------------
JAVA_HOME_ERROR="ERROR: Cannot find Java JRE installation. Please set JAVA_HOME."

#---------------------------------------------------
# DEFINING CONSTANTS DECLARATION
#---------------------------------------------------
if [ -z "${JAVA_HOME}" ]; then
  JAVA=java
elif [ -f "${JAVA_HOME}/jre/bin/java" ]; then
  JAVA="${JAVA_HOME}/jre/bin/java"
elif [ -f "${JAVA_HOME}/bin/java" ]; then
  JAVA="${JAVA_HOME}/bin/java"
else
  fatal_error "${JAVA_HOME_ERROR}" 1
fi


LOADED_SYSTEM_COMMONS_JAVA=1