#!/bin/bash -e

  #
  # HELPER FUNCTIONS
  #

  check_and_load() {
    # Retrieve variable name
    local var=$1

    # Check if variable is defined
    if [ "${!var}" = "" ]; then
      echo "[WARN] No ${var} variable defined"
      if [ -f ".$var" ]; then
        echo "[INFO] Loading ${var} from .${var} file"
        local token
        token=$(cat ".${var}")
        export ${var}=${token}
      else
        echo echo "[ERROR] Export ${var} variable or set .${var} file before running this script"
        exit 1
      fi
    fi
  }


  #
  # MAIN
  #

  # Codacy

  # Load token
  codacy_token_varname="CODACY_PROJECT_TOKEN"
  check_and_load "${codacy_token_varname}"

  # Log token
  echo "[INFO] ${codacy_token_varname}=${!codacy_token_varname}"

  # Upload coverage report to codacy
  python-codacy-coverage -r coverage.xml

  echo "DONE"


  # Codecov

  # Load token
  codecov_token_varname="CODECOV_PROJECT_TOKEN"
  check_and_load "${codecov_token_varname}"

  # Log token
  echo "[INFO] ${codecov_token_varname}=${!codecov_token_varname}"

  # Upload coverage report to codecov
  codecov -t ${!codecov_token_varname}

  echo "DONE"

