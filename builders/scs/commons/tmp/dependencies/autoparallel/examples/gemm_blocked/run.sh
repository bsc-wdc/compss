#!/bin/bash -e

  # Script global variables
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

  for app_version in "${SCRIPT_DIR}"/*/; do
    app_version_name=$(basename "$app_version")
    if [ "${app_version_name}" != "results" ]; then
      echo "--- Executing ${app_version}"
      (
      cd "$app_version"
      ./run.sh "$@"
      )
    fi
  done

