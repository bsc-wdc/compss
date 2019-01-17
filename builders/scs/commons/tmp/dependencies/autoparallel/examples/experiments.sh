#!/bin/bash

  #
  # HELPER METHODS
  #

  experiments_application() {
    local application=$1
    local application_name
    local app_ev
    
    application_name=$(basename "${application}")
    echo "--- Enqueueing Experiments for Application: ${application_name}"

    # Enqueue experiments
    cd "$application" || exit 1
    ./experiments.sh "${USER_OPTS}"
    app_ev=$?
    cd "${SCRIPT_DIR}" || exit 1

    # Set global exit value
    if [ "${app_ev}" -ne 0 ]; then
      GLOBAL_EV=${app_ev}
    fi
  }


  #
  # MAIN
  #

  # Set script variables
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

  # Retrieve script parameters
  USER_OPTS=$*

  # Launch all examples
  GLOBAL_EV=0
  for app in "${SCRIPT_DIR}"/*/; do
    if [ "$app" != "${SCRIPT_DIR}/xml/" ]; then
      experiments_application "$app"
    fi
  done

  # Log execution message
  if [ "${GLOBAL_EV}" -eq 0 ]; then
    echo ""
    echo "ALL APPLICATIONS ENQUEUED SUCCESSFULLY!"
    echo ""
  else
    echo ""
    echo "ERROR: SOME APPLICATION FAILED. CHECK ERRORS ABOVE"
    echo ""
  fi

