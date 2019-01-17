#!/bin/bash

  #
  # HELPER METHODS
  #

  execute_application() {
    local application=$1
    local application_name
    local version_name
    local app_ev
    local app_result
    local app_line
    
    for version in "$application"/*/; do
      application_name=$(basename "${application}")
      version_name=$(basename "${version}")
      if [ "${version_name}" != "results" ]; then
        echo "--- Executing Application: ${application_name} - Version: ${version_name}"

        cd "$version" || exit 1
        if [ ! -f "run.sh" ]; then
          echo "[WARN] Cannot find run.sh script. Skipping application"
        else
          # Run application
          ./run.sh "${USER_OPTS}"
          app_ev=$?

          # Store exit value
          if [ "${app_ev}" -eq 0 ]; then
            app_result="\e[32mOK\e[34m"
          else
            app_result="\e[31mFAIL\e[34m"
          fi
          app_line=$(printf "%-15s | %-15s | %-15s" "${application_name}" "${version_name}" "${app_result}") 
          EXECUTION_RESULTS="${EXECUTION_RESULTS}\n\e[34m${app_line}"

          # Set global exit value
          if [ "${app_ev}" -ne 0 ]; then
            GLOBAL_EV=${app_ev}
          fi
        fi
        cd "${SCRIPT_DIR}" || exit 1
      fi
    done
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
  EXECUTION_RESULTS=""
  for app in "${SCRIPT_DIR}"/*/; do
    if [ "$app" != "${SCRIPT_DIR}/xml/" ]; then
      execute_application "$app"
    fi
  done

  # Print execution results
  echo ""
  echo -e "\e[34m------------------------------------------------------"
  printf "\e[34m%-15s | %-15s | %-15s\n" "APPLICATION" "VERSION" "RESULT"
  echo -e "${EXECUTION_RESULTS}"
  echo -e "\e[34m------------------------------------------------------"
  echo -e "\e[0m"

  # Log execution message
  if [ "${GLOBAL_EV}" -eq 0 ]; then
    echo ""
    echo "ALL APPLICATIONS EXECUTED SUCCESSFULLY!"
    echo ""
  else
    echo ""
    echo "ERROR: SOME APPLICATION FAILED. CHECK ERRORS ABOVE"
    echo ""
  fi

