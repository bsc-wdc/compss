#!/bin/bash

#
# BASH OPTIONS
#

set -e # Exit when an error occurs
set -u # Exit when undefined variable
#set -x # Enable bash trace


#
# SCRIPT GLOBAL VARIABLES
#

NUM_FILES=10


#
# HELPER METHODS
#

get_args() {
  output_path=$1
  sleep_time=$2
}

check_args() {
  echo "Received arguments:"
  echo " - Output path: ${output_path}"
  echo " - Sleep time: ${sleep_time}"

  # Check output path
  if [ ! -d "${output_path}" ]; then
    echo "ERROR: Invalid output path ${output_path}"
    exit 1
  fi

  # Check sleep time
  if ! [[ "${sleep_time}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: Sleep time is not an integer"
    exit 2
  fi
}

write_files() {
  for (( i=0; i<NUM_FILES; i++ )); do
    # Write file
    file_name=$(mktemp -p "${output_path}")
    echo "WRITING FILE: ${file_name}"
    cat > "${file_name}" << EOT
Test ${i}
EOT

    # Sleep between generated files
    sleep "${sleep_time}s"
  done
}


#
# MAIN METHOD
#

main() {
  # Retrive arguments
  get_args "$@"

  # Check arguments
  check_args

  # Write files
  write_files
}


#
# ENTRY POINT
#

main "$@"
