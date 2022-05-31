#!/bin/bash


#
# SCRIPT CONSTANTS
#
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
INSPECTED_DIRS="builders/specs/cli/PyCOMPSsCLIResources/pycompss_cli compss maven-plugins performance_analysis utils/storage"

#
# HELPER METHODS
#

change_headers() {
  # Add java headers
  find . -name "*.java" -exec "${SCRIPT_DIR}/replace_header.sh" {} java_c \;

  # Add c headers
  find . -name "*.c" -exec "${SCRIPT_DIR}/replace_header.sh" {} java_c \;
  find . -name "*.cc" -exec "${SCRIPT_DIR}/replace_header.sh" {} java_c \;
  find . -name "*.h" -exec "${SCRIPT_DIR}/replace_header.sh" {} java_c \;
  find . -name "Makefile*" -exec "${SCRIPT_DIR}/replace_header.sh" {} python \;

  # Add python headers
  find . -name "*.py" -exec "${SCRIPT_DIR}/replace_header.sh" {} python \;
}


#
# MAIN METHOD
#

main() {
  echo "[INFO] Updating headers..."

  for inspect_dir in ${INSPECTED_DIRS}; do
    echo "[INFO] Updating headers on ${inspect_dir}..."
    cd "${SCRIPT_DIR}/../../../${inspect_dir}" || exit 1
    change_headers
    cd "${SCRIPT_DIR}" || exit 1
    echo "[INFO] Headers updated on ${inspect_dir}"
  done

  echo "DONE"
}


#
# ENTRY POINT
#

main
